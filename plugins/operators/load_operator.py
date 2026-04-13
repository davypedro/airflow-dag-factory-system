"""
Load Operator
=============
Carrega dados transformados em um destino configurável.

Destinos suportados
-------------------
- ``postgres``  → via ``PostgresHook`` (INSERT ou UPSERT)
- ``mysql``     → via ``MySqlHook``
- ``s3``        → upload de arquivo CSV/JSON/Parquet
- ``file``      → escrita em arquivo local

Estratégias de carregamento (``load_strategy``)
------------------------------------------------
- ``append``    → INSERT das novas linhas (default)
- ``replace``   → TRUNCATE + INSERT
- ``upsert``    → INSERT ON CONFLICT DO UPDATE (somente Postgres)

Campos de configuração (``config`` no YAML)
-------------------------------------------
Comuns:
    destination_type (str):     Tipo do destino. Obrigatório.
    input_xcom_key (str):       Chave XCom dos dados de entrada. Default: ``transform_result``.
    input_task_id (str):        Task ID de onde ler o XCom.
    load_strategy (str):        ``append`` | ``replace`` | ``upsert``. Default: ``append``.
    batch_size (int):           Número de registros por lote. Default: 1000.

Para ``postgres`` e ``mysql``:
    conn_id (str):              Connection ID do Airflow. Obrigatório.
    table (str):                Tabela de destino. Obrigatório.
    schema (str):               Schema (Postgres). Default: ``public``.
    upsert_keys (list):         Colunas-chave para upsert. Obrigatório se strategy=upsert.

Para ``s3``:
    conn_id (str):              Connection ID. Default: ``aws_default``.
    bucket (str):               Bucket de destino. Obrigatório.
    key (str):                  Chave do objeto. Obrigatório.
    file_format (str):          ``csv`` | ``json`` | ``parquet``. Default: ``csv``.

Para ``file``:
    path (str):                 Caminho do arquivo de saída. Obrigatório.
    file_format (str):          ``csv`` | ``json`` | ``parquet``. Default: ``csv``.

Exemplo de configuração YAML
-----------------------------
.. code-block:: yaml

    - id: load_orders
      type: load
      description: "Carrega pedidos processados no data warehouse"
      config:
        destination_type: postgres
        conn_id: postgres_dwh
        table: fact_orders
        schema: public
        load_strategy: upsert
        upsert_keys: [order_id]
        input_xcom_key: transform_result
        input_task_id: transform_orders
      depends_on: [transform_orders]
"""

from __future__ import annotations

import logging
from typing import Any

from operators.base_etl_operator import BaseETLOperator

log = logging.getLogger(__name__)


class LoadOperator(BaseETLOperator):
    """Operador de carregamento de dados para pipelines ETL."""

    REQUIRED_CONFIG_FIELDS = ["destination_type"]

    ui_color = "#d1ecf1"  # azul claro

    def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        import pandas as pd

        # ── Obter dados de entrada ────────────────────────────────────
        input_xcom_key = self.task_config.get("input_xcom_key", "transform_result")
        input_task_id = self.task_config.get("input_task_id")

        records = context["task_instance"].xcom_pull(
            task_ids=input_task_id,
            key=input_xcom_key,
        )
        if records is None:
            raise ValueError(
                f"XCom vazio para key='{input_xcom_key}', task_ids='{input_task_id}'."
            )

        df = pd.DataFrame(records)
        log.info(
            "Carregando %d linha(s) no destino '%s'.",
            len(df),
            self.task_config["destination_type"],
        )

        destination_type = self.task_config["destination_type"].lower()
        loaders = {
            "postgres": self._load_to_postgres,
            "mysql": self._load_to_mysql,
            "s3": self._load_to_s3,
            "file": self._load_to_file,
        }

        loader = loaders.get(destination_type)
        if loader is None:
            raise ValueError(
                f"destination_type desconhecido: '{destination_type}'. "
                f"Disponíveis: {sorted(loaders)}"
            )

        stats = loader(df, context)
        log.info("Carga concluída: %s", stats)
        return stats

    # ------------------------------------------------------------------
    # Loaders
    # ------------------------------------------------------------------

    def _load_to_postgres(self, df, context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        conn_id = self.task_config.get("conn_id")
        if not conn_id:
            raise ValueError("'conn_id' é obrigatório para destination_type='postgres'.")

        table = self.task_config.get("table")
        if not table:
            raise ValueError("'table' é obrigatório para destination_type='postgres'.")

        schema = self.task_config.get("schema", "public")
        strategy = self.task_config.get("load_strategy", "append").lower()
        batch_size = self.task_config.get("batch_size", 1000)

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            full_table = f"{schema}.{table}"

            if strategy == "replace":
                cur.execute(f"TRUNCATE TABLE {full_table}")
                log.info("Tabela %s truncada.", full_table)

            rows_inserted = 0
            for batch_df in self._batches(df, batch_size):
                if strategy == "upsert":
                    rows_inserted += self._postgres_upsert(cur, batch_df, full_table)
                else:
                    hook.insert_rows(
                        table=full_table,
                        rows=batch_df.itertuples(index=False, name=None),
                        target_fields=list(batch_df.columns),
                        replace=False,
                        commit_every=batch_size,
                    )
                    rows_inserted += len(batch_df)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return {"rows_inserted": rows_inserted, "table": full_table, "strategy": strategy}

    def _postgres_upsert(self, cur, df, full_table: str) -> int:
        """Executa INSERT … ON CONFLICT DO UPDATE para cada lote."""
        upsert_keys = self.task_config.get("upsert_keys", [])
        if not upsert_keys:
            raise ValueError("'upsert_keys' é obrigatório para load_strategy='upsert'.")

        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}"
            for c in columns
            if c not in upsert_keys
        )
        conflict_cols = ", ".join(upsert_keys)

        sql = (
            f"INSERT INTO {full_table} ({', '.join(columns)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
        )

        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        cur.executemany(sql, rows)
        return len(rows)

    def _load_to_mysql(self, df, context) -> dict:
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        conn_id = self.task_config.get("conn_id")
        table = self.task_config.get("table")
        strategy = self.task_config.get("load_strategy", "append").lower()
        batch_size = self.task_config.get("batch_size", 1000)

        if not conn_id or not table:
            raise ValueError("'conn_id' e 'table' são obrigatórios para destination_type='mysql'.")

        hook = MySqlHook(mysql_conn_id=conn_id)

        if strategy == "replace":
            hook.run(f"TRUNCATE TABLE {table}")

        rows_inserted = 0
        for batch_df in self._batches(df, batch_size):
            hook.insert_rows(
                table=table,
                rows=batch_df.itertuples(index=False, name=None),
                target_fields=list(batch_df.columns),
            )
            rows_inserted += len(batch_df)

        return {"rows_inserted": rows_inserted, "table": table, "strategy": strategy}

    def _load_to_s3(self, df, context) -> dict:
        import io
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        conn_id = self.task_config.get("conn_id", "aws_default")
        bucket = self.task_config.get("bucket")
        key = self.task_config.get("key")
        file_format = self.task_config.get("file_format", "csv").lower()

        if not bucket or not key:
            raise ValueError("'bucket' e 'key' são obrigatórios para destination_type='s3'.")

        buf = io.BytesIO()
        if file_format == "csv":
            df.to_csv(buf, index=False)
        elif file_format == "json":
            df.to_json(buf, orient="records", lines=True)
        elif file_format == "parquet":
            df.to_parquet(buf, index=False)
        else:
            raise ValueError(f"file_format não suportado: '{file_format}'.")

        buf.seek(0)
        hook = S3Hook(aws_conn_id=conn_id)
        hook.load_bytes(
            bytes_data=buf.read(),
            key=key,
            bucket_name=bucket,
            replace=True,
        )

        log.info("Upload concluído: s3://%s/%s", bucket, key)
        return {"bucket": bucket, "key": key, "rows": len(df)}

    def _load_to_file(self, df, context) -> dict:
        from pathlib import Path

        path = Path(self.task_config["path"])
        file_format = self.task_config.get("file_format", "csv").lower()

        path.parent.mkdir(parents=True, exist_ok=True)

        if file_format == "csv":
            df.to_csv(path, index=False)
        elif file_format == "json":
            df.to_json(path, orient="records", lines=True)
        elif file_format == "parquet":
            df.to_parquet(path, index=False)
        else:
            raise ValueError(f"file_format não suportado: '{file_format}'.")

        log.info("Arquivo escrito: %s (%d linhas)", path, len(df))
        return {"path": str(path), "rows": len(df)}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _batches(df, size: int):
        """Gera DataFrames de tamanho ``size`` a partir de ``df``."""
        for start in range(0, len(df), size):
            yield df.iloc[start : start + size]
