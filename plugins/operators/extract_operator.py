"""
Extract Operator
================
Extrai dados de uma fonte configurável e disponibiliza o resultado via XCom.

Fontes suportadas
-----------------
- ``postgres``  → via ``PostgresHook`` do Airflow
- ``mysql``     → via ``MySqlHook`` do Airflow
- ``s3``        → via ``S3Hook`` do Airflow (requer ``apache-airflow-providers-amazon``)
- ``http``      → via ``HttpHook`` do Airflow
- ``file``      → leitura de arquivo local (CSV, JSON, Parquet)

Campos de configuração (``config`` no YAML)
-------------------------------------------
Comuns a todas as fontes:
    source_type (str):          Tipo da fonte. Obrigatório.
    output_xcom_key (str):      Chave XCom para o resultado. Default: ``extract_result``.

Para ``postgres`` e ``mysql``:
    conn_id (str):              Connection ID do Airflow. Obrigatório.
    sql (str):                  Query SQL. Obrigatório.
    sql_file (str):             Caminho para arquivo .sql (alternativa a ``sql``).
    parameters (dict):          Parâmetros para a query (evita SQL injection).

Para ``s3``:
    conn_id (str):              Connection ID do Airflow. Obrigatório.
    bucket (str):               Nome do bucket. Obrigatório.
    key (str):                  Chave (path) do objeto. Obrigatório.
    file_format (str):          ``csv`` | ``json`` | ``parquet``. Default: ``csv``.

Para ``http``:
    conn_id (str):              Connection ID do Airflow. Obrigatório.
    endpoint (str):             Endpoint da API. Obrigatório.
    method (str):               Método HTTP. Default: ``GET``.
    headers (dict):             Headers opcionais.
    data (dict):                Body da requisição.

Para ``file``:
    path (str):                 Caminho do arquivo. Obrigatório.
    file_format (str):          ``csv`` | ``json`` | ``parquet``. Default: ``csv``.
    read_options (dict):        Opções extras repassadas ao pandas.read_*.

Exemplo de configuração YAML
-----------------------------
.. code-block:: yaml

    - id: extract_orders
      type: extract
      description: "Extrai pedidos do PostgreSQL"
      config:
        source_type: postgres
        conn_id: postgres_source
        sql_file: include/sql/extract_orders.sql
        parameters:
          days_back: 30
      depends_on: []
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from operators.base_etl_operator import BaseETLOperator

log = logging.getLogger(__name__)


class ExtractOperator(BaseETLOperator):
    """Operador de extração de dados para pipelines ETL."""

    REQUIRED_CONFIG_FIELDS = ["source_type"]

    ui_color = "#d4edda"  # verde claro

    def _run(self, context: dict[str, Any]) -> Any:
        source_type: str = self.task_config["source_type"].lower()

        extractors = {
            "postgres": self._extract_from_db,
            "mysql": self._extract_from_db,
            "s3": self._extract_from_s3,
            "http": self._extract_from_http,
            "file": self._extract_from_file,
        }

        extractor = extractors.get(source_type)
        if extractor is None:
            raise ValueError(
                f"source_type desconhecido: '{source_type}'. "
                f"Disponíveis: {sorted(extractors)}"
            )

        log.info("Extraindo dados via source_type='%s'.", source_type)
        result = extractor(context)

        xcom_key = self.task_config.get("output_xcom_key", "extract_result")
        context["task_instance"].xcom_push(key=xcom_key, value=result)
        log.info("Resultado publicado no XCom com chave '%s'.", xcom_key)
        return result

    # ------------------------------------------------------------------
    # Extratores por fonte
    # ------------------------------------------------------------------

    def _extract_from_db(self, context: dict[str, Any]) -> list[dict]:
        """Extrai dados de banco relacional (Postgres, MySQL)."""
        from airflow.hooks.base import BaseHook

        source_type = self.task_config["source_type"].lower()
        conn_id: str = self.task_config.get("conn_id")
        if not conn_id:
            raise ValueError("'conn_id' é obrigatório para source_type='%s'." % source_type)

        sql = self._resolve_sql()

        if source_type == "postgres":
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            hook = PostgresHook(postgres_conn_id=conn_id)
        else:
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            hook = MySqlHook(mysql_conn_id=conn_id)

        parameters = self.task_config.get("parameters", {})
        log.info("Executando query em conn_id='%s'.", conn_id)
        records = hook.get_records(sql, parameters=parameters or None)
        log.info("%d registros extraídos.", len(records))
        return records

    def _extract_from_s3(self, context: dict[str, Any]) -> list[dict]:
        """Extrai dados de um objeto no Amazon S3."""
        import io
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        conn_id = self.task_config.get("conn_id", "aws_default")
        bucket = self.task_config["bucket"]
        key = self.task_config["key"]
        file_format = self.task_config.get("file_format", "csv").lower()

        hook = S3Hook(aws_conn_id=conn_id)
        obj = hook.get_key(key, bucket_name=bucket)
        raw = obj.get()["Body"].read()

        read_options = self.task_config.get("read_options", {})
        buf = io.BytesIO(raw)

        readers = {
            "csv": lambda: pd.read_csv(buf, **read_options),
            "json": lambda: pd.read_json(buf, **read_options),
            "parquet": lambda: pd.read_parquet(buf, **read_options),
        }
        reader = readers.get(file_format)
        if reader is None:
            raise ValueError(f"file_format não suportado: '{file_format}'.")

        df = reader()
        log.info("S3 s3://%s/%s → %d linhas, %d colunas.", bucket, key, len(df), len(df.columns))
        import json
        return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))

    def _extract_from_http(self, context: dict[str, Any]) -> Any:
        """Extrai dados de uma API HTTP."""
        import json as json_lib
        from airflow.providers.http.hooks.http import HttpHook

        conn_id = self.task_config.get("conn_id", "http_default")
        endpoint = self.task_config.get("endpoint", "/")
        method = self.task_config.get("method", "GET").upper()
        headers = self.task_config.get("headers", {})
        data = self.task_config.get("data", {})

        hook = HttpHook(method=method, http_conn_id=conn_id)
        response = hook.run(endpoint=endpoint, headers=headers, data=data)
        result = response.json()
        log.info("HTTP %s %s → status %s.", method, endpoint, response.status_code)
        return result

    def _extract_from_file(self, context: dict[str, Any]) -> list[dict]:
        """Lê dados de um arquivo local."""
        import pandas as pd

        path = Path(self.task_config["path"])
        file_format = self.task_config.get("file_format", "csv").lower()
        read_options = self.task_config.get("read_options", {})

        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        readers = {
            "csv": lambda: pd.read_csv(path, **read_options),
            "json": lambda: pd.read_json(path, **read_options),
            "parquet": lambda: pd.read_parquet(path, **read_options),
        }
        reader = readers.get(file_format)
        if reader is None:
            raise ValueError(f"file_format não suportado: '{file_format}'.")

        df = reader()
        log.info("Arquivo '%s' → %d linhas, %d colunas.", path, len(df), len(df.columns))
        import json
        return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_sql(self) -> str:
        """Retorna a query SQL do campo ``sql`` ou do arquivo ``sql_file``."""
        sql = self.task_config.get("sql")
        sql_file = self.task_config.get("sql_file")

        if sql:
            return sql

        if sql_file:
            path = Path(sql_file)
            if not path.exists():
                raise FileNotFoundError(f"Arquivo SQL não encontrado: {path}")
            return path.read_text(encoding="utf-8")

        raise ValueError(
            "Defina 'sql' (query inline) ou 'sql_file' (caminho do arquivo)."
        )
