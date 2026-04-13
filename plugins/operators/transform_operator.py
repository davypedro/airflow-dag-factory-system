"""
Transform Operator
==================
Aplica transformações sobre dados recebidos via XCom e publica o resultado.

O operador suporta dois modos de transformação:

1. **Transformações declarativas** (configuradas no YAML):
   Uma sequência de ``steps`` onde cada step é uma operação built-in
   aplicada ao DataFrame pandas em ordem.

2. **Transformação customizada** (callable Python):
   Um ``callable`` (caminho dotted) que recebe o DataFrame e retorna
   o DataFrame transformado. Use este modo para lógicas complexas.

Steps declarativos disponíveis
-------------------------------
- ``rename_columns``    → renomeia colunas (map ``old: new``)
- ``drop_columns``      → remove colunas
- ``filter_rows``       → filtra linhas via query pandas
- ``fill_nulls``        → preenche nulos (por coluna ou valor global)
- ``cast_types``        → converte tipos de colunas
- ``add_column``        → adiciona coluna com valor fixo ou expressão
- ``drop_duplicates``   → remove duplicatas (opcionalmente por colunas)
- ``sort``              → ordena por colunas

Campos de configuração (``config`` no YAML)
-------------------------------------------
    input_xcom_key (str):   Chave XCom com os dados de entrada. Default: ``extract_result``.
    input_task_id (str):    Task ID de onde ler o XCom. Default: tarefa anterior.
    output_xcom_key (str):  Chave XCom de saída. Default: ``transform_result``.
    callable (str):         Caminho dotted para callable customizado (modo 2).
    steps (list):           Lista de steps declarativos (modo 1).

Exemplo de configuração YAML
-----------------------------
.. code-block:: yaml

    - id: transform_orders
      type: transform
      description: "Limpa e enriquece os dados de pedidos"
      config:
        input_xcom_key: extract_result
        input_task_id: extract_orders
        output_xcom_key: transform_result
        steps:
          - type: rename_columns
            mapping:
              order_dt: order_date
              cust_id: customer_id
          - type: drop_columns
            columns: [internal_flag, legacy_id]
          - type: filter_rows
            query: "total_amount > 0"
          - type: fill_nulls
            value: 0
          - type: cast_types
            mapping:
              order_date: datetime
              total_amount: float
          - type: drop_duplicates
            subset: [order_id]
      depends_on: [extract_orders]
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from operators.base_etl_operator import BaseETLOperator

log = logging.getLogger(__name__)


class TransformOperator(BaseETLOperator):
    """Operador de transformação de dados para pipelines ETL."""

    ui_color = "#fff3cd"  # amarelo claro

    def _run(self, context: dict[str, Any]) -> list[dict]:
        import pandas as pd

        # ── Obter dados de entrada ────────────────────────────────────
        input_xcom_key = self.task_config.get("input_xcom_key", "extract_result")
        input_task_id = self.task_config.get("input_task_id")

        records = context["task_instance"].xcom_pull(
            task_ids=input_task_id,
            key=input_xcom_key,
        )
        if records is None:
            raise ValueError(
                f"XCom vazio para key='{input_xcom_key}', task_ids='{input_task_id}'. "
                "Verifique se a tarefa de extração publicou os dados corretamente."
            )

        df = pd.DataFrame(records)
        log.info("DataFrame de entrada: %d linhas × %d colunas.", len(df), len(df.columns))

        # ── Aplicar transformação ──────────────────────────────────────
        callable_path = self.task_config.get("callable")
        if callable_path:
            df = self._apply_callable(df, callable_path)
        else:
            df = self._apply_steps(df, self.task_config.get("steps", []))

        log.info("DataFrame transformado: %d linhas × %d colunas.", len(df), len(df.columns))

        # ── Serializar para tipos nativos Python (XCom usa JSON) ──────
        # df.to_json converte numpy int/float/bool e pd.Timestamp para
        # tipos nativos; json.loads devolve um list[dict] puro Python.
        import json
        result = json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))
        output_xcom_key = self.task_config.get("output_xcom_key", "transform_result")
        context["task_instance"].xcom_push(key=output_xcom_key, value=result)
        return result

    # ------------------------------------------------------------------
    # Modo 1: Callable customizado
    # ------------------------------------------------------------------

    def _apply_callable(self, df, callable_path: str):
        """Aplica uma função Python customizada ao DataFrame."""
        import pandas as pd

        parts = callable_path.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(
                f"callable inválido: '{callable_path}'. "
                "Use o formato 'modulo.funcao'."
            )
        module_path, func_name = parts
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        log.info("Aplicando callable '%s'.", callable_path)
        return func(df)

    # ------------------------------------------------------------------
    # Modo 2: Steps declarativos
    # ------------------------------------------------------------------

    def _apply_steps(self, df, steps: list[dict]) -> Any:
        """Aplica sequencialmente cada step ao DataFrame."""
        for i, step in enumerate(steps):
            step_type = step.get("type", "").lower()
            log.info("Step %d/%d: %s", i + 1, len(steps), step_type)
            handler = self._step_handlers.get(step_type)
            if handler is None:
                raise ValueError(
                    f"Step type desconhecido: '{step_type}'. "
                    f"Disponíveis: {sorted(self._step_handlers)}"
                )
            df = handler(self, df, step)
        return df

    # ── Handlers de steps ─────────────────────────────────────────────

    def _step_rename_columns(self, df, step: dict):
        mapping = step.get("mapping", {})
        return df.rename(columns=mapping)

    def _step_drop_columns(self, df, step: dict):
        columns = step.get("columns", [])
        return df.drop(columns=[c for c in columns if c in df.columns], errors="ignore")

    def _step_filter_rows(self, df, step: dict):
        query = step["query"]
        before = len(df)
        df = df.query(query)
        log.info("filter_rows '%s': %d → %d linhas.", query, before, len(df))
        return df

    def _step_fill_nulls(self, df, step: dict):
        per_column = step.get("columns", {})
        if per_column:
            return df.fillna(per_column)
        return df.fillna(step.get("value", 0))

    def _step_cast_types(self, df, step: dict):
        import pandas as pd

        type_map = {"datetime": "datetime64[ns]", "date": "datetime64[ns]"}
        for col, dtype in step.get("mapping", {}).items():
            if col not in df.columns:
                log.warning("cast_types: coluna '%s' não encontrada.", col)
                continue
            target_dtype = type_map.get(dtype, dtype)
            if "datetime" in target_dtype:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(target_dtype, errors="ignore")
        return df

    def _step_add_column(self, df, step: dict):
        col_name = step["name"]
        value = step.get("value")
        expression = step.get("expression")
        if expression:
            df = df.eval(f"{col_name} = {expression}")
        else:
            df[col_name] = value
        return df

    def _step_drop_duplicates(self, df, step: dict):
        subset = step.get("subset") or None
        before = len(df)
        df = df.drop_duplicates(subset=subset)
        log.info("drop_duplicates: %d → %d linhas.", before, len(df))
        return df

    def _step_sort(self, df, step: dict):
        by = step["by"]
        ascending = step.get("ascending", True)
        return df.sort_values(by=by, ascending=ascending).reset_index(drop=True)

    # Dispatch table (evita if/elif longo)
    _step_handlers = {
        "rename_columns": _step_rename_columns,
        "drop_columns": _step_drop_columns,
        "filter_rows": _step_filter_rows,
        "fill_nulls": _step_fill_nulls,
        "cast_types": _step_cast_types,
        "add_column": _step_add_column,
        "drop_duplicates": _step_drop_duplicates,
        "sort": _step_sort,
    }
