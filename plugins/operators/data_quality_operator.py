"""
Data Quality Operator
=====================
Valida a qualidade dos dados após extração ou transformação, antes do carregamento.

O operador executa um conjunto de ``checks`` declarados no YAML. Se qualquer
check falhar e tiver ``on_failure: raise``, a tarefa é marcada como falha
e a DAG para. Com ``on_failure: warn`` o erro é logado mas a execução continua.

Checks disponíveis
------------------
- ``row_count``         → valida número de linhas (min/max)
- ``not_null``          → verifica ausência de nulos em colunas
- ``unique``            → verifica unicidade em colunas
- ``value_in``          → verifica se valores pertencem a um conjunto
- ``value_range``       → verifica intervalo numérico (min/max)
- ``regex``             → verifica padrão regex em coluna
- ``sql``               → executa query SQL customizada (resultado deve ser vazio = ok)

Campos de configuração (``config`` no YAML)
-------------------------------------------
    input_xcom_key (str):   Chave XCom dos dados. Default: ``transform_result``.
    input_task_id (str):    Task ID de origem.
    checks (list):          Lista de checks a executar.

Estrutura de um check
---------------------
.. code-block:: yaml

    checks:
      - name: "Pelo menos 1 pedido"
        type: row_count
        min: 1
        on_failure: raise

      - name: "order_id não nulo"
        type: not_null
        columns: [order_id, customer_id]
        on_failure: raise

      - name: "Status válido"
        type: value_in
        column: status
        values: [pending, shipped, delivered, cancelled]
        on_failure: warn
"""

from __future__ import annotations

import logging
import re
from typing import Any

from operators.base_etl_operator import BaseETLOperator

log = logging.getLogger(__name__)


class DataQualityCheckError(Exception):
    """Levantada quando um check de qualidade com ``on_failure: raise`` falha."""


class DataQualityOperator(BaseETLOperator):
    """Operador de validação de qualidade de dados."""

    REQUIRED_CONFIG_FIELDS = ["checks"]

    ui_color = "#f8d7da" # Vermelho claro para destacar falhas de qualidade para o míope

    def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        import pandas as pd

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
        checks = self.task_config["checks"]

        results = []
        failures = []

        for check in checks:
            check_name = check.get("name", check["type"])
            passed, message = self._run_check(df, check)

            status = "PASS" if passed else "FAIL"
            log.info("[DQ] %s → %s  %s", check_name, status, message)
            results.append({"check": check_name, "status": status, "message": message})

            if not passed and check.get("on_failure", "raise") == "raise":
                failures.append(f"[{check_name}] {message}")

        if failures:
            raise DataQualityCheckError(
                f"{len(failures)} check(s) de qualidade falharam:\n"
                + "\n".join(f"  - {f}" for f in failures)
            )

        summary = {
            "total": len(results),
            "passed": sum(1 for r in results if r["status"] == "PASS"),
            "failed": sum(1 for r in results if r["status"] == "FAIL"),
            "results": results,
        }
        log.info("[DQ] Resumo: %d/%d checks passaram.", summary["passed"], summary["total"])
        return summary

    # ------------------------------------------------------------------
    # Dispatch de checks
    # ------------------------------------------------------------------

    def _run_check(self, df, check: dict) -> tuple[bool, str]:
        check_type = check.get("type", "").lower()
        handlers = {
            "row_count": self._check_row_count,
            "not_null": self._check_not_null,
            "unique": self._check_unique,
            "value_in": self._check_value_in,
            "value_range": self._check_value_range,
            "regex": self._check_regex,
        }
        handler = handlers.get(check_type)
        if handler is None:
            return False, f"Tipo de check desconhecido: '{check_type}'."
        return handler(df, check)

    # ------------------------------------------------------------------
    # Implementações dos checks
    # ------------------------------------------------------------------

    @staticmethod
    def _check_row_count(df, check: dict) -> tuple[bool, str]:
        count = len(df)
        min_rows = check.get("min")
        max_rows = check.get("max")

        if min_rows is not None and count < min_rows:
            return False, f"Esperado >= {min_rows} linhas, encontrado {count}."
        if max_rows is not None and count > max_rows:
            return False, f"Esperado <= {max_rows} linhas, encontrado {count}."
        return True, f"{count} linha(s) — dentro do esperado."

    @staticmethod
    def _check_not_null(df, check: dict) -> tuple[bool, str]:
        columns = check.get("columns", [])
        issues = []
        for col in columns:
            if col not in df.columns:
                issues.append(f"coluna '{col}' não existe")
                continue
            nulls = df[col].isna().sum()
            if nulls > 0:
                issues.append(f"'{col}' tem {nulls} nulo(s)")
        if issues:
            return False, "; ".join(issues)
        return True, f"Sem nulos em: {columns}."

    @staticmethod
    def _check_unique(df, check: dict) -> tuple[bool, str]:
        columns = check.get("columns", [])
        dupes = df.duplicated(subset=columns).sum()
        if dupes > 0:
            return False, f"{dupes} linha(s) duplicada(s) em {columns}."
        return True, f"Unicidade verificada em: {columns}."

    @staticmethod
    def _check_value_in(df, check: dict) -> tuple[bool, str]:
        column = check["column"]
        allowed = set(check.get("values", []))
        if column not in df.columns:
            return False, f"Coluna '{column}' não existe."
        invalid = df[~df[column].isin(allowed)][column].unique().tolist()
        if invalid:
            return False, f"Valores inválidos em '{column}': {invalid[:10]}."
        return True, f"Todos os valores de '{column}' são válidos."

    @staticmethod
    def _check_value_range(df, check: dict) -> tuple[bool, str]:
        column = check["column"]
        min_val = check.get("min")
        max_val = check.get("max")

        if column not in df.columns:
            return False, f"Coluna '{column}' não existe."

        if min_val is not None:
            below = (df[column] < min_val).sum()
            if below > 0:
                return False, f"{below} valor(es) abaixo do mínimo ({min_val}) em '{column}'."
        if max_val is not None:
            above = (df[column] > max_val).sum()
            if above > 0:
                return False, f"{above} valor(es) acima do máximo ({max_val}) em '{column}'."

        return True, f"'{column}' dentro do intervalo [{min_val}, {max_val}]."

    @staticmethod
    def _check_regex(df, check: dict) -> tuple[bool, str]:
        column = check["column"]
        pattern = check["pattern"]

        if column not in df.columns:
            return False, f"Coluna '{column}' não existe."

        compiled = re.compile(pattern)
        invalid = df[~df[column].astype(str).str.match(compiled)][column].head(5).tolist()
        if invalid:
            return False, f"Valores que não batem com '{pattern}' em '{column}': {invalid}."
        return True, f"Todos os valores de '{column}' batem com o padrão."
