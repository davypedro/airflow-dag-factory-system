"""
Testes dos operadores ETL customizados
=======================================
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Adiciona plugins/ ao path para importação
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))

from operators.base_etl_operator import BaseETLOperator
from operators.data_quality_operator import DataQualityCheckError, DataQualityOperator
from operators.transform_operator import TransformOperator


# ---------------------------------------------------------------------------
# BaseETLOperator
# ---------------------------------------------------------------------------

class ConcreteOperator(BaseETLOperator):
    """Implementação concreta para testes do BaseETLOperator."""
    REQUIRED_CONFIG_FIELDS = ["required_field"]

    def _run(self, context):
        return {"ok": True}


class TestBaseETLOperator:
    def test_missing_required_field_raises(self):
        with pytest.raises(ValueError, match="required_field"):
            ConcreteOperator(task_id="t", task_config={})

    def test_execute_calls_run(self, mock_airflow_context):
        op = ConcreteOperator(task_id="t", task_config={"required_field": "v"})
        result = op.execute(mock_airflow_context)
        assert result == {"ok": True}

    def test_pre_post_hooks_called(self, mock_airflow_context):
        calls = []

        class HookedOp(BaseETLOperator):
            REQUIRED_CONFIG_FIELDS = []
            def _run(self, context): return "result"
            def pre_execute_hook(self, ctx): calls.append("pre")
            def post_execute_hook(self, ctx, res): calls.append(f"post:{res}")

        op = HookedOp(task_id="t", task_config={})
        op.execute(mock_airflow_context)
        assert calls == ["pre", "post:result"]

    def test_exception_propagates(self, mock_airflow_context):
        class FailOp(BaseETLOperator):
            REQUIRED_CONFIG_FIELDS = []
            def _run(self, context): raise RuntimeError("boom")

        op = FailOp(task_id="t", task_config={})
        with pytest.raises(RuntimeError, match="boom"):
            op.execute(mock_airflow_context)


# ---------------------------------------------------------------------------
# TransformOperator — Steps declarativos
# ---------------------------------------------------------------------------

class TestTransformOperatorSteps:
    def _make_operator_and_context(self, steps, sample_records, mock_airflow_context):
        """Helper: popula XCom e cria o operador de transformação."""
        mock_airflow_context["task_instance"].xcom_push(
            key="extract_result", value=sample_records
        )
        op = TransformOperator(
            task_id="transform",
            task_config={
                "input_xcom_key": "extract_result",
                "input_task_id": None,
                "output_xcom_key": "transform_result",
                "steps": steps,
            },
        )
        return op, mock_airflow_context

    def test_rename_columns(self, sample_records, mock_airflow_context):
        op, ctx = self._make_operator_and_context(
            [{"type": "rename_columns", "mapping": {"order_id": "id"}}],
            sample_records,
            mock_airflow_context,
        )
        result = op.execute(ctx)
        assert "id" in result[0]
        assert "order_id" not in result[0]

    def test_drop_columns(self, sample_records, mock_airflow_context):
        op, ctx = self._make_operator_and_context(
            [{"type": "drop_columns", "columns": ["discount_amount"]}],
            sample_records,
            mock_airflow_context,
        )
        result = op.execute(ctx)
        assert "discount_amount" not in result[0]

    def test_filter_rows(self, sample_records, mock_airflow_context):
        op, ctx = self._make_operator_and_context(
            [{"type": "filter_rows", "query": "total_amount > 100"}],
            sample_records,
            mock_airflow_context,
        )
        result = op.execute(ctx)
        assert len(result) == 1
        assert result[0]["total_amount"] == 200.0

    def test_fill_nulls(self, mock_airflow_context):
        records = [{"a": None, "b": 1}, {"a": 2, "b": None}]
        mock_airflow_context["task_instance"].xcom_push(key="extract_result", value=records)
        op = TransformOperator(
            task_id="t",
            task_config={
                "input_xcom_key": "extract_result",
                "steps": [{"type": "fill_nulls", "value": 0}],
            },
        )
        result = op.execute(mock_airflow_context)
        assert result[0]["a"] == 0
        assert result[1]["b"] == 0

    def test_add_column_expression(self, sample_records, mock_airflow_context):
        op, ctx = self._make_operator_and_context(
            [{"type": "add_column", "name": "net", "expression": "total_amount - discount_amount"}],
            sample_records,
            mock_airflow_context,
        )
        result = op.execute(ctx)
        assert result[0]["net"] == pytest.approx(90.0)

    def test_drop_duplicates(self, mock_airflow_context):
        records = [
            {"order_id": 1, "val": "a"},
            {"order_id": 1, "val": "b"},  # duplicata
            {"order_id": 2, "val": "c"},
        ]
        mock_airflow_context["task_instance"].xcom_push(key="extract_result", value=records)
        op = TransformOperator(
            task_id="t",
            task_config={
                "input_xcom_key": "extract_result",
                "steps": [{"type": "drop_duplicates", "subset": ["order_id"]}],
            },
        )
        result = op.execute(mock_airflow_context)
        assert len(result) == 2

    def test_empty_xcom_raises(self, mock_airflow_context):
        op = TransformOperator(
            task_id="t",
            task_config={"input_xcom_key": "missing_key", "steps": []},
        )
        with pytest.raises(ValueError, match="XCom vazio"):
            op.execute(mock_airflow_context)

    def test_unknown_step_raises(self, sample_records, mock_airflow_context):
        op, ctx = self._make_operator_and_context(
            [{"type": "nonexistent_step"}],
            sample_records,
            mock_airflow_context,
        )
        with pytest.raises(ValueError, match="Step type desconhecido"):
            op.execute(ctx)


# ---------------------------------------------------------------------------
# DataQualityOperator
# ---------------------------------------------------------------------------

class TestDataQualityOperator:
    def _make_context_with_records(self, records, mock_airflow_context):
        mock_airflow_context["task_instance"].xcom_push(
            key="transform_result", value=records
        )
        return mock_airflow_context

    def test_row_count_pass(self, sample_records, mock_airflow_context):
        ctx = self._make_context_with_records(sample_records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"name": "rows ok", "type": "row_count", "min": 1}],
            },
        )
        result = op.execute(ctx)
        assert result["passed"] == 1
        assert result["failed"] == 0

    def test_row_count_fail_raises(self, mock_airflow_context):
        ctx = self._make_context_with_records([], mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"type": "row_count", "min": 1, "on_failure": "raise"}],
            },
        )
        with pytest.raises(DataQualityCheckError):
            op.execute(ctx)

    def test_row_count_fail_warn(self, mock_airflow_context):
        ctx = self._make_context_with_records([], mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"type": "row_count", "min": 1, "on_failure": "warn"}],
            },
        )
        result = op.execute(ctx)  # não deve lançar
        assert result["failed"] == 1

    def test_not_null_pass(self, sample_records, mock_airflow_context):
        ctx = self._make_context_with_records(sample_records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"type": "not_null", "columns": ["order_id"]}],
            },
        )
        result = op.execute(ctx)
        assert result["passed"] == 1

    def test_not_null_fail(self, mock_airflow_context):
        records = [{"order_id": None, "val": 1}, {"order_id": 2, "val": 3}]
        ctx = self._make_context_with_records(records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"type": "not_null", "columns": ["order_id"], "on_failure": "raise"}],
            },
        )
        with pytest.raises(DataQualityCheckError):
            op.execute(ctx)

    def test_unique_check(self, sample_records, mock_airflow_context):
        ctx = self._make_context_with_records(sample_records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"type": "unique", "columns": ["order_id"]}],
            },
        )
        result = op.execute(ctx)
        assert result["passed"] == 1

    def test_value_in_fail(self, sample_records, mock_airflow_context):
        ctx = self._make_context_with_records(sample_records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{
                    "type": "value_in",
                    "column": "status",
                    "values": ["delivered"],  # apenas "delivered" é válido
                    "on_failure": "raise",
                }],
            },
        )
        with pytest.raises(DataQualityCheckError):
            op.execute(ctx)

    def test_value_range_pass(self, sample_records, mock_airflow_context):
        ctx = self._make_context_with_records(sample_records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [{"type": "value_range", "column": "total_amount", "min": 0}],
            },
        )
        result = op.execute(ctx)
        assert result["passed"] == 1

    def test_multiple_checks_summary(self, sample_records, mock_airflow_context):
        ctx = self._make_context_with_records(sample_records, mock_airflow_context)
        op = DataQualityOperator(
            task_id="dq",
            task_config={
                "input_xcom_key": "transform_result",
                "checks": [
                    {"type": "row_count", "min": 1},
                    {"type": "not_null", "columns": ["order_id"]},
                    {"type": "unique", "columns": ["order_id"]},
                ],
            },
        )
        result = op.execute(ctx)
        assert result["total"] == 3
        assert result["passed"] == 3


# ---------------------------------------------------------------------------
# Testes de transformações utilitárias
# ---------------------------------------------------------------------------

class TestTransformations:
    def test_normalize_products(self):
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from include.utils.transformations import normalize_products

        df = pd.DataFrame([
            {"Product ID": "P001", "Product Name": "  Widget  ", "SKU": "ab-1234", "Price": "9.99"},
            {"Product ID": None,   "Product Name": "Ghost",       "SKU": "GH-0000", "Price": None},
        ])
        result = normalize_products(df)

        assert len(result) == 1  # linha sem product_id removida
        assert result.iloc[0]["product_name"] == "Widget"
        assert result.iloc[0]["sku"] == "AB-1234"
        assert result.iloc[0]["price"] == pytest.approx(9.99)

    def test_add_audit_columns(self):
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from include.utils.transformations import add_audit_columns

        df = pd.DataFrame([{"id": 1}, {"id": 2}])
        result = add_audit_columns(df)

        assert "_etl_loaded_at" in result.columns
        assert "_etl_source" in result.columns
        assert (result["_etl_source"] == "airflow-etl").all()
