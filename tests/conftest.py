"""
Fixtures compartilhadas para todos os testes.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures de configuração YAML
# ---------------------------------------------------------------------------

@pytest.fixture()
def minimal_dag_config() -> dict:
    """Configuração mínima válida para criação de DAG."""
    return {
        "dag": {
            "id": "test_dag",
            "description": "DAG de teste",
            "start_date": "2024-01-01",
            "schedule": None,
            "tasks": [],
        }
    }


@pytest.fixture()
def full_etl_config() -> dict:
    """Configuração completa com pipeline ETL de 3 etapas."""
    return {
        "dag": {
            "id": "test_full_etl",
            "description": "Pipeline ETL completo para testes",
            "start_date": "2024-01-01",
            "schedule": "@daily",
            "catchup": False,
            "tags": ["test"],
            "default_args": {
                "owner": "test",
                "retries": 0,
            },
            "tasks": [
                {
                    "id": "extract",
                    "type": "empty",
                    "description": "Extração (mock)",
                    "depends_on": [],
                },
                {
                    "id": "transform",
                    "type": "empty",
                    "description": "Transformação (mock)",
                    "depends_on": ["extract"],
                },
                {
                    "id": "load",
                    "type": "empty",
                    "description": "Carregamento (mock)",
                    "depends_on": ["transform"],
                },
            ],
        }
    }


@pytest.fixture()
def sample_yaml_file(tmp_path: Path, full_etl_config) -> Path:
    """Arquivo YAML temporário com configuração ETL completa."""
    import yaml

    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    yaml_file = config_dir / "test_etl.yaml"
    yaml_file.write_text(yaml.dump(full_etl_config), encoding="utf-8")
    return yaml_file


# ---------------------------------------------------------------------------
# Fixtures de dados
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_records() -> list[dict]:
    """Dataset simples para testes de operadores."""
    return [
        {"order_id": 1, "customer_id": 10, "total_amount": 100.0, "status": "delivered", "discount_amount": 10.0},
        {"order_id": 2, "customer_id": 20, "total_amount": 200.0, "status": "shipped",   "discount_amount": 0.0},
        {"order_id": 3, "customer_id": 10, "total_amount": 50.0,  "status": "pending",   "discount_amount": 5.0},
    ]


# ---------------------------------------------------------------------------
# Fixtures de contexto Airflow (mock)
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_airflow_context(sample_records):
    """
    Simula o ``context`` passado pelo Airflow ao ``execute()`` dos operadores.

    O XCom é armazenado em memória (dicionário) para facilitar assertions.
    """
    xcom_store: dict = {}

    ti = MagicMock()
    ti.task_id = "test_task"

    def xcom_push(key, value):
        xcom_store[key] = value

    def xcom_pull(task_ids=None, key=None):
        return xcom_store.get(key)

    ti.xcom_push.side_effect = xcom_push
    ti.xcom_pull.side_effect = xcom_pull

    dag = MagicMock()
    dag.dag_id = "test_dag"

    return {
        "task_instance": ti,
        "dag": dag,
        "run_id": "manual__test",
        "_xcom_store": xcom_store,  # acesso direto nos testes
    }
