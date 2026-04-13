"""
Testes do DAG Factory
=====================
Valida a geração dinâmica de DAGs a partir de arquivos YAML.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _import_factory():
    """Importa o módulo dag_factory isolando o carregamento automático."""
    import importlib
    import sys

    # Remove cache para reimportar limpo
    sys.modules.pop("dag_factory", None)

    with patch("dag_factory.load_dags_from_configs", return_value={}):
        import dag_factory
        return dag_factory


# ---------------------------------------------------------------------------
# Testes de _parse_start_date
# ---------------------------------------------------------------------------

class TestParseStartDate:
    def test_valid_date(self):
        factory = _import_factory()
        from datetime import datetime
        result = factory._parse_start_date("2024-03-15")
        assert result == datetime(2024, 3, 15)

    def test_invalid_date_raises(self):
        factory = _import_factory()
        with pytest.raises(ValueError, match="start_date inválido"):
            factory._parse_start_date("15/03/2024")

    def test_invalid_format_raises(self):
        factory = _import_factory()
        with pytest.raises(ValueError):
            factory._parse_start_date("not-a-date")


# ---------------------------------------------------------------------------
# Testes de _parse_default_args
# ---------------------------------------------------------------------------

class TestParseDefaultArgs:
    def test_defaults_when_empty(self):
        factory = _import_factory()
        result = factory._parse_default_args({})
        assert result["owner"] == "airflow"
        assert result["retries"] == 1
        assert result["depends_on_past"] is False

    def test_custom_values(self):
        from datetime import timedelta
        factory = _import_factory()
        result = factory._parse_default_args({
            "owner": "my-team",
            "retries": 3,
            "retry_delay_minutes": 10,
        })
        assert result["owner"] == "my-team"
        assert result["retries"] == 3
        assert result["retry_delay"] == timedelta(minutes=10)


# ---------------------------------------------------------------------------
# Testes de build_dag
# ---------------------------------------------------------------------------

class TestBuildDag:
    def test_minimal_config(self, minimal_dag_config):
        factory = _import_factory()
        dag = factory.build_dag(minimal_dag_config)
        assert dag.dag_id == "test_dag"
        assert dag.description == "DAG de teste"
        assert len(dag.tasks) == 0

    def test_full_etl_config(self, full_etl_config):
        factory = _import_factory()
        dag = factory.build_dag(full_etl_config)
        assert dag.dag_id == "test_full_etl"
        assert len(dag.tasks) == 3

    def test_task_dependencies(self, full_etl_config):
        factory = _import_factory()
        dag = factory.build_dag(full_etl_config)
        task_map = {t.task_id: t for t in dag.tasks}

        assert "extract" in task_map["transform"].upstream_task_ids
        assert "transform" in task_map["load"].upstream_task_ids

    def test_unknown_task_type_raises(self, minimal_dag_config):
        factory = _import_factory()
        minimal_dag_config["dag"]["tasks"] = [{
            "id": "bad_task",
            "type": "nonexistent_type",
            "depends_on": [],
        }]
        with pytest.raises(ValueError, match="Tipo de tarefa desconhecido"):
            factory.build_dag(minimal_dag_config)

    def test_unknown_dependency_raises(self, minimal_dag_config):
        factory = _import_factory()
        minimal_dag_config["dag"]["tasks"] = [{
            "id": "task_a",
            "type": "empty",
            "depends_on": ["ghost_task"],
        }]
        with pytest.raises(ValueError, match="ghost_task"):
            factory.build_dag(minimal_dag_config)

    def test_dag_tags(self, full_etl_config):
        factory = _import_factory()
        dag = factory.build_dag(full_etl_config)
        assert "test" in dag.tags

    def test_catchup_false(self, full_etl_config):
        factory = _import_factory()
        dag = factory.build_dag(full_etl_config)
        assert dag.catchup is False

    def test_python_task_requires_callable(self, minimal_dag_config):
        factory = _import_factory()
        minimal_dag_config["dag"]["tasks"] = [{
            "id": "py_task",
            "type": "python",
            "config": {},  # sem callable
            "depends_on": [],
        }]
        with pytest.raises(ValueError, match="callable"):
            factory.build_dag(minimal_dag_config)

    def test_bash_task_requires_bash_command(self, minimal_dag_config):
        factory = _import_factory()
        minimal_dag_config["dag"]["tasks"] = [{
            "id": "bash_task",
            "type": "bash",
            "config": {},  # sem bash_command
            "depends_on": [],
        }]
        with pytest.raises(ValueError, match="bash_command"):
            factory.build_dag(minimal_dag_config)


# ---------------------------------------------------------------------------
# Testes de load_dags_from_configs
# ---------------------------------------------------------------------------

class TestLoadDagsFromConfigs:
    def test_loads_yaml_files(self, sample_yaml_file, tmp_path):
        factory = _import_factory()
        configs_dir = tmp_path / "configs"
        dags = factory.load_dags_from_configs(configs_dir)
        assert "test_full_etl" in dags

    def test_missing_directory_returns_empty(self, tmp_path):
        factory = _import_factory()
        dags = factory.load_dags_from_configs(tmp_path / "nonexistent")
        assert dags == {}

    def test_invalid_yaml_is_skipped(self, tmp_path):
        factory = _import_factory()
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        # YAML inválido (sem chave 'dag')
        (configs_dir / "bad.yaml").write_text("invalid: content\nno: dag key", encoding="utf-8")

        # YAML válido
        valid = {
            "dag": {
                "id": "valid_dag",
                "start_date": "2024-01-01",
                "tasks": [],
            }
        }
        (configs_dir / "good.yaml").write_text(yaml.dump(valid), encoding="utf-8")

        dags = factory.load_dags_from_configs(configs_dir)
        assert "valid_dag" in dags
        assert len(dags) == 1

    def test_multiple_yaml_files(self, tmp_path):
        factory = _import_factory()
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()

        for i in range(3):
            cfg = {"dag": {"id": f"dag_{i}", "start_date": "2024-01-01", "tasks": []}}
            (configs_dir / f"dag_{i}.yaml").write_text(yaml.dump(cfg), encoding="utf-8")

        dags = factory.load_dags_from_configs(configs_dir)
        assert len(dags) == 3

    def test_scans_subdirectories(self, tmp_path):
        factory = _import_factory()
        configs_dir = tmp_path / "configs"
        sub_dir = configs_dir / "subdir"
        sub_dir.mkdir(parents=True)

        cfg = {"dag": {"id": "nested_dag", "start_date": "2024-01-01", "tasks": []}}
        (sub_dir / "nested.yaml").write_text(yaml.dump(cfg), encoding="utf-8")

        dags = factory.load_dags_from_configs(configs_dir)
        assert "nested_dag" in dags
