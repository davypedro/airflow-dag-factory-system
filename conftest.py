"""
conftest.py raiz
================
Executado pelo pytest antes de qualquer coleta de teste.

Injeta stubs mínimos de ``airflow`` no ``sys.modules`` para que os
operadores customizados possam ser importados e testados localmente
sem o Airflow instalado no ambiente virtual.

Os testes de integração reais (que precisam do Airflow de verdade)
rodam dentro do container Docker — não neste ambiente local.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

# ── Adiciona plugins/, include/ e dags/ ao sys.path ──────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "plugins"))
sys.path.insert(0, str(ROOT / "dags"))
sys.path.insert(0, str(ROOT))

# ── Cria stubs de airflow se não estiver instalado ────────────────────────

def _make_stub(name: str) -> ModuleType:
    mod = ModuleType(name)
    sys.modules[name] = mod
    return mod


if "airflow" not in sys.modules:
    # Módulos que os operadores importam diretamente
    airflow_stubs = [
        "airflow",
        "airflow.models",
        "airflow.operators",
        "airflow.operators.bash",
        "airflow.operators.empty",
        "airflow.operators.python",
        "airflow.hooks",
        "airflow.hooks.base",
    ]

    for stub_name in airflow_stubs:
        _make_stub(stub_name)

    # DAG stub — declarado antes de BaseOperator para evitar forward reference
    class _DAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
            self.description = kwargs.get("description", "")
            self.catchup = kwargs.get("catchup", False)
            self.tags = kwargs.get("tags", [])
            self.tasks: list = []

    # BaseOperator stub: classe que os operadores herdam
    class _BaseOperator:
        ui_color = "#ffffff"

        def __init__(self, task_id: str, dag=None, **kwargs):
            self.task_id = task_id
            self.dag = dag
            self.doc_md = ""
            self.upstream_task_ids: set[str] = set()
            self._downstream: list = []
            # Registra a tarefa na DAG, igual ao Airflow real
            if dag is not None:
                dag.tasks.append(self)

        def __rshift__(self, other):
            """Suporte ao operador >> para definir dependências."""
            other.upstream_task_ids.add(self.task_id)
            self._downstream.append(other)
            return other

        def execute(self, context):
            pass

    sys.modules["airflow.models"].BaseOperator = _BaseOperator  # type: ignore

    # Operadores nativos stub
    sys.modules["airflow.operators.bash"].BashOperator = type(  # type: ignore
        "BashOperator", (_BaseOperator,), {}
    )
    sys.modules["airflow.operators.empty"].EmptyOperator = type(  # type: ignore
        "EmptyOperator", (_BaseOperator,), {}
    )
    sys.modules["airflow.operators.python"].PythonOperator = type(  # type: ignore
        "PythonOperator", (_BaseOperator,), {}
    )

    sys.modules["airflow"].DAG = _DAG  # type: ignore
    _make_stub("airflow.dag")
