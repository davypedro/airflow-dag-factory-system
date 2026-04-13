"""
Base ETL Operator
=================
Classe base abstrata para todos os operadores ETL customizados.

Responsabilidades:
    - Recebe e valida ``task_config`` passado pelo DAG Factory.
    - Fornece hooks ``pre_execute_hook`` e ``post_execute_hook`` para
      instrumentação sem alterar a lógica de negócio.
    - Emite métricas básicas de duração via log estruturado.
    - Gerencia contexto de execução e garante que subclasses implementem
      ``_run``.

Como criar um operador customizado:
    1. Herde de ``BaseETLOperator``.
    2. Implemente o método ``_run(self, context) -> Any``.
    3. Declare os campos obrigatórios em ``REQUIRED_CONFIG_FIELDS``.
    4. Registre o novo tipo no ``dag_factory.py``.

Exemplo::

    class MyOperator(BaseETLOperator):
        REQUIRED_CONFIG_FIELDS = ["source", "destination"]

        def _run(self, context):
            source = self.task_config["source"]
            # ... lógica de negócio
            return {"rows_processed": 42}
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from airflow.models import BaseOperator

log = logging.getLogger(__name__)


class BaseETLOperator(BaseOperator, ABC):
    """
    Operador base para pipelines ETL.

    Parâmetros
    ----------
    task_config:
        Dicionário de configuração proveniente do bloco 'config' no YAML.
        Cada subclasse declara os campos obrigatórios em
        'REQUIRED_CONFIG_FIELDS'.
    **kwargs:
        Repassados ao 'BaseOperator' do Airflow.
    """

    # Subclasses que subscrevem e declaram os campos obrigatórios para validação automática
    REQUIRED_CONFIG_FIELDS: list[str] = []

    # Cor diferente para operadores ETL na UI do Airflow. Ajuda nos zoimétricos de quem usa duas lupas na cara.
    ui_color = "#f0e4d7"

    def __init__(self, task_config: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.task_config = task_config
        self._validate_config()

    # Validação

    def _validate_config(self) -> None:
        """Verifica se todos os campos obrigatórios estão presentes."""
        missing = [
            field
            for field in self.REQUIRED_CONFIG_FIELDS
            if field not in self.task_config
        ]
        if missing:
            raise ValueError(
                f"[{self.__class__.__name__}] Campos obrigatórios ausentes em "
                f"task_config: {missing}. Config recebida: {list(self.task_config)}"
            )

    # Ciclo de execução

    def execute(self, context: dict[str, Any]) -> Any:
        """
        Orquestra a execução: hook pré, lógica principal, hook pós.

        Não sobrescreva este método nas subclasses. Implemente ``_run``.
        """
        task_id = context["task_instance"].task_id
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        log.info(
            "[%s/%s] Iniciando tarefa (run_id=%s).",
            dag_id,
            task_id,
            run_id,
        )

        self.pre_execute_hook(context)
        start = time.monotonic()

        try:
            result = self._run(context)
        except Exception as exc:
            log.error(
                "[%s/%s] Falha na execução: %s",
                dag_id,
                task_id,
                exc,
            )
            raise

        elapsed = time.monotonic() - start
        log.info(
            "[%s/%s] Tarefa concluída em %.2fs.",
            dag_id,
            task_id,
            elapsed,
        )

        self.post_execute_hook(context, result)
        return result

    @abstractmethod
    def _run(self, context: dict[str, Any]) -> Any:
        """
        Contém a lógica de negócio do operador.

        Deve ser implementado por todas as subclasses.
        O valor retornado é enviado ao XCom automaticamente pelo Airflow.
        """

    # Hooks de extensão (opcionais nas subclasses!!!!!!)

    def pre_execute_hook(self, context: dict[str, Any]) -> None:
        """Executado antes de '_run'. Sobrescreve para adicionar lógica."""

    def post_execute_hook(self, context: dict[str, Any], result: Any) -> None:
        """Executado após '_run' com sucesso. Sobrescreve para adicionar lógica."""
