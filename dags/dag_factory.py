"""
DAG Factory
===========
Gera DAGs do Airflow dinamicamente a partir de arquivos YAML localizados em
``dags/configs/``.

Fluxo:
    Arquivo YAML → ``load_dags_from_configs`` → ``build_dag`` → DAG object
    → globals() → Scheduler do Airflow descobre automaticamente

Como adicionar uma nova DAG:
    1. Crie um arquivo ``.yaml`` em ``dags/configs/``.
    2. Siga o schema documentado em ``docs/yaml_schema.md``.
    3. O Scheduler detecta o novo arquivo no próximo ciclo de varredura.

Tipos de tarefa suportados:
    - ``extract``      → ExtractOperator (operador customizado ETL)
    - ``transform``    → TransformOperator (operador customizado ETL)
    - ``load``         → LoadOperator (operador customizado ETL)
    - ``data_quality`` → DataQualityOperator (validação de dados)
    - ``python``       → PythonOperator nativo do Airflow
    - ``bash``         → BashOperator nativo do Airflow
    - ``empty``        → EmptyOperator (dummy / ponto de sincronização)
"""

from __future__ import annotations

import importlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

CONFIGS_DIR = Path(__file__).parent / "configs"

# ---------------------------------------------------------------------------
# Registro de operadores
# ---------------------------------------------------------------------------

def _build_operator_registry() -> dict[str, type]:
    """
    Constrói o dicionário que mapeia ``type`` (YAML) → classe do operador.

    Operadores customizados são importados opcionalmente para que o factory
    funcione mesmo em ambientes sem os plugins instalados (ex.: durante testes
    unitários isolados).
    """
    registry: dict[str, type] = {
        "python": PythonOperator,
        "bash": BashOperator,
        "empty": EmptyOperator,
    }

    custom_operators = {
        "extract": ("operators.extract_operator", "ExtractOperator"),
        "transform": ("operators.transform_operator", "TransformOperator"),
        "load": ("operators.load_operator", "LoadOperator"),
        "data_quality": ("operators.data_quality_operator", "DataQualityOperator"),
    }

    for op_type, (module_path, class_name) in custom_operators.items():
        try:
            module = importlib.import_module(module_path)
            registry[op_type] = getattr(module, class_name)
        except (ImportError, AttributeError) as exc:
            log.warning(
                "Operador '%s' não disponível (%s: %s). "
                "Instale os plugins para habilitar este tipo de tarefa.",
                op_type,
                type(exc).__name__,
                exc,
            )

    return registry


# Parsing de configuração

def _parse_default_args(raw: dict[str, Any]) -> dict[str, Any]:
    """Converte o bloco ``default_args`` do YAML para o formato do Airflow."""
    return {
        "owner": raw.get("owner", "airflow"),
        "depends_on_past": raw.get("depends_on_past", False),
        "email": raw.get("email", []),
        "email_on_failure": raw.get("email_on_failure", False),
        "email_on_retry": raw.get("email_on_retry", False),
        "retries": raw.get("retries", 1),
        "retry_delay": timedelta(minutes=raw.get("retry_delay_minutes", 5)),
    }


def _parse_start_date(raw: str) -> datetime:
    """Converte string ISO 'YYYY-MM-DD' para objeto 'datetime."""
    try:
        return datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"start_date inválido: '{raw}'. Use o formato YYYY-MM-DD."
        ) from exc


def _resolve_callable(dotted_path: str):
    """
    Resolve um caminho pontilhado (``modulo.submodulo.funcao``) em callable.

    Exemplo:
        ``include.utils.transformations.clean_nulls``
        →  importa ``include.utils.transformations`` e retorna ``clean_nulls``
    """
    parts = dotted_path.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(
            f"callable inválido: '{dotted_path}'. "
            "Use o formato 'modulo.submodulo.funcao'."
        )
    module_path, func_name = parts
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


# ---------------------------------------------------------------------------
# Construção de tarefas
# ---------------------------------------------------------------------------

def _build_task(
    task_cfg: dict[str, Any],
    dag: DAG,
    operator_registry: dict[str, type],
) -> Any:
    """
    Instancia um operador Airflow a partir da configuração de uma tarefa YAML.

    Parâmetros
    ----------
    task_cfg:
        Dicionário com a definição da tarefa (campos: id, type, config, ...).
    dag:
        DAG à qual a tarefa pertence.
    operator_registry:
        Mapeamento de tipo → classe do operador.

    Retorna
    -------
    Instância do operador Airflow.
    """
    task_id: str = task_cfg["id"]
    task_type: str = task_cfg.get("type", "empty").lower()
    config: dict = task_cfg.get("config", {})
    description: str = task_cfg.get("description", "")

    operator_cls = operator_registry.get(task_type)
    if operator_cls is None:
        raise ValueError(
            f"Tipo de tarefa desconhecido: '{task_type}' (tarefa '{task_id}'). "
            f"Tipos disponíveis: {sorted(operator_registry)}"
        )

    kwargs: dict[str, Any] = {"task_id": task_id, "dag": dag}

    if task_type == "python":
        callable_path = config.get("callable")
        if not callable_path:
            raise ValueError(
                f"Tarefa '{task_id}' do tipo 'python' exige o campo 'config.callable'."
            )
        kwargs["python_callable"] = _resolve_callable(callable_path)
        kwargs["op_kwargs"] = config.get("op_kwargs", {})

    elif task_type == "bash":
        bash_command = config.get("bash_command")
        if not bash_command:
            raise ValueError(
                f"Tarefa '{task_id}' do tipo 'bash' exige o campo 'config.bash_command'."
            )
        kwargs["bash_command"] = bash_command
        kwargs["env"] = config.get("env", {})

    elif task_type in ("extract", "transform", "load", "data_quality"):
        kwargs["task_config"] = config

    # EmptyOperator não precisa de kwargs extras

    task = operator_cls(**kwargs)
    task.doc_md = description
    return task


# ---------------------------------------------------------------------------
# Construção da DAG
# ---------------------------------------------------------------------------

def build_dag(config: dict[str, Any]) -> DAG:
    """
    Constrói e retorna um objeto ``DAG`` a partir de um dicionário de config.

    Parâmetros
    ----------
    config:
        Dicionário parsed de um arquivo YAML. Deve conter a chave ``dag``.

    Retorna
    -------
    Objeto ``DAG`` completamente configurado com todas as tarefas e dependências.
    """
    dag_cfg: dict[str, Any] = config["dag"]
    operator_registry = _build_operator_registry()

    dag = DAG(
        dag_id=dag_cfg["id"],
        description=dag_cfg.get("description", ""),
        schedule=dag_cfg.get("schedule", None),
        start_date=_parse_start_date(dag_cfg["start_date"]),
        catchup=dag_cfg.get("catchup", False),
        max_active_runs=dag_cfg.get("max_active_runs", 1),
        tags=dag_cfg.get("tags", []),
        default_args=_parse_default_args(dag_cfg.get("default_args", {})),
        doc_md=dag_cfg.get("doc_md", dag_cfg.get("description", "")),
        render_template_as_native_obj=dag_cfg.get("render_template_as_native_obj", False),
    )

    # Criar tasks
    task_map: dict[str, Any] = {}
    for task_cfg in dag_cfg.get("tasks", []):
        task = _build_task(task_cfg, dag, operator_registry)
        task_map[task_cfg["id"]] = task

    # Definir dependências
    for task_cfg in dag_cfg.get("tasks", []):
        task_id = task_cfg["id"]
        for dep_id in task_cfg.get( "depends_on", []):
            if dep_id not in task_map:
                raise ValueError(
                    f"Tarefa '{task_id}' depende de '{dep_id}', "
                    "mas essa tarefa não foi encontrada nesta DAG."
                )
            task_map[dep_id] >> task_map[task_id]

    log.info(
        "DAG '%s' construída com %d tarefa(s).",
        dag.dag_id,
        len(task_map),
    )
    return dag


# Carregamento de todas as configs

def load_dags_from_configs(configs_dir: Path) -> dict[str, DAG]:
    """
    Varre o diretório de configs e constrói uma DAG para cada arquivo YAML.

    Arquivos com erro de parsing são ignorados com log de warning para que
    uma config inválida não derrube as outras DAGs.

    Parâmetros

    configs_dir:
        Caminho para o diretório contendo os arquivos '.yaml'.

    Retorna:

    Dicionário '{dag_id: DAG}' pronto para ser injetado no namespace global.
    """
    dags: dict[str, DAG] = {}

    if not configs_dir.exists():
        log.warning(
            "Diretório de configs não encontrado: %s. "
            "Nenhuma DAG será carregada.",
            configs_dir,
        )
        return dags

    yaml_files = sorted(configs_dir.glob("**/*.yaml"))

    if not yaml_files:
        log.info("Nenhum arquivo .yaml encontrado em %s.", configs_dir)
        return dags

    for yaml_file in yaml_files:
        try:
            with yaml_file.open("r", encoding="utf-8") as f:
                raw = yaml.safe_load(f)

            if not raw or "dag" not in raw:
                log.warning(
                    "Arquivo ignorado (chave 'dag' ausente): %s", yaml_file
                )
                continue

            dag = build_dag(raw)

            if dag.dag_id in dags:
                log.warning(
                    "dag_id duplicado '%s' encontrado em %s. "
                    "A DAG anterior será sobrescrita.",
                    dag.dag_id,
                    yaml_file,
                )

            dags[dag.dag_id] = dag

        except Exception:
            log.exception("Falha ao carregar DAG de %s", yaml_file)

    log.info("%d DAG(s) carregada(s) de %s.", len(dags), configs_dir)
    return dags


# ---------------------------------------------------------------------------
# Ponto de entrada — descoberta automática pelo Airflow Scheduler
# ---------------------------------------------------------------------------
# O Scheduler descobre DAGs buscando objetos ``DAG`` no namespace global de
# cada módulo Python presente no ``dags_folder``.  Injetamos aqui todas as
# DAGs geradas para que sejam detectadas automaticamente.

_discovered_dags = load_dags_from_configs(CONFIGS_DIR)
globals().update(_discovered_dags)
