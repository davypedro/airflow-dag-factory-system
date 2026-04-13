"""
Transformações customizadas de negócio
=======================================
Funções Python usadas pelas DAGs via ``callable`` no YAML.

Cada função recebe um DataFrame pandas e retorna um DataFrame
transformado. Os dados de entrada vêm do XCom (resultado do
ExtractOperator ou de um TransformOperator anterior).

Como adicionar uma nova transformação:
    1. Implemente a função neste módulo (ou em outro arquivo em ``include/``).
    2. Referencie o caminho dotted no YAML:
       ``callable: include.utils.transformations.minha_funcao``

Convenções:
    - Funções não devem ter efeitos colaterais (sem I/O, sem estado global).
    - Documente os campos esperados e retornados.
    - Escreva um teste unitário correspondente em ``tests/test_transformations.py``.
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)


def normalize_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o catálogo de produtos carregado de um CSV.

    Operações:
        - Converte nomes de colunas para snake_case
        - Remove espaços extras de colunas de texto
        - Converte SKU para maiúsculas
        - Preenche preço nulo com 0.0
        - Descarta linhas sem product_id

    Parâmetros
    ----------
    df:
        DataFrame bruto proveniente do arquivo CSV.

    Retorna
    -------
    DataFrame normalizado.
    """
    # snake_case nos nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )

    # Remove linhas sem identificador
    before = len(df)
    df = df.dropna(subset=["product_id"])
    if len(df) < before:
        log.warning("Removidas %d linhas sem product_id.", before - len(df))

    # Limpeza de texto
    for col in ("product_name", "category", "description"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # SKU em maiúsculas
    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str).str.upper().str.strip()

    # Preço padrão
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)

    log.info("normalize_products: %d produto(s) após normalização.", len(df))
    return df


def add_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de auditoria ao DataFrame.

    Colunas adicionadas:
        - ``_etl_loaded_at``: timestamp UTC de carregamento
        - ``_etl_source``:    identificador fixo da fonte

    Útil como último step em qualquer pipeline antes do carregamento.
    """
    from datetime import datetime, timezone

    df = df.copy()
    df["_etl_loaded_at"] = datetime.now(timezone.utc)
    df["_etl_source"] = "airflow-etl"
    return df
