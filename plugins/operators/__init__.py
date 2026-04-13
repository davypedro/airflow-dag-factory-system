"""
Pacote de operadores ETL customizados.

Importações são lazy para que o pacote possa ser usado em ambientes
sem o Airflow instalado (ex.: testes locais com stubs).
"""

__all__ = [
    "ExtractOperator",
    "TransformOperator",
    "LoadOperator",
    "DataQualityOperator",
]
