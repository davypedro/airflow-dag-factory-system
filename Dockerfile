# ============================================================
#  Dockerfile — Airflow ETL
# ============================================================
#  Baseado na imagem oficial do Airflow 2.9.
#  Instala dependências extras e copia os plugins customizados.
#
#  Build:
#    docker build -t airflow-etl:latest .
#
#  Não faça build manualmente em produção — use o docker-compose.
# ============================================================

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ── Metadados ─────────────────────────────────────────────────────────────
LABEL maintainer="data-engineering@empresa.com" \
      description="Airflow ETL com DAGs geradas por YAML" \
      version="1.0.0"

# ── Dependências Python extras ─────────────────────────────────────────────
# Copiamos apenas o requirements.txt primeiro para aproveitar cache de layers
COPY requirements.txt /requirements.txt

# Instalação como usuário airflow (requerido pela imagem oficial)
RUN pip install --no-cache-dir -r /requirements.txt

# ── Plugins customizados ────────────────────────────────────────────────────
# O diretório plugins/ é montado como volume em dev, mas copiado na imagem
# de produção para garantir imutabilidade
COPY --chown=airflow:root plugins/ ${AIRFLOW_HOME}/plugins/

# ── Scripts de utilitários ──────────────────────────────────────────────────
COPY --chown=airflow:root include/ ${AIRFLOW_HOME}/include/

# ── Variáveis de ambiente padrão ────────────────────────────────────────────
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False \
    PYTHONPATH="${AIRFLOW_HOME}/plugins:${AIRFLOW_HOME}/include:${PYTHONPATH}"

# ── Healthcheck ─────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}" \
    || exit 1
