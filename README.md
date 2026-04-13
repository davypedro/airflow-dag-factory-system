# Airflow ETL — Sistema de Pipelines Orientado a Configuração

Sistema de ETL orquestrado pelo Apache Airflow onde **cada pipeline é definido por um arquivo YAML** — sem escrever Python para casos comuns. Um DAG Factory lê os arquivos de configuração e gera as DAGs automaticamente, enquanto operadores customizados cuidam de extração, transformação, validação e carga.

```
arquivo YAML  →  DAG Factory  →  Airflow Scheduler  →  ETL Pipeline
```

---

## Sumário

- [Arquitetura](#arquitetura)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Início rápido](#início-rápido)
- [Pipelines incluídos](#pipelines-incluídos)
- [Como criar uma nova DAG](#como-criar-uma-nova-dag)
- [Operadores disponíveis](#operadores-disponíveis)
- [Fluxo de dados via XCom](#fluxo-de-dados-via-xcom)
- [Validação de qualidade](#validação-de-qualidade)
- [Transformações declarativas](#transformações-declarativas)
- [Funções Python customizadas](#funções-python-customizadas)
- [Como estender o sistema](#como-estender-o-sistema)
- [Testes](#testes)
- [Escalabilidade](#escalabilidade)
- [Troubleshooting](#troubleshooting)

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│  dags/configs/*.yaml                                            │
│  (definição declarativa dos pipelines)                          │
└────────────────────────┬────────────────────────────────────────┘
                         │ lido na inicialização
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  dags/dag_factory.py                                            │
│  • varre dags/configs/ buscando *.yaml                          │
│  • instancia DAG + tasks + dependências                         │
│  • injeta no globals() → Scheduler descobre automaticamente     │
└────────────────────────┬────────────────────────────────────────┘
                         │ gera
                         ▼
┌───────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Extract  │──▶│ DataQuality  │──▶│  Transform   │──▶│    Load      │
│ Operator  │   │  Operator    │   │  Operator    │   │  Operator    │
└───────────┘   └──────────────┘   └──────────────┘   └──────────────┘
      │                │                  │                  │
      └────────────────┴──────────────────┘                  │
                    XCom (JSON no Postgres)                   │
                                                             ▼
                                                    ┌─────────────────┐
                                                    │  Postgres / S3  │
                                                    │  MySQL / Arquivo│
                                                    └─────────────────┘
```

Todos os operadores herdam de `BaseETLOperator`, que gerencia o ciclo de execução, logs estruturados e hooks de extensão.

---

## Estrutura do projeto

```
airflow-etl/
├── dags/
│   ├── dag_factory.py              # Lê YAMLs e gera DAGs automaticamente
│   └── configs/                    # ← Adicione seus pipelines aqui
│       ├── example_csv_etl.yaml    # Produtos: CSV → dim_products
│       ├── example_postgres_etl.yaml  # Pedidos: Postgres → DWH
│       ├── customers_etl.yaml      # Clientes: CSV → dim_customers
│       ├── orders_etl.yaml         # Pedidos: CSV → fact_orders
│       ├── order_items_etl.yaml    # Itens: CSV → fact_order_items
│       └── inventory_etl.yaml      # Estoque: CSV → dim_inventory
│
├── plugins/
│   └── operators/                  # Operadores ETL customizados
│       ├── base_etl_operator.py    # Classe base abstrata
│       ├── extract_operator.py     # Extração (Postgres, MySQL, S3, HTTP, arquivo)
│       ├── transform_operator.py   # Transformação (steps declarativos ou callable)
│       ├── load_operator.py        # Carga (Postgres, MySQL, S3, arquivo)
│       └── data_quality_operator.py # Validação de qualidade
│
├── include/
│   ├── sql/
│   │   ├── extract_orders.sql      # Query de extração de pedidos
│   │   └── init_dwh.sql            # DDL das tabelas do DWH (executado na init)
│   └── utils/
│       └── transformations.py      # Funções Python de transformação customizada
│
├── data/                           # Arquivos CSV de entrada (montado como volume)
│   ├── products.csv
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── inventory.csv
│
├── tests/
│   ├── conftest.py
│   ├── test_dag_factory.py
│   └── test_operators.py
│
├── docs/
│   └── yaml_schema.md              # Schema completo do YAML
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── requirements-dev.txt
└── .env.example
```

---

## Início rápido

### Pré-requisitos

- Docker ≥ 24
- Docker Compose ≥ 2.20

### 1. Configure o ambiente

```bash
cp .env.example .env
```

Gere os valores de segurança e preencha no `.env`:

```bash
# Fernet key (criptografia de conexões no Airflow)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret key (sessão do Flask/Webserver)
python -c "import secrets; print(secrets.token_hex(32))"
```

### 2. Inicialize o banco de metadados

```bash
docker compose up airflow-init
```

Aguarde a mensagem `Inicialização concluída.` O container encerra sozinho.

### 3. Suba os serviços

```bash
docker compose up -d
```

### 4. Verifique o status

```bash
docker compose ps
```

Todos os serviços devem estar `healthy` (~1-2 minutos):

| Serviço | Descrição | Porta |
|---|---|---|
| `airflow-postgres` | Banco de metadados + DWH | 5432 |
| `airflow-webserver` | UI do Airflow | 8080 |
| `airflow-scheduler` | Agendador de DAGs | — |

### 5. Acesse a UI

Abra [http://localhost:8080](http://localhost:8080) com as credenciais do `.env` (padrão: `admin` / `admin`).

### 6. Configure a connection do DWH

**Admin → Connections → +**

| Campo | Valor |
|---|---|
| Connection Id | `postgres_dwh` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Database | `airflow` |
| Login | `airflow` |
| Password | `airflow_secret_change_me` |
| Port | `5432` |

### 7. Dispare um pipeline

Ative o toggle de qualquer DAG e clique em **▶ Trigger DAG**.

---

## Pipelines incluídos

Os pipelines simulam um e-commerce completo. Os horários foram ordenados para respeitar dependências entre dimensões e fatos.

| DAG | Horário | Fonte | Tabela destino | Descrição |
|---|---|---|---|---|
| `inventory_etl` | 04:00 diário | `data/inventory.csv` | `dim_inventory` | Posição de estoque por warehouse |
| `customers_etl` | 05:00 diário | `data/customers.csv` | `dim_customers` | Clientes ativos com segmentação |
| `example_csv_etl` | seg 06:00 | `data/products.csv` | `dim_products` | Catálogo de produtos com normalização |
| `orders_etl` | 06:00 diário | `data/orders.csv` | `fact_orders` | Pedidos com remoção de colunas internas |
| `order_items_etl` | 06:30 diário | `data/order_items.csv` | `fact_order_items` | Itens com cálculo de `line_total` |
| `example_postgres_etl` | diário | Postgres (`postgres_source`) | `fact_orders` | Extração via SQL com transformações |

### Schema do DWH

```
dim_inventory          dim_customers          dim_products
  product_id (PK)        customer_id (PK)       product_id (PK)
  sku                    full_name              product_name
  stock_quantity         email                  sku
  reserved_quantity      segment                category
  warehouse              state                  price
  restock_threshold      registration_date      description
  unit_cost              is_active

fact_orders            fact_order_items
  order_id (PK)          item_id (PK)
  customer_id            order_id
  order_date             product_id
  status                 quantity
  total_amount           unit_price
  discount_amount        discount_pct
  net_amount             line_total
  payment_method
```

---

## Como criar uma nova DAG

Crie um arquivo `.yaml` em `dags/configs/`. A DAG aparece automaticamente na UI no próximo ciclo de varredura (≈ 30 segundos), sem restart e sem tocar em Python.

**Exemplo mínimo — leitura de CSV com validação e carga:**

```yaml
dag:
  id: meu_pipeline
  description: "Pipeline de exemplo"
  schedule: "@daily"
  start_date: "2024-01-01"
  catchup: false
  tags: [meu-time, etl]

  default_args:
    owner: meu-time
    retries: 2
    retry_delay_minutes: 5

  tasks:
    - id: extract
      type: extract
      config:
        source_type: file
        path: /opt/airflow/data/meu_arquivo.csv
        file_format: csv
        output_xcom_key: dados_brutos
      depends_on: []

    - id: validar
      type: data_quality
      config:
        input_xcom_key: dados_brutos
        input_task_id: extract
        checks:
          - name: "Ao menos 1 linha"
            type: row_count
            min: 1
            on_failure: raise
          - name: "id não nulo"
            type: not_null
            columns: [id]
            on_failure: raise
      depends_on: [extract]

    - id: carregar
      type: load
      config:
        destination_type: postgres
        conn_id: postgres_dwh
        table: minha_tabela
        schema: public
        load_strategy: upsert
        upsert_keys: [id]
        input_xcom_key: dados_brutos
        input_task_id: extract
      depends_on: [validar]
```

> Consulte [`docs/yaml_schema.md`](docs/yaml_schema.md) para o schema completo com todos os campos e exemplos.

---

## Operadores disponíveis

| Tipo YAML | Operador | Cor na UI | Fontes / Destinos |
|---|---|---|---|
| `extract` | `ExtractOperator` | Verde claro | Postgres, MySQL, S3, HTTP, CSV/JSON/Parquet |
| `transform` | `TransformOperator` | Amarelo claro | Steps declarativos ou callable Python |
| `load` | `LoadOperator` | Azul claro | Postgres, MySQL, S3, arquivo local |
| `data_quality` | `DataQualityOperator` | Vermelho claro | Validações em memória sobre DataFrame |
| `python` | `PythonOperator` (nativo) | — | Qualquer callable Python |
| `bash` | `BashOperator` (nativo) | — | Qualquer comando shell |
| `empty` | `EmptyOperator` (nativo) | — | Sincronização / marcadores |

---

## Fluxo de dados via XCom

Os dados trafegam entre tasks pelo **XCom** do Airflow (armazenado em JSON no Postgres). Cada operador lê de uma chave e publica em outra:

```
ExtractOperator
  xcom_push(key="raw_products", value=[{...}, {...}])
       │
       ▼
DataQualityOperator
  xcom_pull(key="raw_products")  → valida regras
       │
       ▼ (mesmos dados)
TransformOperator
  xcom_pull(key="raw_products")  → transforma
  xcom_push(key="clean_products", value=[{...}, {...}])
       │
       ▼
LoadOperator
  xcom_pull(key="clean_products") → grava no banco
```

**Observação técnica:** todos os valores são serializados para JSON nativo antes do push (`json.loads(df.to_json(...))`) para garantir compatibilidade com o backend de XCom do Airflow, convertendo automaticamente tipos numpy (`int64`, `bool_`) e `pd.Timestamp` para tipos Python nativos.

---

## Validação de qualidade

O `DataQualityOperator` executa checks declarados no YAML antes do carregamento. Se um check com `on_failure: raise` falhar, a task é marcada como falha e o pipeline para — evitando dados inválidos no DWH.

### Checks disponíveis

| Tipo | O que valida | Campos obrigatórios |
|---|---|---|
| `row_count` | Quantidade de linhas dentro de intervalo | `min` e/ou `max` |
| `not_null` | Ausência de nulos em colunas | `columns` |
| `unique` | Unicidade de valores em colunas | `columns` |
| `value_in` | Valores pertencem a um conjunto permitido | `column`, `values` |
| `value_range` | Valores numéricos dentro de intervalo | `column`, `min` e/ou `max` |
| `regex` | Valores correspondem a padrão regex | `column`, `pattern` |

### Exemplo com múltiplos checks

```yaml
- id: dq_pedidos
  type: data_quality
  config:
    input_xcom_key: raw_orders
    input_task_id: extract_orders
    checks:
      - name: "Ao menos 1 pedido"
        type: row_count
        min: 1
        on_failure: raise

      - name: "Campos obrigatórios presentes"
        type: not_null
        columns: [order_id, customer_id, total_amount]
        on_failure: raise

      - name: "order_id único"
        type: unique
        columns: [order_id]
        on_failure: raise

      - name: "Status válido"
        type: value_in
        column: status
        values: [pending, confirmed, shipped, delivered, cancelled]
        on_failure: raise

      - name: "Valor total positivo"
        type: value_range
        column: total_amount
        min: 0
        on_failure: warn

      - name: "SKU no formato correto"
        type: regex
        column: sku
        pattern: "^[A-Z]{2,4}-\\d{4,6}$"
        on_failure: warn
  depends_on: [extract_orders]
```

---

## Transformações declarativas

O `TransformOperator` aceita uma lista de `steps` aplicados sequencialmente ao DataFrame.

### Steps disponíveis

**`rename_columns`** — renomeia colunas
```yaml
- type: rename_columns
  mapping:
    order_dt: order_date
    cust_id: customer_id
```

**`drop_columns`** — remove colunas
```yaml
- type: drop_columns
  columns: [internal_flag, legacy_id, _row_hash]
```

**`filter_rows`** — filtra linhas usando sintaxe `pandas.DataFrame.query`
```yaml
- type: filter_rows
  query: "total_amount > 0 and status != 'cancelled'"
```

**`fill_nulls`** — preenche valores nulos
```yaml
# Por coluna:
- type: fill_nulls
  columns:
    discount_amount: 0.0
    notes: ""

# Valor global:
- type: fill_nulls
  value: 0
```

**`cast_types`** — converte tipos de colunas
```yaml
- type: cast_types
  mapping:
    order_date: datetime   # pd.to_datetime
    total_amount: float
    customer_id: int
    is_active: bool
```

**`add_column`** — cria nova coluna com valor fixo ou expressão
```yaml
# Valor fixo:
- type: add_column
  name: source_system
  value: "erp_v2"

# Expressão pandas.eval:
- type: add_column
  name: net_amount
  expression: "total_amount - discount_amount"
```

**`drop_duplicates`** — remove linhas duplicadas
```yaml
- type: drop_duplicates
  subset: [order_id]   # omita para usar todas as colunas
```

**`sort`** — ordena o DataFrame
```yaml
- type: sort
  by: [order_date, customer_id]
  ascending: true
```

---

## Funções Python customizadas

Para lógica complexa que não cabe em steps declarativos, implemente uma função em `include/utils/transformations.py` e referencie no YAML pelo caminho dotted:

```python
# include/utils/transformations.py

def normalize_products(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza o catálogo de produtos."""
    df.columns = df.columns.str.lower().str.strip()
    df["sku"] = df["sku"].str.upper()
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    return df.dropna(subset=["product_id"])
```

```yaml
- id: transform_products
  type: transform
  config:
    input_xcom_key: raw_products
    input_task_id: extract_products
    output_xcom_key: clean_products
    callable: include.utils.transformations.normalize_products
  depends_on: [extract_products]
```

A função recebe um `pd.DataFrame` e deve retornar um `pd.DataFrame`.

---

## Como estender o sistema

### Adicionando um novo operador

1. Crie `plugins/operators/meu_operator.py` herdando de `BaseETLOperator`:

```python
from operators.base_etl_operator import BaseETLOperator

class MeuOperator(BaseETLOperator):
    REQUIRED_CONFIG_FIELDS = ["campo_obrigatorio"]

    def _run(self, context):
        dados = context["task_instance"].xcom_pull(
            task_ids=self.task_config.get("input_task_id"),
            key=self.task_config.get("input_xcom_key", "extract_result"),
        )
        # ... lógica de negócio
        return {"linhas_processadas": len(dados)}
```

2. Registre em `dags/dag_factory.py` dentro de `_build_operator_registry()`:

```python
custom_operators = {
    ...
    "meu_tipo": ("operators.meu_operator", "MeuOperator"),
}
```

3. Use no YAML:

```yaml
- id: minha_task
  type: meu_tipo
  config:
    campo_obrigatorio: valor
    input_xcom_key: raw_data
    input_task_id: extract
```

### Adicionando um novo step de transformação

Em `transform_operator.py`, adicione um método e registre no dispatch table:

```python
def _step_meu_step(self, df, step: dict):
    # ... lógica
    return df

_step_handlers = {
    ...
    "meu_step": _step_meu_step,
}
```

---

## Testes

```bash
# Instalar dependências de desenvolvimento
pip install -r requirements-dev.txt

# Executar todos os testes
pytest tests/ -v

# Com relatório de cobertura
pytest tests/ --cov=. --cov-report=html
open htmlcov/index.html
```

Os testes são unitários e não dependem do Airflow em execução — usam mocks do contexto de execução.

---

## Escalabilidade

O projeto usa `LocalExecutor` por padrão (ideal para desenvolvimento e cargas moderadas).

Para escalar horizontalmente com **CeleryExecutor**:

1. Descomente os serviços `redis` e `worker` no `docker-compose.yml`
2. Ajuste o `.env`:

```env
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
```

3. Escale os workers conforme necessário:

```bash
docker compose up -d --scale worker=3
```

---

## Troubleshooting

### Tabelas não existem no banco

O script `include/sql/init_dwh.sql` só roda quando o volume do Postgres é criado pela primeira vez. Se as tabelas não existirem:

```bash
docker compose down -v   # destrói o volume (metadados do Airflow também)
docker compose up airflow-init
docker compose up -d
```

Lembre de recriar a connection `postgres_dwh` na UI após reinicializar.

### XCom vazio entre tasks

Verifique se a task anterior completou com sucesso (verde na UI). Tasks downstream só recebem o XCom de runs que completaram — não de runs com falha.

### Timestamp não serializável / numpy types

Já corrigido nos operadores: `df.to_json(orient="records", date_format="iso")` + `json.loads()` garante que todos os tipos sejam Python nativos antes de entrar no XCom.

### DAG não aparece na UI

- Confirme que o arquivo está em `dags/configs/` com extensão `.yaml`
- Verifique se a chave `dag` existe no arquivo
- Consulte os logs do scheduler: `docker compose logs -f scheduler`

### Logs em tempo real

```bash
docker compose logs -f scheduler    # logs do agendador
docker compose logs -f webserver    # logs da UI
docker compose logs -f              # todos os serviços
```

---

## Boas práticas aplicadas

- **DAGs como código declarativo** — YAML versionado no Git, sem boilerplate Python repetitivo
- **Separação de responsabilidades** — Factory, Operadores, Transformações e Configs são módulos independentes
- **Falha rápida** — `DataQualityOperator` antes do `LoadOperator` para nunca sujar o DWH com dados inválidos
- **Idempotência** — `load_strategy: upsert` ou `replace` garantem reexecução segura sem duplicatas
- **Sem segredos no código** — conexões via Airflow Connections, credenciais via `.env` (nunca commitado)
- **Serialização segura** — conversão para tipos Python nativos antes de qualquer push no XCom
- **Testes unitários** — cobrindo factory, operadores e transformações sem dependência do Airflow rodando
- **Logs estruturados** — cada operador loga métricas de linhas processadas e duração de execução

---

## Autor

**Davy Pedro Moreira Porto**
[![LinkedIn](https://img.shields.io/badge/LinkedIn-dpedromoura-blue?logo=linkedin)](https://www.linkedin.com/in/dpedromoura/)

---

## Licença

MIT
