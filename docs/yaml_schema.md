# Schema YAML — Referência Completa

Este documento descreve todos os campos disponíveis para definir uma DAG no projeto `airflow-etl`. Cada pipeline é um arquivo `.yaml` em `dags/configs/` e é descoberto automaticamente pelo Scheduler.

---

## Sumário

- [Estrutura raiz](#estrutura-raiz)
- [Bloco `dag`](#bloco-dag)
- [Bloco `default_args`](#bloco-default_args)
- [Bloco `tasks`](#bloco-tasks)
- [Operador `extract`](#operador-extract)
- [Operador `transform`](#operador-transform)
- [Operador `load`](#operador-load)
- [Operador `data_quality`](#operador-data_quality)
- [Operador `python`](#operador-python)
- [Operador `bash`](#operador-bash)
- [Operador `empty`](#operador-empty)
- [Exemplos completos](#exemplos-completos)
- [Boas práticas](#boas-práticas)

---

## Estrutura raiz

Todo arquivo YAML deve ter exatamente uma chave raiz: `dag`.

```yaml
dag:
  # campos do pipeline
  tasks:
    - # lista de tasks
```

---

## Bloco `dag`

Configuração geral do pipeline.

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `id` | string | **sim** | — | Identificador único da DAG no Airflow. Use snake_case. |
| `start_date` | string | **sim** | — | Data de início no formato `YYYY-MM-DD`. |
| `description` | string | não | `""` | Texto curto exibido na lista de DAGs da UI. |
| `schedule` | string | não | `null` | Cron expression ou preset (`@daily`, `@hourly`, `@weekly`, `@monthly`, `null`). |
| `catchup` | bool | não | `false` | Se `true`, executa runs retroativos desde `start_date`. |
| `max_active_runs` | int | não | `1` | Número máximo de runs simultâneos. |
| `tags` | list[string] | não | `[]` | Tags para filtrar na UI. |
| `doc_md` | string | não | `""` | Documentação em Markdown exibida na UI (suporta bloco `|`). |
| `render_template_as_native_obj` | bool | não | `false` | Renderiza templates Jinja como objetos Python nativos. |
| `default_args` | dict | não | `{}` | Argumentos padrão das tasks (ver abaixo). |
| `tasks` | list | não | `[]` | Lista de tasks do pipeline. |

```yaml
dag:
  id: exemplo_dag
  description: "Pipeline de exemplo"
  schedule: "0 6 * * 1-5"  # seg a sex às 06:00
  start_date: "2024-01-01"
  catchup: false
  max_active_runs: 1
  tags: [etl, producao, financeiro]
  doc_md: |
    ## Pipeline de Exemplo

    Extrai dados do sistema transacional e carrega no DWH.

    **Responsável:** time-dados@empresa.com
```

---

## Bloco `default_args`

Configurações aplicadas a todas as tasks da DAG, salvo sobrescrita individual.

| Campo | Tipo | Default | Descrição |
|---|---|---|---|
| `owner` | string | `airflow` | Proprietário exibido na UI. |
| `retries` | int | `1` | Número de tentativas automáticas em caso de falha. |
| `retry_delay_minutes` | int | `5` | Minutos entre tentativas. |
| `depends_on_past` | bool | `false` | Task só roda se o run anterior da mesma task teve sucesso. |
| `email` | list[string] | `[]` | E-mails para alertas. |
| `email_on_failure` | bool | `false` | Enviar e-mail quando uma task falhar. |
| `email_on_retry` | bool | `false` | Enviar e-mail em cada retry. |

```yaml
default_args:
  owner: data-engineering
  retries: 2
  retry_delay_minutes: 10
  email_on_failure: true
  email: [data-alerts@empresa.com]
```

---

## Bloco `tasks`

Lista de tasks que compõem o pipeline. Cada task tem os seguintes campos comuns:

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `id` | string | **sim** | Identificador único dentro da DAG. Use snake_case com verbo: `extract_orders`. |
| `type` | string | **sim** | Tipo do operador: `extract`, `transform`, `load`, `data_quality`, `python`, `bash`, `empty`. |
| `description` | string | não | Texto exibido como `doc_md` da task na UI. |
| `config` | dict | não | Configuração específica do operador (ver cada tipo abaixo). |
| `depends_on` | list[string] | não | IDs das tasks que devem completar antes desta. Define as arestas do DAG. |

```yaml
tasks:
  - id: extract_vendas
    type: extract
    description: "Extrai vendas do banco transacional"
    config:
      source_type: postgres
      conn_id: postgres_source
      sql: "SELECT * FROM vendas WHERE data = '{{ ds }}'"
      output_xcom_key: raw_vendas
    depends_on: []

  - id: load_vendas
    type: load
    config:
      destination_type: postgres
      conn_id: postgres_dwh
      table: fact_vendas
      load_strategy: upsert
      upsert_keys: [venda_id]
      input_xcom_key: raw_vendas
      input_task_id: extract_vendas
    depends_on: [extract_vendas]
```

---

## Operador `extract`

Extrai dados de uma fonte e publica o resultado via XCom.

### Campos comuns

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `source_type` | string | **sim** | — | `postgres`, `mysql`, `s3`, `http` ou `file`. |
| `output_xcom_key` | string | não | `extract_result` | Chave XCom para o resultado. |

### `source_type: postgres` ou `mysql`

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `conn_id` | string | **sim** | Connection ID configurado no Airflow. |
| `sql` | string | um dos dois | Query SQL inline. Suporta templates Jinja (`{{ ds }}`, etc.). |
| `sql_file` | string | um dos dois | Caminho para arquivo `.sql` (relativo à raiz do projeto). |
| `parameters` | dict | não | Parâmetros da query (substitui `%s` com segurança). |

```yaml
config:
  source_type: postgres
  conn_id: postgres_source
  sql_file: include/sql/extract_orders.sql
  parameters:
    days_back: 7
  output_xcom_key: raw_orders
```

### `source_type: s3`

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `conn_id` | string | não | `aws_default` | Connection ID da AWS. |
| `bucket` | string | **sim** | — | Nome do bucket. |
| `key` | string | **sim** | — | Caminho do objeto no bucket. |
| `file_format` | string | não | `csv` | `csv`, `json` ou `parquet`. |
| `read_options` | dict | não | `{}` | Opções extras para `pandas.read_*`. |

```yaml
config:
  source_type: s3
  bucket: meu-data-lake
  key: raw/pedidos/2024-01-01.parquet
  file_format: parquet
  output_xcom_key: raw_pedidos
```

### `source_type: http`

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `conn_id` | string | não | `http_default` | Connection ID HTTP. |
| `endpoint` | string | não | `/` | Path do endpoint. |
| `method` | string | não | `GET` | Método HTTP. |
| `headers` | dict | não | `{}` | Headers da requisição. |
| `data` | dict | não | `{}` | Body da requisição. |

```yaml
config:
  source_type: http
  conn_id: api_externa
  endpoint: /v1/produtos
  method: GET
  headers:
    Authorization: "Bearer {{ var.value.api_token }}"
  output_xcom_key: raw_api
```

### `source_type: file`

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `path` | string | **sim** | — | Caminho absoluto do arquivo dentro do container. |
| `file_format` | string | não | `csv` | `csv`, `json` ou `parquet`. |
| `read_options` | dict | não | `{}` | Opções extras para `pandas.read_*` (ex: `sep`, `encoding`). |

```yaml
config:
  source_type: file
  path: /opt/airflow/data/products.csv
  file_format: csv
  read_options:
    sep: ";"
    encoding: utf-8
  output_xcom_key: raw_products
```

---

## Operador `transform`

Aplica transformações a dados recebidos via XCom e publica o resultado.

### Campos comuns

| Campo | Tipo | Default | Descrição |
|---|---|---|---|
| `input_xcom_key` | string | `extract_result` | Chave XCom de entrada. |
| `input_task_id` | string | — | Task ID de onde ler o XCom. |
| `output_xcom_key` | string | `transform_result` | Chave XCom de saída. |

O operador suporta dois modos: **callable** ou **steps declarativos**.

### Modo 1: callable Python

```yaml
config:
  input_xcom_key: raw_products
  input_task_id: extract_products
  output_xcom_key: clean_products
  callable: include.utils.transformations.normalize_products
```

A função referenciada recebe um `pd.DataFrame` e deve retornar um `pd.DataFrame`.

### Modo 2: steps declarativos

```yaml
config:
  input_xcom_key: raw_orders
  input_task_id: extract_orders
  output_xcom_key: clean_orders
  steps:
    - type: rename_columns
      mapping:
        order_dt: order_date
        cust_id: customer_id
        tot_amt: total_amount

    - type: drop_columns
      columns: [internal_flag, legacy_id, _row_hash]

    - type: filter_rows
      query: "total_amount > 0"

    - type: fill_nulls
      columns:
        discount_amount: 0.0
        notes: ""

    - type: cast_types
      mapping:
        order_date: datetime
        total_amount: float
        customer_id: int

    - type: add_column
      name: net_amount
      expression: "total_amount - discount_amount"

    - type: drop_duplicates
      subset: [order_id]

    - type: sort
      by: [order_date]
      ascending: false
```

### Referência de steps

| Step | Campos | Descrição |
|---|---|---|
| `rename_columns` | `mapping: {old: new}` | Renomeia colunas. |
| `drop_columns` | `columns: [...]` | Remove colunas (ignora se não existir). |
| `filter_rows` | `query: "expr"` | Filtra usando `DataFrame.query`. |
| `fill_nulls` | `columns: {col: val}` ou `value: val` | Preenche nulos. |
| `cast_types` | `mapping: {col: tipo}` | Converte tipos: `datetime`, `float`, `int`, `str`, `bool`. |
| `add_column` | `name`, `value` ou `expression` | Cria coluna com valor fixo ou expressão `eval`. |
| `drop_duplicates` | `subset: [...]` | Remove duplicatas por subconjunto de colunas. |
| `sort` | `by: [...]`, `ascending: bool` | Ordena o DataFrame. |

---

## Operador `load`

Carrega dados recebidos via XCom em um destino.

### Campos comuns

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `destination_type` | string | **sim** | — | `postgres`, `mysql`, `s3` ou `file`. |
| `input_xcom_key` | string | não | `transform_result` | Chave XCom de entrada. |
| `input_task_id` | string | não | — | Task ID de onde ler o XCom. |
| `load_strategy` | string | não | `append` | `append`, `replace` ou `upsert`. |
| `batch_size` | int | não | `1000` | Registros por lote de inserção. |

### Estratégias de carga

| Estratégia | Comportamento | Ideal para |
|---|---|---|
| `append` | INSERT puro | Logs, eventos, dados históricos imutáveis |
| `replace` | TRUNCATE + INSERT | Dimensões que são sempre recarregadas por completo |
| `upsert` | INSERT … ON CONFLICT DO UPDATE | Dimensões e fatos com chave natural, permite reexecução segura |

### `destination_type: postgres` ou `mysql`

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `conn_id` | string | **sim** | Connection ID do Airflow. |
| `table` | string | **sim** | Nome da tabela de destino. |
| `schema` | string | não | Schema (apenas Postgres). Default: `public`. |
| `upsert_keys` | list[string] | se upsert | Colunas-chave para o `ON CONFLICT`. |

```yaml
config:
  destination_type: postgres
  conn_id: postgres_dwh
  table: fact_orders
  schema: public
  load_strategy: upsert
  upsert_keys: [order_id]
  batch_size: 500
  input_xcom_key: clean_orders
  input_task_id: transform_orders
```

### `destination_type: s3`

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `conn_id` | string | não | `aws_default` | Connection ID da AWS. |
| `bucket` | string | **sim** | — | Bucket de destino. |
| `key` | string | **sim** | — | Caminho do objeto. |
| `file_format` | string | não | `csv` | `csv`, `json` ou `parquet`. |

```yaml
config:
  destination_type: s3
  bucket: meu-data-lake
  key: processed/orders/{{ ds }}.parquet
  file_format: parquet
  input_xcom_key: clean_orders
  input_task_id: transform_orders
```

### `destination_type: file`

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `path` | string | **sim** | — | Caminho do arquivo de saída. |
| `file_format` | string | não | `csv` | `csv`, `json` ou `parquet`. |

```yaml
config:
  destination_type: file
  path: /opt/airflow/data/output/clean_orders.csv
  file_format: csv
  input_xcom_key: clean_orders
  input_task_id: transform_orders
```

---

## Operador `data_quality`

Valida os dados antes do carregamento. Cada check falha ou emite aviso conforme `on_failure`.

### Campos

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `input_xcom_key` | string | não | `transform_result` | Chave XCom dos dados a validar. |
| `input_task_id` | string | não | — | Task ID de onde ler o XCom. |
| `checks` | list | **sim** | — | Lista de validações a executar. |

### Campos de cada check

| Campo | Tipo | Obrigatório | Default | Descrição |
|---|---|---|---|---|
| `type` | string | **sim** | — | Tipo do check (ver tabela abaixo). |
| `name` | string | não | igual ao `type` | Rótulo exibido no log. |
| `on_failure` | string | não | `raise` | `raise` (falha a task) ou `warn` (só loga). |

### Tipos de check

| Tipo | Campos específicos | Descrição |
|---|---|---|
| `row_count` | `min: int`, `max: int` | Número de linhas dentro do intervalo. |
| `not_null` | `columns: [str]` | Nenhuma das colunas pode ter nulos. |
| `unique` | `columns: [str]` | Combinação de colunas deve ser única. |
| `value_in` | `column: str`, `values: [...]` | Todos os valores devem estar na lista. |
| `value_range` | `column: str`, `min: num`, `max: num` | Valores dentro do intervalo numérico. |
| `regex` | `column: str`, `pattern: str` | Todos os valores devem bater com o regex. |

```yaml
config:
  input_xcom_key: raw_clientes
  input_task_id: extract_clientes
  checks:
    - name: "Volume mínimo"
      type: row_count
      min: 100
      max: 1000000
      on_failure: raise

    - name: "Chaves não nulas"
      type: not_null
      columns: [cliente_id, cpf, email]
      on_failure: raise

    - name: "CPF único"
      type: unique
      columns: [cpf]
      on_failure: raise

    - name: "UF válida"
      type: value_in
      column: estado
      values: [AC, AL, AP, AM, BA, CE, DF, ES, GO, MA, MT, MS,
               MG, PA, PB, PR, PE, PI, RJ, RN, RS, RO, RR, SC, SP, SE, TO]
      on_failure: raise

    - name: "Saldo não negativo"
      type: value_range
      column: saldo
      min: 0
      on_failure: warn

    - name: "Formato de CPF"
      type: regex
      column: cpf
      pattern: "^\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}$"
      on_failure: warn
```

---

## Operador `python`

Executa uma função Python arbitrária.

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `callable` | string | **sim** | Caminho dotted para a função: `modulo.submodulo.funcao`. |
| `op_kwargs` | dict | não | Argumentos nomeados repassados ao callable. |

```yaml
- id: enviar_alerta
  type: python
  config:
    callable: include.utils.notifications.send_slack_alert
    op_kwargs:
      channel: "#data-alerts"
      message: "Pipeline de pedidos concluído com sucesso."
  depends_on: [load_orders]
```

---

## Operador `bash`

Executa um comando shell.

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `bash_command` | string | **sim** | Comando a executar. Suporta templates Jinja. |
| `env` | dict | não | Variáveis de ambiente extras para o processo. |

```yaml
- id: exportar_relatorio
  type: bash
  config:
    bash_command: >
      python /opt/airflow/scripts/gerar_relatorio.py
      --data {{ ds }}
      --output /opt/airflow/data/output/relatorio_{{ ds }}.csv
    env:
      PYTHONPATH: /opt/airflow
  depends_on: [load_orders]
```

---

## Operador `empty`

Operador sem lógica — útil como ponto de sincronização, marcador de início/fim ou gate condicional.

```yaml
- id: pipeline_concluido
  type: empty
  description: "Todos os carregamentos finalizados com sucesso"
  depends_on: [load_orders, load_items, load_customers]
```

---

## Exemplos completos

### Pipeline CSV → Postgres com validação

```yaml
dag:
  id: produtos_etl
  description: "Importa catálogo de produtos do CSV para o DWH"
  schedule: "0 6 * * 1"
  start_date: "2024-01-01"
  catchup: false
  tags: [etl, csv, produtos]

  default_args:
    owner: data-engineering
    retries: 1
    retry_delay_minutes: 10

  tasks:
    - id: extract_produtos
      type: extract
      config:
        source_type: file
        path: /opt/airflow/data/products.csv
        file_format: csv
        read_options:
          sep: ";"
          encoding: utf-8
        output_xcom_key: raw_produtos
      depends_on: []

    - id: dq_produtos
      type: data_quality
      config:
        input_xcom_key: raw_produtos
        input_task_id: extract_produtos
        checks:
          - name: "Ao menos 10 produtos"
            type: row_count
            min: 10
            on_failure: raise
          - name: "Campos obrigatórios"
            type: not_null
            columns: [product_id, product_name, sku]
            on_failure: raise
          - name: "SKU único"
            type: unique
            columns: [sku]
            on_failure: raise
          - name: "Preço positivo"
            type: value_range
            column: price
            min: 0
            on_failure: warn
      depends_on: [extract_produtos]

    - id: transform_produtos
      type: transform
      config:
        input_xcom_key: raw_produtos
        input_task_id: extract_produtos
        output_xcom_key: clean_produtos
        callable: include.utils.transformations.normalize_products
      depends_on: [dq_produtos]

    - id: load_produtos
      type: load
      config:
        destination_type: postgres
        conn_id: postgres_dwh
        table: dim_products
        schema: public
        load_strategy: upsert
        upsert_keys: [product_id]
        batch_size: 200
        input_xcom_key: clean_produtos
        input_task_id: transform_produtos
      depends_on: [transform_produtos]

    - id: fim
      type: empty
      description: "Pipeline de produtos concluído"
      depends_on: [load_produtos]
```

### Pipeline Postgres → Postgres (SQL externo)

```yaml
dag:
  id: pedidos_etl
  description: "Extrai pedidos do banco transacional e carrega no DWH"
  schedule: "@daily"
  start_date: "2024-01-01"
  catchup: false
  tags: [etl, postgres, pedidos]

  default_args:
    owner: data-engineering
    retries: 2
    retry_delay_minutes: 5

  tasks:
    - id: extract_pedidos
      type: extract
      config:
        source_type: postgres
        conn_id: postgres_source
        sql_file: include/sql/extract_orders.sql
        parameters:
          days_back: 1
        output_xcom_key: raw_pedidos
      depends_on: []

    - id: dq_pedidos
      type: data_quality
      config:
        input_xcom_key: raw_pedidos
        input_task_id: extract_pedidos
        checks:
          - type: row_count
            min: 1
            on_failure: raise
          - type: not_null
            columns: [order_id, customer_id, total_amount]
            on_failure: raise
      depends_on: [extract_pedidos]

    - id: transform_pedidos
      type: transform
      config:
        input_xcom_key: raw_pedidos
        input_task_id: extract_pedidos
        output_xcom_key: clean_pedidos
        steps:
          - type: filter_rows
            query: "total_amount > 0"
          - type: fill_nulls
            columns:
              discount_amount: 0.0
          - type: add_column
            name: net_amount
            expression: "total_amount - discount_amount"
      depends_on: [dq_pedidos]

    - id: load_pedidos
      type: load
      config:
        destination_type: postgres
        conn_id: postgres_dwh
        table: fact_orders
        schema: public
        load_strategy: upsert
        upsert_keys: [order_id]
        batch_size: 1000
        input_xcom_key: clean_pedidos
        input_task_id: transform_pedidos
      depends_on: [transform_pedidos]
```

---

## Boas práticas

1. **Um arquivo YAML por DAG** — facilita revisão em pull requests e rastreabilidade de mudanças.
2. **Use `catchup: false`** — evita backfill acidental ao ativar uma DAG em produção.
3. **Use `max_active_runs: 1`** — previne condições de corrida em pipelines ETL com estado.
4. **Adicione `data_quality` antes do `load`** — falhe cedo, não no DWH.
5. **Prefira `sql_file` a `sql` inline** — queries longas ficam no controle de versão como `.sql`, com syntax highlighting.
6. **Nomeie tasks com `verbo_substantivo`** — `extract_orders`, `dq_orders`, `transform_orders`, `load_orders`.
7. **Documente com `doc_md`** — visível na UI sem abrir código; inclua responsável e dependências externas.
8. **Use `on_failure: warn`** para checks informativos — status incomum que não deve parar o pipeline.
9. **Declare `depends_on: []` explicitamente** na primeira task — deixa a intenção clara no YAML.
10. **Versione os arquivos `data/`** junto ao YAML correspondente — facilita reprodução e testes locais.
