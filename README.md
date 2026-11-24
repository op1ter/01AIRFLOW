# ShopBrasil - Pipeline Airflow de Pricing

Projeto Dockerizado com Apache Airflow 2.10.5 para coletar produtos da
FakeStore API, calcular métricas por categoria e persistir os resultados em
PostgreSQL.

## Arquitetura

- **Ingestão:** busca resiliente dos produtos, validação por operador customizado
  e callbacks de ciclo de vida.
- **Análise:** extração dinâmica de categorias, cálculo paralelo com Dynamic Task
  Mapping e consolidação fan-in.
- **Persistência:** snapshot idempotente por categoria e histórico append por
  data de execução.

O DAG executa todos os dias às 06:00 em `America/Sao_Paulo`, sem catchup.

## Serviços

```text
postgres          Banco de metadados do Airflow e banco ecommerce
airflow-init      Migração do banco, usuário admin e pool ecommerce_pool
airflow-webserver Interface web na porta 8080
airflow-scheduler Execução das tarefas
```

## Como executar

```bash
docker compose up --build -d
```

A interface fica disponível em http://localhost:8081.

- Usuário: `admin`
- Senha: `admin`

Os recursos são iniciados automaticamente:

- Connection: `postgres_ecommerce`
- Pool: `ecommerce_pool`, com 2 slots
- Banco analítico: `ecommerce`

## Estrutura

```text
dags/
  ecommerce_pricing_dag.py
plugins/
  operators/
    validar_produtos_operator.py
init/
  01-create-ecommerce-db.sql
docker-compose.yaml
Dockerfile
requirements.txt
```

## Tabelas PostgreSQL

### ecommerce_category_metrics

Snapshot atualizado por UPSERT usando `category` como chave primária:

- `category`
- `average_price`
- `minimum_price`
- `maximum_price`
- `product_count`
- `updated_at`

### ecommerce_category_metrics_history

Histórico append por data de execução:

- `execution_date`
- `category`
- `average_price`
- `minimum_price`
- `maximum_price`
- `product_count`

## Resiliência e observabilidade

A tarefa crítica `buscar_produtos` possui cinco tentativas, backoff exponencial
e delay máximo de dez minutos. Falhas, retries e sucessos geram mensagens de
log estruturadas pelos callbacks configurados.

## Validação rápida

Com os containers ativos:

```bash
docker compose exec airflow-scheduler python -m compileall -q /opt/airflow/dags /opt/airflow/plugins
```