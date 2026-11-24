from datetime import timedelta
import logging

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.utils.timezone import coerce_datetime
from operators.validar_produtos_operator import ValidarProdutosOperator


API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONNECTION_ID = "postgres_ecommerce"
SAO_PAULO = pendulum.timezone("America/Sao_Paulo")


def on_failure_callback(context: Context) -> None:
    task_instance = context["task_instance"]
    logging.error(
        "Alerta: task %s falhou na execução %s: %s",
        task_instance.task_id,
        context["dag_run"].run_id,
        context.get("exception"),
    )


def on_retry_callback(context: Context) -> None:
    task_instance = context["task_instance"]
    logging.warning(
        "Alerta: task %s será tentada novamente; tentativa %s",
        task_instance.task_id,
        task_instance.try_number,
    )


def on_success_callback(context: Context) -> None:
    task_instance = context["task_instance"]
    logging.info(
        "Task %s concluída com sucesso na execução %s",
        task_instance.task_id,
        context["dag_run"].run_id,
    )


default_args = {
    "owner": "shopbrasil",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="ecommerce_pricing_pipeline",
    description="Coleta produtos da FakeStore API e calcula métricas por categoria.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=SAO_PAULO),
    catchup=False,
    default_args=default_args,
    tags=["shopbrasil", "pricing", "ecommerce"],
)
def ecommerce_pricing_pipeline() -> None:
    @task_group(group_id="ingestao")
    def ingestao() -> list[dict]:
        @task(
            task_id="buscar_produtos",
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
        )
        def buscar_produtos() -> list[dict]:
            try:
                response = requests.get(API_URL, timeout=15)
                response.raise_for_status()
                products = response.json()
                if not isinstance(products, list):
                    raise TypeError("A API retornou um formato inesperado.")
                return products
            except (requests.RequestException, TypeError, ValueError) as exc:
                logging.exception("Falha ao buscar produtos da FakeStore API")
                raise RuntimeError("Não foi possível buscar os produtos") from exc

        produtos = buscar_produtos()
        produtos_validados = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos,
        )
        return produtos_validados.output

    @task_group(group_id="analise")
    def analise(produtos: list[dict]) -> list[dict]:
        @task(task_id="extrair_categorias")
        def extrair_categorias(produtos_mapeaveis: list[dict]) -> list[str]:
            return sorted(
                {str(produto["category"]).strip() for produto in produtos_mapeaveis}
            )

        @task(
            task_id="calcular_metricas",
            pool="ecommerce_pool",
        )
        def calcular_metricas(category: str, produtos_mapeaveis: list[dict]) -> dict:
            prices = [
                float(produto["price"])
                for produto in produtos_mapeaveis
                if str(produto["category"]).strip() == category
            ]
            if not prices:
                raise ValueError(f"Categoria sem produtos: {category}")

            return {
                "category": category,
                "average_price": round(sum(prices) / len(prices), 2),
                "minimum_price": round(min(prices), 2),
                "maximum_price": round(max(prices), 2),
                "product_count": len(prices),
            }

        @task(task_id="consolidar_metricas")
        def consolidar_metricas(metricas_por_categoria: list[dict]) -> list[dict]:
            return sorted(
                metricas_por_categoria,
                key=lambda metricas: metricas["category"],
            )

        categorias = extrair_categorias(produtos)
        metricas = calcular_metricas.partial(
            produtos_mapeaveis=produtos
        ).expand(category=categorias)
        return consolidar_metricas(metricas)

    @task(task_id="persistir_postgres")
    def persistir_postgres(metricas: list[dict]) -> None:
        context = get_current_context()
        execution_date = coerce_datetime(context["data_interval_start"]).date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        connection = hook.get_conn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ecommerce_category_metrics (
                        category VARCHAR(255) PRIMARY KEY,
                        average_price NUMERIC(12, 2) NOT NULL,
                        minimum_price NUMERIC(12, 2) NOT NULL,
                        maximum_price NUMERIC(12, 2) NOT NULL,
                        product_count INTEGER NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS ecommerce_category_metrics_history (
                        execution_date DATE NOT NULL,
                        category VARCHAR(255) NOT NULL,
                        average_price NUMERIC(12, 2) NOT NULL,
                        minimum_price NUMERIC(12, 2) NOT NULL,
                        maximum_price NUMERIC(12, 2) NOT NULL,
                        product_count INTEGER NOT NULL,
                        PRIMARY KEY (execution_date, category)
                    );
                    """
                )

                for metricas_categoria in metricas:
                    cursor.execute(
                        """
                        INSERT INTO ecommerce_category_metrics (
                            category,
                            average_price,
                            minimum_price,
                            maximum_price,
                            product_count,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (category)
                        DO UPDATE SET
                            average_price = EXCLUDED.average_price,
                            minimum_price = EXCLUDED.minimum_price,
                            maximum_price = EXCLUDED.maximum_price,
                            product_count = EXCLUDED.product_count,
                            updated_at = EXCLUDED.updated_at;
                        """,
                        (
                            metricas_categoria["category"],
                            metricas_categoria["average_price"],
                            metricas_categoria["minimum_price"],
                            metricas_categoria["maximum_price"],
                            metricas_categoria["product_count"],
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO ecommerce_category_metrics_history (
                            execution_date,
                            category,
                            average_price,
                            minimum_price,
                            maximum_price,
                            product_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (execution_date, category)
                        DO UPDATE SET
                            average_price = EXCLUDED.average_price,
                            minimum_price = EXCLUDED.minimum_price,
                            maximum_price = EXCLUDED.maximum_price,
                            product_count = EXCLUDED.product_count;
                        """,
                        (
                            execution_date,
                            metricas_categoria["category"],
                            metricas_categoria["average_price"],
                            metricas_categoria["minimum_price"],
                            metricas_categoria["maximum_price"],
                            metricas_categoria["product_count"],
                        ),
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            logging.exception("Falha ao persistir métricas no PostgreSQL")
            raise
        finally:
            connection.close()

    produtos = ingestao()
    metricas = analise(produtos)
    persistir_postgres(metricas)


ecommerce_pricing_pipeline()
