
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="supply_chain_connection_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["supply-chain", "postgresql"],
)
def supply_chain_connection_test():

    @task
    def verify_database():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        database = hook.get_first(
            "SELECT current_database();"
        )[0]

        total_records = hook.get_first(
            "SELECT COUNT(*) FROM dataco.order_items;"
        )[0]

        stockout_rate = hook.get_first(
            "SELECT stockout_rate FROM ops.v_kpi_overview;"
        )[0]

        print("Connected database:", database)
        print("DataCo order-item records:", total_records)
        print("Baseline stockout rate:", stockout_rate)

        if database != "supply_chain_db":
            raise ValueError("Connected to the wrong database")

        if total_records != 180519:
            raise ValueError("Unexpected order-item count")

        print("PostgreSQL connection test PASSED!")

    verify_database()


supply_chain_connection_test()
