
import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "supply_chain_postgres"
MODEL_NAME = "HOURLY_MEAN_BASELINE_V1"
DATA_SOURCE = "SIMULATED_HOURLY"

EXPECTED_PRODUCTS = 118
FORECAST_HOURS = 24
MIN_HISTORY_HOURS = 24


@dag(
    dag_id="operational_demand_forecast_pipeline",
    schedule="15 * * * *",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["supply-chain", "forecasting", "automation"],
)
def operational_demand_forecast_pipeline():

    # ========================================================
    # TASK 1: VALIDATE OPERATIONAL DEMAND
    # ========================================================

    @task
    def check_operational_history():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        result = hook.get_first("""
            SELECT
                COUNT(DISTINCT snapshot_hour),
                MAX(snapshot_hour),
                COUNT(DISTINCT product_id)
            FROM ops.live_inventory_hourly;
        """)

        if result is None:
            raise ValueError("No operational history found.")

        history_hours, asof_hour, product_count = result

        print("History hours:", history_hours)
        print("Latest simulated hour:", asof_hour)
        print("Products:", product_count)

        if history_hours < MIN_HISTORY_HOURS:
            raise ValueError(
                f"At least {MIN_HISTORY_HOURS} hours required."
            )

        if product_count != EXPECTED_PRODUCTS:
            raise ValueError(
                f"Expected {EXPECTED_PRODUCTS} products, "
                f"found {product_count}."
            )

        if asof_hour is None:
            raise ValueError("No simulation timestamp found.")

        print("Operational history validation PASSED!")

        # Preserve a fixed training cutoff across tasks.
        return asof_hour.isoformat()

    # ========================================================
    # TASK 2: GENERATE 24-HOUR PRODUCT FORECASTS
    # ========================================================

    @task
    def generate_operational_forecasts(asof_hour):

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        sql = """
            WITH history AS (
                SELECT
                    MAX(snapshot_hour) AS asof_hour,
                    COUNT(DISTINCT snapshot_hour)::INTEGER
                        AS history_hours
                FROM ops.live_inventory_hourly
                WHERE snapshot_hour <=
                    CAST(%(asof_hour)s AS TIMESTAMPTZ)
            ),

            product_demand AS (
                SELECT
                    product_id,
                    SUM(demand_units)::NUMERIC
                        AS observed_units
                FROM ops.live_inventory_hourly
                WHERE snapshot_hour <=
                    CAST(%(asof_hour)s AS TIMESTAMPTZ)
                GROUP BY product_id
            ),

            forecasts AS (
                SELECT
                    p.product_id,

                    'HOURLY_MEAN_BASELINE_V1'::TEXT
                        AS model_name,

                    h.asof_hour,

                    h.asof_hour
                        + g.hour_number * INTERVAL '1 hour'
                        AS forecast_hour,

                    ROUND(
                        COALESCE(d.observed_units, 0)
                        / NULLIF(h.history_hours, 0),
                        4
                    ) AS predicted_units,

                    h.history_hours,

                    'SIMULATED_HOURLY'::TEXT
                        AS data_source

                FROM dataco.products p

                CROSS JOIN history h

                CROSS JOIN generate_series(1, 24)
                    AS g(hour_number)

                LEFT JOIN product_demand d
                    ON p.product_id = d.product_id

                WHERE h.history_hours >= 24
                  AND h.asof_hour =
                      CAST(%(asof_hour)s AS TIMESTAMPTZ)
            )

            INSERT INTO analytics.operational_demand_forecasts (
                product_id,
                model_name,
                asof_hour,
                forecast_hour,
                predicted_units,
                history_hours,
                data_source
            )

            SELECT
                product_id,
                model_name,
                asof_hour,
                forecast_hour,
                predicted_units,
                history_hours,
                data_source

            FROM forecasts

            ON CONFLICT (
                product_id,
                model_name,
                asof_hour,
                forecast_hour
            )

            DO UPDATE SET
                predicted_units = EXCLUDED.predicted_units,
                history_hours = EXCLUDED.history_hours,
                data_source = EXCLUDED.data_source,
                created_at = NOW();
        """

        hook.run(
            sql=sql,
            parameters={"asof_hour": asof_hour},
            autocommit=False
        )

        print("Forecast cutoff:", asof_hour)
        print("24-hour forecasting SQL completed.")

        return asof_hour

    # ========================================================
    # TASK 3: VERIFY SAVED FORECASTS
    # ========================================================

    @task
    def verify_operational_forecasts(asof_hour):

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        result = hook.get_first(
            sql="""
                SELECT
                    COUNT(*) AS forecast_rows,
                    COUNT(DISTINCT product_id) AS products,
                    COUNT(DISTINCT forecast_hour) AS hours,
                    COUNT(DISTINCT product_id) FILTER (
                        WHERE predicted_units > 0
                    ) AS positive_products,
                    ROUND(SUM(predicted_units), 2)
                        AS predicted_demand,
                    MIN(forecast_hour) AS first_hour,
                    MAX(forecast_hour) AS last_hour

                FROM analytics.operational_demand_forecasts

                WHERE model_name = %(model_name)s
                  AND data_source = %(data_source)s
                  AND asof_hour =
                      CAST(%(asof_hour)s AS TIMESTAMPTZ);
            """,
            parameters={
                "model_name": MODEL_NAME,
                "data_source": DATA_SOURCE,
                "asof_hour": asof_hour
            }
        )

        if result is None:
            raise ValueError("Forecast validation failed.")

        (
            forecast_rows,
            products,
            hours,
            positive_products,
            predicted_demand,
            first_hour,
            last_hour
        ) = result

        print("================================")
        print("OPERATIONAL FORECAST VALIDATION")
        print("================================")

        print("Forecast rows:", forecast_rows)
        print("Products:", products)
        print("Forecast hours:", hours)
        print("Positive-demand products:", positive_products)
        print("Predicted 24h units:", predicted_demand)
        print("First forecast hour:", first_hour)
        print("Last forecast hour:", last_hour)

        expected_rows = EXPECTED_PRODUCTS * FORECAST_HOURS

        if forecast_rows != expected_rows:
            raise ValueError(
                f"Expected {expected_rows} rows, "
                f"found {forecast_rows}."
            )

        if products != EXPECTED_PRODUCTS:
            raise ValueError(
                f"Expected {EXPECTED_PRODUCTS} products."
            )

        if hours != FORECAST_HOURS:
            raise ValueError(
                f"Expected {FORECAST_HOURS} forecast hours."
            )

        cutoff = pendulum.parse(asof_hour)

        expected_first = cutoff.add(hours=1)
        expected_last = cutoff.add(hours=FORECAST_HOURS)

        if first_hour != expected_first or last_hour != expected_last:
            raise ValueError(
                "Forecast horizon does not match training cutoff."
            )

        print("OPERATIONAL FORECAST PIPELINE PASSED!")

    # ========================================================
    # TASK EXECUTION ORDER
    # ========================================================

    history = check_operational_history()
    forecast = generate_operational_forecasts(history)
    verify_operational_forecasts(forecast)


operational_demand_forecast_pipeline()
