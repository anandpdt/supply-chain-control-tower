
import pendulum

from airflow.sdk import dag, task


# ============================================================
# NEXVIA - HOURLY SUPPLY CHAIN PIPELINE
# ============================================================

POSTGRES_CONN_ID = "supply_chain_postgres"

EXPECTED_ORDER_ITEMS = 180519
EXPECTED_LEDGER_ROWS = 32568
EXPECTED_INVENTORY_PAIRS = 354

# Maximum missed execution hours processed in one DAG run
MAX_BACKFILL_HOURS = 3


@dag(
    dag_id="hourly_supply_chain_pipeline",
    schedule="@hourly",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["supply-chain", "automation"],
)
def hourly_supply_chain_pipeline():

    # ========================================================
    # TASK 1: VALIDATE SOURCE DATA
    # ========================================================

    @task
    def check_source_data():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        result = hook.get_first("""
            SELECT
                (SELECT COUNT(*)
                 FROM dataco.order_items),

                (SELECT COUNT(*)
                 FROM ops.inventory_ledger_daily);
        """)

        if result is None:
            raise ValueError(
                "Source validation returned no result."
            )

        order_items, inventory_records = result

        print("================================")
        print("SOURCE DATA VALIDATION")
        print("================================")

        print("Order items:", order_items)
        print("Inventory ledger:", inventory_records)

        if order_items != EXPECTED_ORDER_ITEMS:
            raise ValueError(
                f"Expected {EXPECTED_ORDER_ITEMS} order items, "
                f"found {order_items}"
            )

        if inventory_records != EXPECTED_LEDGER_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_LEDGER_ROWS} ledger rows, "
                f"found {inventory_records}"
            )

        print("Source validation PASSED!")

    # ========================================================
    # TASK 2: ADVANCE INVENTORY SIMULATION
    # ========================================================

    @task
    def advance_inventory_simulation():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        print("================================")
        print("HOURLY INVENTORY SIMULATION")
        print("================================")

        # ----------------------------------------------------
        # 2A. FIND MISSED EXECUTION HOURS
        # ----------------------------------------------------

        missed_hours = hook.get_records(
            sql="""
                WITH bounds AS (
                    SELECT
                        MIN(run_hour) AS first_run,
                        date_trunc(
                            'hour', NOW()
                        ) AS current_run
                    FROM ops.hourly_simulation_runs
                )

                SELECT
                    g.run_hour

                FROM bounds b

                CROSS JOIN LATERAL generate_series(
                    b.first_run,
                    b.current_run - INTERVAL '1 hour',
                    INTERVAL '1 hour'
                ) AS g(run_hour)

                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ops.hourly_simulation_runs r
                    WHERE r.run_hour = g.run_hour
                )

                ORDER BY g.run_hour ASC

                LIMIT %(max_hours)s;
            """,
            parameters={
                "max_hours": MAX_BACKFILL_HOURS
            }
        )

        print(
            "Missed execution hours selected:",
            len(missed_hours)
        )

        # ----------------------------------------------------
        # 2B. PROCESS ONE SIMULATION HOUR
        # ----------------------------------------------------

        def process_hour(run_at=None):

            if run_at is None:

                sql = """
                    SELECT
                        simulated_hour,
                        inventory_rows,
                        units_received,
                        shipments_created,
                        already_processed
                    FROM ops.advance_simulated_hour();
                """

                parameters = None

            else:

                sql = """
                    SELECT
                        simulated_hour,
                        inventory_rows,
                        units_received,
                        shipments_created,
                        already_processed
                    FROM ops.advance_simulated_hour(
                        %(run_at)s
                    );
                """

                parameters = {
                    "run_at": run_at
                }

            # Each execution has its own transaction.
            # This is important because the PostgreSQL
            # function uses an ON COMMIT DROP temp table.

            result = hook.run(
                sql=sql,
                parameters=parameters,
                autocommit=False,
                handler=lambda cursor: cursor.fetchone()
            )

            if result is None:
                raise ValueError(
                    "Simulation function returned no result."
                )

            (
                simulated_hour,
                inventory_rows,
                units_received,
                shipments_created,
                already_processed
            ) = result

            print("--------------------------------")
            print("Requested run hour:", run_at)
            print("Simulated hour:", simulated_hour)
            print("Inventory rows:", inventory_rows)
            print("Units received:", units_received)
            print("Shipments created:", shipments_created)
            print("Already processed:", already_processed)

            if inventory_rows != EXPECTED_INVENTORY_PAIRS:
                raise ValueError(
                    "Incomplete inventory update. "
                    f"Expected {EXPECTED_INVENTORY_PAIRS}, "
                    f"got {inventory_rows}"
                )

            if already_processed:
                print(
                    "Duplicate execution hour detected. "
                    "No additional inventory changes."
                )
            else:
                print(
                    "New simulated inventory hour created."
                )

            return bool(already_processed)

        # ----------------------------------------------------
        # 2C. CONTROLLED CATCH-UP
        # ----------------------------------------------------

        new_backfilled_hours = 0

        for record in missed_hours:

            missed_hour = record[0]

            print(
                "Backfilling execution hour:",
                missed_hour
            )

            repeated = process_hour(
                run_at=missed_hour
            )

            if not repeated:
                new_backfilled_hours += 1

        # ----------------------------------------------------
        # 2D. PROCESS CURRENT EXECUTION HOUR
        # ----------------------------------------------------

        print("Processing current execution hour...")

        current_repeated = process_hour()

        # ----------------------------------------------------
        # 2E. VERIFY INVENTORY STATE
        # ----------------------------------------------------

        state = hook.get_first("""
            SELECT
                COUNT(DISTINCT snapshot_hour),
                MAX(snapshot_hour),
                COUNT(*)
            FROM ops.live_inventory_hourly;
        """)

        if state is None:
            raise ValueError(
                "Inventory state verification failed."
            )

        total_hours, latest_hour, total_rows = state

        if latest_hour is None:
            raise ValueError(
                "No simulated inventory snapshots found."
            )

        if total_rows != (
            total_hours * EXPECTED_INVENTORY_PAIRS
        ):
            raise ValueError(
                "Inventory row count is inconsistent "
                "with the number of simulated hours."
            )

        print("================================")
        print("SIMULATION SUMMARY")
        print("================================")
        print("New backfilled hours:", new_backfilled_hours)
        print("Current hour repeated:", current_repeated)
        print("Total simulated hours:", total_hours)
        print("Latest simulated hour:", latest_hour)
        print("Total inventory rows:", total_rows)

        print("Inventory simulation PASSED!")

    # ========================================================
    # TASK 3: SAVE OPERATIONAL KPI SNAPSHOTS
    # ========================================================

    @task
    def save_operational_kpis():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        print("================================")
        print("SAVE OPERATIONAL KPI SNAPSHOTS")
        print("================================")

        # Self-correcting KPI update:
        # Insert any missing simulated hour,
        # including gaps earlier than the latest KPI.

        hook.run(
            sql="""
                INSERT INTO ops.hourly_operational_kpi_snapshots (
                    simulated_hour,
                    total_demand,
                    fulfilled_units,
                    unmet_units,
                    closing_stock,
                    stockout_rate_percent,
                    fulfillment_rate_percent,
                    projected_days_of_inventory,
                    captured_at
                )

                SELECT
                    v.snapshot_hour,
                    v.total_demand,
                    v.fulfilled_units,
                    v.unmet_units,
                    v.closing_stock,
                    v.stockout_rate_percent,
                    v.fulfillment_rate_percent,
                    v.projected_days_of_inventory,
                    NOW()

                FROM ops.v_live_hourly_kpis v

                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ops.hourly_operational_kpi_snapshots k
                    WHERE k.simulated_hour = v.snapshot_hour
                )

                ON CONFLICT (simulated_hour)
                DO NOTHING;
            """,
            autocommit=False
        )

        # ----------------------------------------------------
        # VERIFY KPI SNAPSHOT COMPLETENESS
        # ----------------------------------------------------

        result = hook.get_first("""
            SELECT
                COUNT(*),
                MAX(simulated_hour)
            FROM ops.hourly_operational_kpi_snapshots;
        """)

        if result is None:
            raise ValueError(
                "Operational KPI verification failed."
            )

        total_kpis, latest_kpi_hour = result

        missing_result = hook.get_first("""
            SELECT
                COUNT(*)
            FROM ops.v_live_hourly_kpis v
            WHERE NOT EXISTS (
                SELECT 1
                FROM ops.hourly_operational_kpi_snapshots k
                WHERE k.simulated_hour = v.snapshot_hour
            );
        """)

        if missing_result is None:
            raise ValueError(
                "Could not check missing KPI snapshots."
            )

        missing_kpis = missing_result[0]

        print("Total operational KPI snapshots:", total_kpis)
        print("Latest KPI hour:", latest_kpi_hour)
        print("Missing KPI snapshots:", missing_kpis)

        if missing_kpis != 0:
            raise ValueError(
                f"{missing_kpis} simulated hours "
                "have no operational KPI snapshot."
            )

        print("Operational KPI synchronization PASSED!")

    # ========================================================
    # TASK 4: MONITOR ANOMALIES
    # ========================================================

    @task
    def monitor_anomalies():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        print("================================")
        print("ANOMALY MONITORING")
        print("================================")

        # ----------------------------------------------------
        # VALIDATE EXISTING ANOMALY RESULTS
        # ----------------------------------------------------

        validation = hook.get_first("""
            SELECT
                (
                    SELECT COUNT(*)
                    FROM analytics.demand_spike_anomalies
                    WHERE z_score < 3
                       OR demand_units < 3
                       OR baseline_std <= 0
                ),

                (
                    SELECT COUNT(*)
                    FROM analytics.v_supplier_delay_anomalies
                    WHERE delay_days < 2
                       OR (
                           delay_days = 2
                           AND anomaly_severity <> 'WARNING'
                       )
                       OR (
                           delay_days >= 3
                           AND anomaly_severity <> 'CRITICAL'
                       )
                );
        """)

        if validation is None or any(validation):
            raise ValueError(
                f"Anomaly validation failed: {validation}"
            )

        print("Anomaly rule validation PASSED!")

        # ----------------------------------------------------
        # SAVE HOURLY ANOMALY COUNTS
        # ----------------------------------------------------

        hook.run("""
            INSERT INTO analytics.hourly_anomaly_snapshots (
                snapshot_hour,
                historical_demand_spikes,
                simulated_supplier_delays,
                supplier_warnings,
                supplier_critical,
                checked_at
            )

            SELECT
                date_trunc('hour', NOW()),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'DEMAND_SPIKE'
                ),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'SUPPLIER_DELAY'
                ),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'SUPPLIER_DELAY'
                      AND severity = 'WARNING'
                ),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'SUPPLIER_DELAY'
                      AND severity = 'CRITICAL'
                ),

                NOW()

            FROM analytics.v_control_tower_anomalies

            ON CONFLICT (snapshot_hour)
            DO UPDATE SET
                historical_demand_spikes =
                    EXCLUDED.historical_demand_spikes,

                simulated_supplier_delays =
                    EXCLUDED.simulated_supplier_delays,

                supplier_warnings =
                    EXCLUDED.supplier_warnings,

                supplier_critical =
                    EXCLUDED.supplier_critical,

                checked_at = NOW();
        """)

        # ----------------------------------------------------
        # VERIFY ANOMALY SNAPSHOT
        # ----------------------------------------------------

        result = hook.get_first("""
            SELECT
                historical_demand_spikes,
                simulated_supplier_delays,
                supplier_warnings,
                supplier_critical
            FROM analytics.hourly_anomaly_snapshots
            ORDER BY snapshot_hour DESC
            LIMIT 1;
        """)

        if result is None:
            raise ValueError(
                "Anomaly monitoring snapshot missing."
            )

        print("Historical demand spikes:", result[0])
        print("Simulated supplier delays:", result[1])
        print("Supplier warnings:", result[2])
        print("Critical supplier alerts:", result[3])

        print("Anomaly monitoring PASSED!")

    # ========================================================
    # TASK 5: REFRESH HOURLY KPI OVERVIEW
    # ========================================================

    @task
    def refresh_kpi_snapshot():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        print("================================")
        print("REFRESH KPI OVERVIEW")
        print("================================")

        hook.run("""
            INSERT INTO ops.hourly_kpi_snapshots (
                snapshot_hour,
                stockout_rate,
                customer_shipping_pct,
                supplier_on_time_pct,
                avg_supplier_lead_days,
                days_of_inventory,
                inventory_as_of,
                source_note
            )

            SELECT
                date_trunc('hour', NOW()),
                stockout_rate,
                customer_shipping_pct,
                supplier_on_time_pct,
                avg_supplier_lead_days,
                days_of_inventory,
                inventory_as_of,
                'Historical DataCo + simulated operations'

            FROM ops.v_kpi_overview

            ON CONFLICT (snapshot_hour)
            DO UPDATE SET

                stockout_rate =
                    EXCLUDED.stockout_rate,

                customer_shipping_pct =
                    EXCLUDED.customer_shipping_pct,

                supplier_on_time_pct =
                    EXCLUDED.supplier_on_time_pct,

                avg_supplier_lead_days =
                    EXCLUDED.avg_supplier_lead_days,

                days_of_inventory =
                    EXCLUDED.days_of_inventory,

                inventory_as_of =
                    EXCLUDED.inventory_as_of,

                source_note =
                    EXCLUDED.source_note,

                refreshed_at = NOW();
        """)

        print("Hourly KPI overview saved!")

    # ========================================================
    # TASK 6: FINAL PIPELINE VERIFICATION
    # ========================================================

    @task
    def verify_refresh():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        print("================================")
        print("FINAL PIPELINE VERIFICATION")
        print("================================")

        # ----------------------------------------------------
        # VERIFY HOURLY OVERVIEW REFRESH
        # ----------------------------------------------------

        result = hook.get_first("""
            SELECT
                COUNT(*),
                MAX(refreshed_at)
            FROM ops.hourly_kpi_snapshots
            WHERE snapshot_hour =
                date_trunc('hour', NOW());
        """)

        if result is None or result[0] != 1:
            raise ValueError(
                "Current hourly KPI overview is missing."
            )

        print("Latest KPI overview refresh:", result[1])

        # ----------------------------------------------------
        # VERIFY LATEST INVENTORY / KPI ALIGNMENT
        # ----------------------------------------------------

        simulation_check = hook.get_first("""
            SELECT
                (
                    SELECT MAX(snapshot_hour)
                    FROM ops.v_live_hourly_kpis
                ),
                (
                    SELECT MAX(simulated_hour)
                    FROM ops.hourly_operational_kpi_snapshots
                );
        """)

        if simulation_check is None:
            raise ValueError(
                "Simulation verification returned no result."
            )

        latest_inventory_hour = simulation_check[0]
        latest_operational_kpi_hour = simulation_check[1]

        print(
            "Latest inventory hour:",
            latest_inventory_hour
        )

        print(
            "Latest operational KPI hour:",
            latest_operational_kpi_hour
        )

        if latest_inventory_hour is None:
            raise ValueError(
                "No inventory snapshots available."
            )

        if latest_inventory_hour != latest_operational_kpi_hour:
            raise ValueError(
                "Latest operational KPI is not aligned "
                "with simulated inventory."
            )

        # ----------------------------------------------------
        # VERIFY NO MISSING HISTORICAL KPI HOURS
        # ----------------------------------------------------

        gap_check = hook.get_first("""
            SELECT COUNT(*)
            FROM ops.v_live_hourly_kpis v
            WHERE NOT EXISTS (
                SELECT 1
                FROM ops.hourly_operational_kpi_snapshots k
                WHERE k.simulated_hour = v.snapshot_hour
            );
        """)

        if gap_check is None:
            raise ValueError(
                "KPI history completeness check failed."
            )

        missing_hours = gap_check[0]

        print("Missing operational KPI hours:", missing_hours)

        if missing_hours != 0:
            raise ValueError(
                f"{missing_hours} operational KPI "
                "snapshots are missing."
            )

        print("================================")
        print("ALL PIPELINE CHECKS PASSED")
        print("================================")

    # ========================================================
    # TASK EXECUTION ORDER
    # ========================================================

    check = check_source_data()
    simulate = advance_inventory_simulation()
    operational_kpi = save_operational_kpis()
    anomalies = monitor_anomalies()
    refresh = refresh_kpi_snapshot()
    verify = verify_refresh()

    (
        check
        >> simulate
        >> operational_kpi
        >> anomalies
        >> refresh
        >> verify
    )


# ============================================================
# REGISTER DAG — ONLY ONCE
# ============================================================

hourly_supply_chain_pipeline()
