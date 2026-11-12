
from getpass import getpass
from pathlib import Path

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# 1. DATABASE CONNECTION
# ============================================================

password = getpass("PostgreSQL password: ")

engine = create_engine(
    URL.create(
        "postgresql+psycopg2",
        username="postgres",
        password=password,
        host="localhost",
        port=5432,
        database="supply_chain_db"
    )
)


# ============================================================
# 2. SETTINGS
# ============================================================

START_DATE = "2017-11-01"
END_DATE = "2018-02-01"  # Exclusive: includes January 31

TEST_DAYS = 30
FORECAST_DAYS = 30

ALPHA = 0.10

DATA_SOURCE = "DATACO_HISTORICAL"

MODEL_CROSTON = "CROSTON_SBA_V1"
MODEL_ZERO = "ZERO_BASELINE_V1"

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "dashboards"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 3. LOAD DAILY DEMAND FOR ALL PRODUCTS
# ============================================================

SQL = text("""
    WITH dates AS (
        SELECT generate_series(
            CAST(:start_date AS DATE),
            CAST(:end_date AS DATE) - INTERVAL '1 day',
            INTERVAL '1 day'
        )::DATE AS demand_date
    ),

    daily_sales AS (
        SELECT
            oi.product_id,
            co.order_at::DATE AS demand_date,
            SUM(oi.quantity)::FLOAT AS demand_units
        FROM dataco.order_items oi
        JOIN dataco.customer_orders co
            ON oi.order_id = co.order_id
        WHERE co.order_status NOT IN (
            'CANCELED',
            'SUSPECTED_FRAUD'
        )
          AND co.order_at >= CAST(:start_date AS DATE)
          AND co.order_at < CAST(:end_date AS DATE)
        GROUP BY
            oi.product_id,
            co.order_at::DATE
    )

    SELECT
        p.product_id,
        p.product_name,
        d.demand_date,
        COALESCE(s.demand_units, 0)::FLOAT AS demand_units
    FROM dataco.products p
    CROSS JOIN dates d
    LEFT JOIN daily_sales s
        ON s.product_id = p.product_id
       AND s.demand_date = d.demand_date
    ORDER BY
        p.product_id,
        d.demand_date;
""")


# ============================================================
# 4. CROSTON-SBA FORECAST FUNCTION
# ============================================================

def croston_sba(values, alpha=0.10):

    values = np.asarray(values, dtype=float)

    nonzero_indices = np.where(values > 0)[0]

    if len(nonzero_indices) == 0:
        return 0.0

    first_index = nonzero_indices[0]

    demand_estimate = values[first_index]
    interval_estimate = float(first_index + 1)

    last_demand_index = first_index

    for current_index in nonzero_indices[1:]:

        interval = current_index - last_demand_index

        demand_estimate = (
            alpha * values[current_index]
            + (1 - alpha) * demand_estimate
        )

        interval_estimate = (
            alpha * interval
            + (1 - alpha) * interval_estimate
        )

        last_demand_index = current_index

    forecast = (
        (1 - alpha / 2)
        * demand_estimate
        / interval_estimate
    )

    return max(float(forecast), 0.0)


# ============================================================
# 5. FORECASTING METHODS AND EVALUATION METRICS
# ============================================================

def naive_average(values):
    """Average daily demand across the training period."""

    values = np.asarray(values, dtype=float)

    if len(values) == 0:
        return 0.0

    return max(float(np.mean(values)), 0.0)


def recent_14_day_average(values):
    """Average demand over the most recent 14 training days."""

    values = np.asarray(values, dtype=float)

    recent_values = values[-14:]

    if len(recent_values) == 0:
        return 0.0

    return max(float(np.mean(recent_values)), 0.0)


def calculate_metrics(actual, predicted):

    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)

    if len(actual) == 0:
        return np.nan, np.nan

    mae = np.mean(np.abs(actual - predicted))

    total_actual = np.sum(np.abs(actual))

    if total_actual > 0:

        wape = (
            np.sum(np.abs(actual - predicted))
            / total_actual
        ) * 100

    else:
        wape = np.nan

    return float(mae), float(wape)


# ============================================================
# 6. MAIN EXECUTION
# ============================================================

def main():

    print("\nLoading historical product demand...")

    with engine.connect() as connection:

        df = pd.read_sql(
            SQL,
            connection,
            params={
                "start_date": START_DATE,
                "end_date": END_DATE
            }
        )

    if df.empty:
        raise ValueError("No product demand data found.")

    df["demand_date"] = pd.to_datetime(df["demand_date"])
    df["demand_units"] = df["demand_units"].astype(float)

    print("\n========== DATA SUMMARY ==========")

    print("Products:", df["product_id"].nunique())
    print("Historical days:", df["demand_date"].nunique())
    print("First date:", df["demand_date"].min().date())
    print("Last date:", df["demand_date"].max().date())

    date_count = df.groupby("product_id")["demand_date"].nunique()

    if date_count.nunique() != 1:
        raise ValueError(
            "Products do not have equal historical date coverage."
        )

    historical_days = df["demand_date"].nunique()

    if historical_days <= TEST_DAYS:
        raise ValueError(
            "Not enough historical days for the test split."
        )

    # --------------------------------------------------------
    # 7. TRAIN / TEST SPLIT AND FOUR-MODEL EVALUATION
    # --------------------------------------------------------

    print("\nPreparing four-model holdout evaluation...")

    all_actual = []

    all_croston_predictions = []
    all_zero_predictions = []
    all_naive_predictions = []
    all_recent_predictions = []

    product_metrics = []
    daily_backtest_records = []

    for product_id, group in df.groupby("product_id"):

        group = group.sort_values("demand_date")

        values = group["demand_units"].to_numpy()

        train = values[:-TEST_DAYS]
        test = values[-TEST_DAYS:]

        test_dates = group["demand_date"].iloc[-TEST_DAYS:]

        if len(train) == 0 or len(test) != TEST_DAYS:
            continue

        training_sales_days = int(np.sum(train > 0))
        test_sales_days = int(np.sum(test > 0))

        # Model 1: Croston-SBA with zero fallback
        croston_prediction = croston_sba(train, ALPHA)

        # Model 2: Zero-demand baseline
        zero_prediction = 0.0

        # Model 3: Historical average
        naive_prediction = naive_average(train)

        # Model 4: Recent 14-day average
        recent_prediction = recent_14_day_average(train)

        croston_predictions = np.full(
            TEST_DAYS, croston_prediction
        )

        zero_predictions = np.zeros(TEST_DAYS)

        naive_predictions = np.full(
            TEST_DAYS, naive_prediction
        )

        recent_predictions = np.full(
            TEST_DAYS, recent_prediction
        )

        all_actual.extend(test)

        all_croston_predictions.extend(croston_predictions)
        all_zero_predictions.extend(zero_predictions)
        all_naive_predictions.extend(naive_predictions)
        all_recent_predictions.extend(recent_predictions)

        # Product-level metrics

        croston_mae, croston_wape = calculate_metrics(
            test, croston_predictions
        )

        zero_mae, zero_wape = calculate_metrics(
            test, zero_predictions
        )

        naive_mae, naive_wape = calculate_metrics(
            test, naive_predictions
        )

        recent_mae, recent_wape = calculate_metrics(
            test, recent_predictions
        )

        product_metrics.append({

            "product_id": int(product_id),

            "product_name": group["product_name"].iloc[0],

            "training_sales_days": training_sales_days,
            "test_sales_days": test_sales_days,

            "training_units": round(float(np.sum(train)), 2),
            "test_units": round(float(np.sum(test)), 2),

            "croston_prediction": round(croston_prediction, 4),
            "croston_mae": round(croston_mae, 4),

            "croston_wape": (
                round(croston_wape, 2)
                if not np.isnan(croston_wape) else None
            ),

            "zero_prediction": zero_prediction,
            "zero_mae": round(zero_mae, 4),

            "zero_wape": (
                round(zero_wape, 2)
                if not np.isnan(zero_wape) else None
            ),

            "naive_prediction": round(naive_prediction, 4),
            "naive_mae": round(naive_mae, 4),

            "naive_wape": (
                round(naive_wape, 2)
                if not np.isnan(naive_wape) else None
            ),

            "recent_14d_prediction": round(recent_prediction, 4),
            "recent_14d_mae": round(recent_mae, 4),

            "recent_14d_wape": (
                round(recent_wape, 2)
                if not np.isnan(recent_wape) else None
            )

        })

        # Daily predictions for analysis

        for date, actual, croston_pred, zero_pred, naive_pred, recent_pred in zip(
            test_dates,
            test,
            croston_predictions,
            zero_predictions,
            naive_predictions,
            recent_predictions
        ):

            daily_backtest_records.append({

                "product_id": int(product_id),

                "product_name": group["product_name"].iloc[0],

                "demand_date": date.date(),

                "actual_units": float(actual),

                "croston_prediction": float(croston_pred),

                "zero_prediction": float(zero_pred),

                "naive_prediction": float(naive_pred),

                "recent_14d_prediction": float(recent_pred)

            })

    # --------------------------------------------------------
    # 8. OVERALL MODEL EVALUATION
    # --------------------------------------------------------

    if not product_metrics:
        raise ValueError(
            "No products were available for backtesting."
        )

    models = {

        "Croston-SBA": all_croston_predictions,

        "Zero baseline": all_zero_predictions,

        "Naive average": all_naive_predictions,

        "Recent 14-day average": all_recent_predictions

    }

    model_summary = []

    for model_name, predictions in models.items():

        mae, wape = calculate_metrics(
            all_actual,
            predictions
        )

        model_summary.append({

            "model": model_name,

            "mae": round(mae, 4),

            "wape": (
                round(wape, 2)
                if not np.isnan(wape) else None
            )

        })

    model_summary_df = pd.DataFrame(model_summary)

    product_metrics_df = pd.DataFrame(product_metrics)

    daily_backtest_df = pd.DataFrame(daily_backtest_records)

    print("\n========== MODEL COMPARISON ==========")

    print("Training period: 2017-11-01 to 2018-01-01")

    print("Test period: 2018-01-02 to 2018-01-31")

    print("Test days per product:", TEST_DAYS)

    print(model_summary_df.to_string(index=False))

    print(
        "\nProducts with training demand:",
        int(
            (product_metrics_df["training_sales_days"] > 0).sum()
        )
    )

    print(
        "Products with test demand:",
        int(
            (product_metrics_df["test_sales_days"] > 0).sum()
        )
    )

    print(
        "Products without training demand:",
        int(
            (product_metrics_df["training_sales_days"] == 0).sum()
        )
    )

    # --------------------------------------------------------
    # 8B. EVALUATION ON PRODUCTS WITH TEST-PERIOD DEMAND
    # --------------------------------------------------------

    active_product_ids = product_metrics_df.loc[
        product_metrics_df["test_sales_days"] > 0,
        "product_id"
    ].tolist()

    active_test_df = daily_backtest_df[
        daily_backtest_df["product_id"].isin(active_product_ids)
    ].copy()

    print("\n========== ACTIVE PRODUCT EVALUATION ==========")
    print("Products evaluated:", len(active_product_ids))
    print("Product-days evaluated:", len(active_test_df))

    active_model_columns = {
        "Croston-SBA": "croston_prediction",
        "Zero baseline": "zero_prediction",
        "Naive average": "naive_prediction",
        "Recent 14-day average": "recent_14d_prediction"
    }

    active_results = []

    for model_name, prediction_column in active_model_columns.items():

        mae, wape = calculate_metrics(
            active_test_df["actual_units"],
            active_test_df[prediction_column]
        )

        active_results.append({
            "model": model_name,
            "mae": round(mae, 4),
            "wape": round(wape, 2)
        })

    active_summary_df = pd.DataFrame(active_results)

    print(active_summary_df.to_string(index=False))

    active_summary_path = OUTPUT_DIR / "active_product_model_comparison.csv"
    active_summary_df.to_csv(active_summary_path, index=False)

    print("\nActive-product comparison saved:")
    print(active_summary_path)


    # --------------------------------------------------------
    # 9. SAVE BACKTEST RESULTS
    # --------------------------------------------------------

    metrics_path = OUTPUT_DIR / "product_backtest_metrics.csv"

    daily_path = OUTPUT_DIR / "product_backtest_daily_predictions.csv"

    summary_path = OUTPUT_DIR / "product_model_comparison.csv"

    model_summary_df.to_csv(summary_path, index=False)

    product_metrics_df.to_csv(metrics_path, index=False)

    daily_backtest_df.to_csv(daily_path, index=False)

    print("\nBacktest files saved:")

    print(summary_path)

    print(metrics_path)

    print(daily_path)

    # --------------------------------------------------------
    # 10. FINAL FORECAST USING ALL 92 DAYS
    # --------------------------------------------------------

    print("\nGenerating final 30-day product forecasts...")

    final_records = []

    last_historical_date = df["demand_date"].max().date()

    for product_id, group in df.groupby("product_id"):

        group = group.sort_values("demand_date")

        values = group["demand_units"].to_numpy()

        sales_days = int(np.sum(values > 0))

        if sales_days > 0:

            predicted_units = croston_sba(
                values,
                ALPHA
            )

            model_name = MODEL_CROSTON

        else:

            predicted_units = 0.0

            model_name = MODEL_ZERO

        trained_through = last_historical_date

        for day in range(1, FORECAST_DAYS + 1):

            forecast_date = (
                pd.Timestamp(last_historical_date)
                + pd.Timedelta(days=day)
            ).date()

            final_records.append({

                "product_id": int(product_id),

                "model_name": model_name,

                "forecast_date": forecast_date,

                "predicted_units": round(
                    float(predicted_units), 4
                ),

                "lower_bound": None,

                "upper_bound": None,

                "trained_through": trained_through,

                "data_source": DATA_SOURCE

            })

    # --------------------------------------------------------
    # 11. SAVE FORECASTS TO POSTGRESQL
    # --------------------------------------------------------

    insert_sql = text("""
        INSERT INTO analytics.product_demand_forecasts (
            product_id,
            model_name,
            forecast_date,
            predicted_units,
            lower_bound,
            upper_bound,
            trained_through,
            data_source
        )
        VALUES (
            :product_id,
            :model_name,
            :forecast_date,
            :predicted_units,
            :lower_bound,
            :upper_bound,
            :trained_through,
            :data_source
        )
        ON CONFLICT (
            product_id,
            model_name,
            forecast_date
        )
        DO UPDATE SET
            predicted_units = EXCLUDED.predicted_units,
            lower_bound = EXCLUDED.lower_bound,
            upper_bound = EXCLUDED.upper_bound,
            trained_through = EXCLUDED.trained_through,
            data_source = EXCLUDED.data_source,
            created_at = NOW();
    """)

    print("\nSaving forecasts to PostgreSQL...")

    with engine.begin() as connection:

        connection.execute(
            insert_sql,
            final_records
        )

    # --------------------------------------------------------
    # 12. FINAL SUMMARY
    # --------------------------------------------------------

    croston_products = sum(
        1 for record in final_records
        if record["model_name"] == MODEL_CROSTON
    ) // FORECAST_DAYS

    zero_products = sum(
        1 for record in final_records
        if record["model_name"] == MODEL_ZERO
    ) // FORECAST_DAYS

    print("\n========== FORECAST SUMMARY ==========")

    print("Products processed:", df["product_id"].nunique())

    print("Products using Croston-SBA:", croston_products)

    print("Products using zero baseline:", zero_products)

    print("Forecast days:", FORECAST_DAYS)

    print("Forecast rows saved:", len(final_records))

    print("\nPRODUCT-LEVEL FORECASTING COMPLETED.")


# ============================================================
# 13. SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        main()

    finally:
        engine.dispose()
