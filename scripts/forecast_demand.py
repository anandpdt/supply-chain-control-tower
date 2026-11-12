
from getpass import getpass
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from prophet import Prophet
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ======================================
# 1. DATABASE CONNECTION
# ======================================

password = getpass("PostgreSQL password: ")

engine = create_engine(URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password=password,
    host="localhost",
    port=5432,
    database="supply_chain_db"
))


# ======================================
# 2. LOAD REAL DATACO DEMAND
# ======================================

sql = text("""
SELECT
    co.order_at::DATE AS ds,
    SUM(oi.quantity)::FLOAT AS y
FROM dataco.order_items oi
JOIN dataco.customer_orders co
    ON oi.order_id = co.order_id
WHERE co.order_status NOT IN (
    'CANCELED', 'SUSPECTED_FRAUD'
)
GROUP BY co.order_at::DATE
ORDER BY ds
""")

with engine.connect() as conn:
    df = pd.read_sql_query(sql, conn)

df["ds"] = pd.to_datetime(df["ds"])

# Include calendar days with zero orders.
calendar = pd.DataFrame({
    "ds": pd.date_range(
        df["ds"].min(), df["ds"].max(), freq="D"
    )
})

df = calendar.merge(df, on="ds", how="left")
df["y"] = df["y"].fillna(0).astype(float)

# Focus on demand after the structural break
df = df[
    df["ds"] >= pd.Timestamp("2017-11-01")
].copy().reset_index(drop=True)

print("Historical days:", len(df))
print("First date:", df["ds"].min().date())
print("Last date:", df["ds"].max().date())


# ======================================
# 3. TIME-BASED TRAIN / TEST SPLIT
# ======================================

TEST_DAYS = 30
FORECAST_DAYS = 30

if len(df) < 60:
    raise ValueError("Insufficient historical data")

train = df.iloc[:-TEST_DAYS].copy()
test = df.iloc[-TEST_DAYS:].copy()


# ======================================
# 4. TRAIN AND EVALUATE PROPHET
# ======================================



def make_model():
    return Prophet(
        growth="flat",
        weekly_seasonality=True,
        yearly_seasonality=False,
        daily_seasonality=False,
        seasonality_prior_scale=0.1,
        interval_width=0.80
    )




model = make_model()
model.fit(train)

test_predictions = model.predict(test[["ds"]])

actual = test["y"].to_numpy()
predicted = np.maximum(
    test_predictions["yhat"].to_numpy(), 0
)

mae = np.mean(np.abs(actual - predicted))

wape = (
    100 * np.sum(np.abs(actual - predicted))
    / np.sum(actual)
    if np.sum(actual) > 0 else np.nan
)

# Simple comparison: fixed average demand
# from the last 7 training days.
naive_prediction = train["y"].tail(7).mean()
naive_mae = np.mean(
    np.abs(actual - naive_prediction)
)

print("\nMODEL EVALUATION")
print("Test days:", TEST_DAYS)
print("Prophet MAE:", round(mae, 3))
print("Prophet WAPE (%):", round(wape, 2))
print("Naive baseline MAE:", round(naive_mae, 3))


# ======================================
# 5. RETRAIN USING FULL HISTORY
# ======================================

final_model = make_model()
final_model.fit(df)

future_dates = pd.DataFrame({
    "ds": pd.date_range(
        df["ds"].max() + pd.Timedelta(days=1),
        periods=FORECAST_DAYS,
        freq="D"
    )
})

forecast = final_model.predict(future_dates)

for col in ["yhat", "yhat_lower", "yhat_upper"]:
    forecast[col] = forecast[col].clip(lower=0)


# ======================================
# 6. SAVE FORECAST INTO POSTGRESQL
# ======================================
MODEL_NAME = "PROPHET_POSTBREAK_FLAT_V3"
DATA_SOURCE = "DATACO_HISTORICAL"
records = []

for _, row in forecast.iterrows():
    records.append({
    "model_name": MODEL_NAME,
    "forecast_date": row["ds"].date(),
    "predicted_units": round(float(row["yhat"]), 2),
    "lower_bound": round(
        float(row["yhat_lower"]), 2
    ),
    "upper_bound": round(
        float(row["yhat_upper"]), 2
    ),
    "trained_through": df["ds"].max().date(),
    "data_source": DATA_SOURCE
})

insert = text("""
INSERT INTO analytics.demand_forecasts (
    model_name,
    forecast_date,
    predicted_units,
    lower_bound,
    upper_bound,
    trained_through,
    data_source
)
VALUES (
    :model_name,
    :forecast_date,
    :predicted_units,
    :lower_bound,
    :upper_bound,
    :trained_through,
    :data_source
)
ON CONFLICT (model_name, forecast_date)
DO UPDATE SET
    predicted_units = EXCLUDED.predicted_units,
    lower_bound = EXCLUDED.lower_bound,
    upper_bound = EXCLUDED.upper_bound,
    trained_through = EXCLUDED.trained_through,
    data_source = EXCLUDED.data_source,
    created_at = NOW()
""")

with engine.begin() as conn:
    conn.execute(insert, records)

print("\nForecast rows saved:", len(records))


# ======================================
# 7. SAVE FORECAST GRAPH
# ======================================

project_dir = Path(__file__).resolve().parents[1]
chart_dir = project_dir / "dashboards"
chart_dir.mkdir(exist_ok=True)

plt.figure(figsize=(12, 5))

recent = df.tail(180)

plt.plot(
    recent["ds"], recent["y"],
    label="Historical demand"
)

plt.plot(
    forecast["ds"], forecast["yhat"],
    label="Prophet forecast"
)

plt.fill_between(
    forecast["ds"],
    forecast["yhat_lower"],
    forecast["yhat_upper"],
    alpha=0.2,
    label="80% model interval"
)

plt.xlabel("Date")
plt.ylabel("Daily demand (units)")
plt.title("DataCo Daily Demand Forecast")
plt.legend()
plt.tight_layout()

chart_path = chart_dir / "prophet_forecast_v3.png"
plt.savefig(chart_path, dpi=200)
plt.close()

print("Forecast chart:", chart_path)

engine.dispose()
print("\nFORECASTING COMPLETED")
