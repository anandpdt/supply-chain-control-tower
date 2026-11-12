
import numpy as np
import pandas as pd
from getpass import getpass
from prophet import Prophet
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

password = getpass("PostgreSQL password: ")

engine = create_engine(URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password=password,
    host="localhost",
    port=5432,
    database="supply_chain_db"
))

query = text("""
    SELECT
        co.order_at::DATE AS ds,
        SUM(oi.quantity)::FLOAT AS y
    FROM dataco.order_items oi
    JOIN dataco.customer_orders co
        ON oi.order_id = co.order_id
    WHERE co.order_at >= DATE '2017-11-01'
      AND co.order_at < DATE '2018-02-01'
      AND co.order_status NOT IN (
          'CANCELED', 'SUSPECTED_FRAUD'
      )
    GROUP BY co.order_at::DATE
    ORDER BY ds
""")

with engine.connect() as conn:
    df = pd.read_sql_query(query, conn)

df["ds"] = pd.to_datetime(df["ds"])

calendar = pd.DataFrame({
    "ds": pd.date_range(
        "2017-11-01", "2018-01-31", freq="D"
    )
})

df = calendar.merge(df, on="ds", how="left")
df["y"] = df["y"].fillna(0).astype(float)

results = []

# Three historical training cutoffs.
# Each predicts the NEXT seven days.
for training_days in [45, 60, 75]:

    train = df.iloc[:training_days].copy()
    test = df.iloc[
        training_days:training_days + 7
    ].copy()

    model = Prophet(
        growth="flat",
        weekly_seasonality=True,
        yearly_seasonality=False,
        daily_seasonality=False,
        seasonality_prior_scale=0.1
    )

    model.fit(train)

    forecast = model.predict(test[["ds"]])

    actual = test["y"].to_numpy()
    predicted = np.maximum(
        forecast["yhat"].to_numpy(), 0
    )

    baseline = train["y"].tail(7).mean()

    prophet_mae = np.mean(
        np.abs(actual - predicted)
    )

    baseline_mae = np.mean(
        np.abs(actual - baseline)
    )

    results.append({
        "test_start": test["ds"].min().date(),
        "test_end": test["ds"].max().date(),
        "prophet_mae": round(prophet_mae, 3),
        "baseline_mae": round(baseline_mae, 3)
    })

results_df = pd.DataFrame(results)

print("\nROLLING BACKTEST RESULTS")
print(results_df.to_string(index=False))

print("\nAVERAGE MAE")
print(
    "Prophet:",
    round(results_df["prophet_mae"].mean(), 3)
)
print(
    "Baseline:",
    round(results_df["baseline_mae"].mean(), 3)
)

engine.dispose()
print("\nBACKTEST COMPLETED")
