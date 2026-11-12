
import pandas as pd
import numpy as np

from getpass import getpass
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ==========================================
# 1. CONNECT TO POSTGRESQL
# ==========================================

password = getpass("Enter PostgreSQL password: ")

db_url = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password=password,
    host="localhost",
    port=5432,
    database="supply_chain_db"
)

engine = create_engine(db_url)


# ==========================================
# 2. EXTRACT HISTORICAL DEMAND
# ==========================================

query = """
SELECT
    oi.product_id,
    co.order_at::DATE AS demand_date,
    SUM(oi.quantity)::INT AS demand_units

FROM dataco.order_items oi

JOIN dataco.customer_orders co
    ON oi.order_id = co.order_id

WHERE co.order_status NOT IN (
    'CANCELED',
    'SUSPECTED_FRAUD'
)

GROUP BY
    oi.product_id,
    co.order_at::DATE

ORDER BY
    oi.product_id,
    demand_date;
"""

with engine.connect() as conn:
    df = pd.read_sql_query(text(query), conn)

df["demand_date"] = pd.to_datetime(df["demand_date"])

print("Historical product-date records:", len(df))
print("Products:", df["product_id"].nunique())


# ==========================================
# 3. DETECT UNUSUAL DEMAND SPIKES
# ==========================================

alerts = []

WINDOW = 30
MIN_HISTORY = 14
Z_THRESHOLD = 3.0

for product_id, group in df.groupby("product_id"):

    group = group.sort_values("demand_date")

    # Include days with no orders between
    # the first and last observed order.
    daily = (
        group.set_index("demand_date")["demand_units"]
        .astype(float)
    )

    dates = pd.date_range(
        daily.index.min(),
        daily.index.max(),
        freq="D"
    )

    daily = daily.reindex(dates, fill_value=0)

    # Use only previous days.
    # This prevents using today's demand
    # to calculate today's baseline.
    previous = daily.shift(1)

    rolling_mean = previous.rolling(
        WINDOW,
        min_periods=MIN_HISTORY
    ).mean()

    rolling_std = previous.rolling(
        WINDOW,
        min_periods=MIN_HISTORY
    ).std(ddof=0)

    prior_active_days = (
        daily.gt(0)
        .astype(int)
        .shift(1)
        .rolling(WINDOW, min_periods=MIN_HISTORY)
        .sum()
    )

    z_scores = (
        (daily - rolling_mean)
        / rolling_std.replace(0, np.nan)
    )

    # Flag substantial positive deviations,
    # not ordinary one-unit sales.
    anomaly_mask = (
        (z_scores >= Z_THRESHOLD)
        & (daily >= 3)
        & (daily >= 1.5 * rolling_mean)
        & (prior_active_days >= 3)
    )

    for date in daily.index[anomaly_mask.fillna(False)]:

        alerts.append({
            "product_id": int(product_id),
            "demand_date": date.date(),
            "demand_units": int(daily.loc[date]),
            "baseline_mean": round(
                float(rolling_mean.loc[date]), 3
            ),
            "baseline_std": round(
                float(rolling_std.loc[date]), 3
            ),
            "z_score": round(
                float(z_scores.loc[date]), 3
            )
        })


# ==========================================
# 4. SAVE RESULTS IN POSTGRESQL
# ==========================================

insert_sql = text("""
INSERT INTO analytics.demand_spike_anomalies (
    product_id,
    demand_date,
    demand_units,
    baseline_mean,
    baseline_std,
    z_score
)
VALUES (
    :product_id,
    :demand_date,
    :demand_units,
    :baseline_mean,
    :baseline_std,
    :z_score
)
ON CONFLICT (product_id, demand_date)
DO UPDATE SET
    demand_units = EXCLUDED.demand_units,
    baseline_mean = EXCLUDED.baseline_mean,
    baseline_std = EXCLUDED.baseline_std,
    z_score = EXCLUDED.z_score,
    detected_at = NOW();
""")

with engine.begin() as conn:

    # Replace only previous anomaly results.
    # Original DataCo tables remain untouched.
    conn.execute(text(
        "DELETE FROM analytics.demand_spike_anomalies"
    ))

    if alerts:
        conn.execute(insert_sql, alerts)


# ==========================================
# 5. DISPLAY SUMMARY
# ==========================================

print("\nDEMAND ANOMALY DETECTION COMPLETE")
print("Method: Rolling Z-score")
print("Historical window: 30 days")
print("Threshold:", Z_THRESHOLD)
print("Anomalies detected:", len(alerts))

if alerts:
    results = pd.DataFrame(alerts)
    print(
        results.sort_values(
            "z_score", ascending=False
        ).head(10).to_string(index=False)
    )
else:
    print(
        "No spikes met the current thresholds."
    )

engine.dispose()
