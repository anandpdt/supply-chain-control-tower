
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# 1. APPLICATION CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Nexvia Supply Chain Control Tower",
    page_icon="ðŸ“¦",
    layout="wide",
    initial_sidebar_state="expanded"
)

DASHBOARD_DIR = Path(__file__).resolve().parent

st.title("NEXVIA | Supply Chain Control Tower")

st.caption(
    "Retail Analytics â€¢ Inventory Monitoring â€¢ "
    "Supplier Performance â€¢ Anomaly Detection â€¢ "
    "Demand Forecasting â€¢ Replenishment Intelligence"
)


# ============================================================
# 2. POSTGRESQL CONNECTION
# ============================================================

st.sidebar.title("NEXVIA")
st.sidebar.subheader("Database Connection")

password = st.sidebar.text_input(
    "PostgreSQL Password",
    type="password"
)

if not password:
    st.info("Enter your PostgreSQL password in the sidebar.")
    st.stop()


@st.cache_data(ttl=60, show_spinner=False)
def load_database_data(db_password):

    engine = create_engine(
        URL.create(
            "postgresql+psycopg2",
            username="postgres",
            password=db_password,
            host="localhost",
            port=5432,
            database="supply_chain_db"
        ),
        pool_pre_ping=True
    )

    queries = {

        "kpis": """
            SELECT *
            FROM analytics.v_replenishment_policy_kpis_30d
        """,

        "stockout": """
            SELECT *
            FROM analytics.v_product_stockout_risk_30d
            ORDER BY projected_shortage DESC, product_id
        """,

        "replenishment": """
            SELECT *
            FROM analytics.v_replenishment_policy_comparison_30d
            ORDER BY total_order_qty DESC
        """,

        "hourly": """
            SELECT *
            FROM ops.v_live_hourly_kpis
            ORDER BY snapshot_hour DESC
            LIMIT 168
        """,

        "suppliers": """
            SELECT *
            FROM ops.v_supplier_leadtime_kpi
            ORDER BY on_time_percent ASC
        """,

        "anomalies": """
            SELECT *
            FROM analytics.v_control_tower_anomalies
            ORDER BY event_date DESC, anomaly_type
        """,

        "aggregate_forecasts": """
            SELECT
                model_name,
                forecast_date,
                predicted_units,
                lower_bound,
                upper_bound,
                trained_through,
                data_source
            FROM analytics.demand_forecasts
            ORDER BY model_name, forecast_date
        """,


        "operational_forecasts": """
            SELECT
                f.product_id,
                p.product_name,
                f.model_name,
                f.asof_hour,
                f.forecast_hour,
                f.predicted_units,
                f.history_hours,
                f.data_source
            FROM analytics.operational_demand_forecasts f
            LEFT JOIN dataco.products p
                ON f.product_id = p.product_id
            WHERE f.model_name = 'HOURLY_MEAN_BASELINE_V1'
              AND f.data_source = 'SIMULATED_HOURLY'
              AND f.asof_hour = (
                  SELECT MAX(asof_hour)
                  FROM analytics.operational_demand_forecasts
                  WHERE model_name = 'HOURLY_MEAN_BASELINE_V1'
                    AND data_source = 'SIMULATED_HOURLY'
              )
            ORDER BY f.product_id, f.forecast_hour
        """,

        "operational_actions": """
            SELECT
                asof_hour,
                extra_delay_days,
                product_id,
                product_name,
                destination_warehouse,
                available_stock,
                required_stock,
                original_shortage,
                recommended_transfer_units,
                transfer_sources,
                emergency_procurement_units,
                recommended_action
            FROM analytics.v_operational_replenishment_actions
            ORDER BY
                extra_delay_days,
                emergency_procurement_units DESC,
                product_id,
                destination_warehouse
        """,

        "operational_sync": """
            SELECT
                (
                    SELECT MAX(snapshot_hour)
                    FROM ops.live_inventory_hourly
                ) AS latest_inventory_hour,

                (
                    SELECT MAX(asof_hour)
                    FROM analytics.operational_demand_forecasts
                    WHERE model_name = 'HOURLY_MEAN_BASELINE_V1'
                      AND data_source = 'SIMULATED_HOURLY'
                ) AS latest_forecast_hour
        """,



        "product_forecasts": """
            SELECT
                f.product_id,
                p.product_name,
                f.model_name,
                f.forecast_date,
                f.predicted_units,
                f.lower_bound,
                f.upper_bound,
                f.trained_through,
                f.data_source
            FROM analytics.product_demand_forecasts f
            LEFT JOIN dataco.products p
                ON f.product_id = p.product_id
            ORDER BY f.product_id, f.forecast_date
        """
    }

    try:
        with engine.connect() as connection:
            data = {
                name: pd.read_sql(text(query), connection)
                for name, query in queries.items()
            }
        return data

    finally:
        engine.dispose()


@st.cache_data(ttl=60, show_spinner=False)
def load_model_evaluations():

    filenames = {
        "overall": "product_model_comparison.csv",
        "active": "active_product_model_comparison.csv",
        "backtest": "product_backtest_daily_predictions.csv"
    }

    results = {}

    for key, filename in filenames.items():

        filepath = DASHBOARD_DIR / filename

        if filepath.exists():
            results[key] = pd.read_csv(filepath)
        else:
            results[key] = pd.DataFrame()

    return results


try:
    with st.spinner("Loading Nexvia data..."):
        data = load_database_data(password)
        evaluations = load_model_evaluations()

except Exception as error:
    st.error("Unable to connect to PostgreSQL or load data.")
    st.exception(error)
    st.stop()


st.sidebar.success("PostgreSQL Connected")

if st.sidebar.button("Refresh Dashboard"):
    load_database_data.clear()
    load_model_evaluations.clear()
    st.rerun()

st.sidebar.divider()

st.sidebar.caption(
    "Historical 2018 forecasts and simulated "
    "2026 inventory operations are kept separate."
)


# ============================================================
# 3. DASHBOARD TABS
# ============================================================

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Historical Replenishment",
    "Hourly Inventory",
    "Supplier Performance",
    "Anomaly Detection",
    "Demand Forecasting",
    "Risk-Aware Replenishment"
])


# ============================================================
# TAB 1 â€” HISTORICAL REPLENISHMENT
# ============================================================

with tab1:

    st.header("Historical Replenishment Scenario")

    st.caption(
        "Februaryâ€“March 2018 demand forecast scenario. "
        "Results are simulated, not actual operational outcomes."
    )

    kpis = data["kpis"]
    stockout = data["stockout"].copy()
    replenishment = data["replenishment"].copy()

    if kpis.empty:
        st.warning("No replenishment KPI data available.")

    else:
        r = kpis.iloc[0]

        st.subheader("Executive Overview")

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Total Products",
            int(r["total_products"])
        )

        c2.metric(
            "At-Risk Products",
            int(r["at_risk_products"])
        )

        c3.metric(
            "Recommended Order",
            f"{int(r['total_order_units']):,} units"
        )

        c4.metric(
            "Modeled Shortage Reduction",
            f"{float(r['emergency_reduction_percent']):.2f}%"
        )

        st.divider()

        st.subheader("Inventory Policy Comparison")

        policy_df = pd.DataFrame({
            "Policy": [
                "No Replenishment",
                "Regular Replenishment",
                "Emergency + Regular"
            ],
            "Projected Shortage": [
                float(r["baseline_shortage"]),
                float(r["normal_policy_shortage"]),
                float(r["emergency_policy_shortage"])
            ]
        })

        st.bar_chart(
            policy_df.set_index("Policy"),
            height=330
        )

        c1, c2, c3 = st.columns(3)

        c1.metric(
            "Baseline Shortage",
            f"{float(r['baseline_shortage']):,.2f}"
        )

        c2.metric(
            "Regular Policy Shortage",
            f"{float(r['normal_policy_shortage']):,.2f}"
        )

        c3.metric(
            "Emergency Policy Shortage",
            f"{float(r['emergency_policy_shortage']):,.2f}"
        )

        st.divider()

        st.subheader("Replenishment Order Breakdown")

        c1, c2, c3 = st.columns(3)

        c1.metric("Total Units", int(r["total_order_units"]))
        c2.metric("Emergency Units", int(r["emergency_units"]))
        c3.metric("Regular Units", int(r["regular_units"]))

        order_df = pd.DataFrame({
            "Order Type": ["Emergency", "Regular"],
            "Units": [
                int(r["emergency_units"]),
                int(r["regular_units"])
            ]
        })

        st.bar_chart(
            order_df.set_index("Order Type"),
            height=260
        )

        st.divider()

        st.subheader("Product-Level Stockout Risk")

        if not stockout.empty:

            risk_filter = st.selectbox(
                "Risk Category",
                [
                    "All Products",
                    "AT RISK",
                    "COVERED",
                    "NO FORECAST DEMAND"
                ],
                key="risk_filter"
            )

            if risk_filter == "All Products":
                filtered = stockout.copy()
            else:
                filtered = stockout[
                    stockout["stockout_risk"] == risk_filter
                ].copy()

            st.write(f"Showing {len(filtered)} products")

            st.dataframe(
                filtered,
                width="stretch",
                hide_index=True
            )

            risk_counts = (
                stockout["stockout_risk"]
                .value_counts()
                .rename_axis("Risk Category")
                .reset_index(name="Products")
            )

            st.subheader("Stockout Risk Distribution")

            st.bar_chart(
                risk_counts.set_index("Risk Category"),
                height=270
            )

            at_risk = stockout[
                stockout["stockout_risk"] == "AT RISK"
            ].copy()

            if not at_risk.empty:

                at_risk["projected_shortage"] = pd.to_numeric(
                    at_risk["projected_shortage"],
                    errors="coerce"
                ).fillna(0)

                st.subheader("Top 10 Projected Shortages")

                top_risk = at_risk.nlargest(
                    10, "projected_shortage"
                )

                st.bar_chart(
                    top_risk.set_index("product_name")[
                        ["projected_shortage"]
                    ],
                    horizontal=True,
                    height=370
                )

        st.divider()

        st.subheader("Replenishment Recommendations")

        if not replenishment.empty:

            st.dataframe(
                replenishment,
                width="stretch",
                hide_index=True
            )

            st.subheader("Emergency Replenishment")

            emergency = replenishment[
                replenishment["emergency_qty"] > 0
            ]

            st.dataframe(
                emergency[
                    [
                        "product_id",
                        "product_name",
                        "lead_days",
                        "total_order_qty",
                        "emergency_qty",
                        "regular_qty"
                    ]
                ],
                width="stretch",
                hide_index=True
            )

        st.info(
            "The scenario assumes uniform daily demand, "
            "pooled inventory and immediately available "
            "emergency stock. The 100% modeled reduction "
            "does not demonstrate real-world optimization."
        )


# ============================================================
# TAB 2 â€” HOURLY INVENTORY
# ============================================================

with tab2:

    st.header("Simulated Hourly Inventory Monitoring")

    st.caption(
        "November 2026 simulated operational inventory. "
        "Not directly comparable with 2018 forecasts."
    )

    hourly = data["hourly"].copy()

    if hourly.empty:
        st.warning("No hourly inventory records available.")

    else:

        hourly["snapshot_hour"] = pd.to_datetime(
            hourly["snapshot_hour"]
        )

        hourly = hourly.sort_values(
            "snapshot_hour",
            ascending=False
        )

        latest = hourly.iloc[0]

        st.caption(
            f"Latest recorded snapshot: {latest['snapshot_hour']}"
        )

        st.subheader("Inventory Status")

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Closing Stock",
            f"{int(latest['closing_stock']):,}"
        )

        c2.metric(
            "Hourly Demand",
            int(latest["total_demand"])
        )

        c3.metric(
            "Fulfilled Units",
            int(latest["fulfilled_units"])
        )

        c4.metric(
            "Unmet Units",
            int(latest["unmet_units"])
        )

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Stockout Rate",
            f"{float(latest['stockout_rate_percent']):.2f}%"
        )

        c2.metric(
            "Fulfillment Rate",
            f"{float(latest['fulfillment_rate_percent']):.2f}%"
        )

        c3.metric(
            "Stockout Events",
            int(latest["stockout_events"])
        )

        c4.metric(
            "Projected Days of Inventory",
            f"{float(latest['projected_days_of_inventory']):.2f}"
        )

        st.divider()

        chart_df = hourly.sort_values(
            "snapshot_hour"
        ).copy()

        numeric_columns = [
            "opening_stock",
            "closing_stock",
            "received_units",
            "total_demand",
            "fulfilled_units",
            "unmet_units",
            "stockout_rate_percent",
            "fulfillment_rate_percent",
            "projected_days_of_inventory"
        ]

        for column in numeric_columns:
            chart_df[column] = pd.to_numeric(
                chart_df[column],
                errors="coerce"
            ).fillna(0)

        chart_df = chart_df.set_index("snapshot_hour")

        st.subheader("Inventory Stock Trend")

        st.line_chart(
            chart_df[
                ["opening_stock", "closing_stock"]
            ],
            height=330
        )

        st.subheader("Demand vs Fulfillment")

        st.bar_chart(
            chart_df[
                [
                    "total_demand",
                    "fulfilled_units",
                    "unmet_units"
                ]
            ],
            height=330
        )

        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Stockout Rate")

            st.line_chart(
                chart_df[["stockout_rate_percent"]],
                height=270
            )

        with c2:
            st.subheader("Fulfillment Rate")

            st.line_chart(
                chart_df[["fulfillment_rate_percent"]],
                height=270
            )

        st.subheader("Projected Days of Inventory")

        st.line_chart(
            chart_df[["projected_days_of_inventory"]],
            height=280
        )

        st.subheader("Recent Hourly Records")

        st.dataframe(
            hourly.head(24),
            width="stretch",
            hide_index=True
        )


# ============================================================
# TAB 3 â€” SUPPLIER PERFORMANCE
# ============================================================

with tab3:

    st.header("Supplier Performance & Reliability")

    st.caption("Simulated supplier delivery performance.")

    suppliers = data["suppliers"].copy()

    if suppliers.empty:
        st.warning("No supplier performance data.")

    else:

        for column in [
            "expected_lead_days",
            "total_deliveries",
            "avg_actual_lead_days",
            "avg_delay_days",
            "on_time_percent"
        ]:
            suppliers[column] = pd.to_numeric(
                suppliers[column],
                errors="coerce"
            )

        suppliers = suppliers.sort_values(
            "on_time_percent"
        )

        total_deliveries = int(
            suppliers["total_deliveries"].sum()
        )

        overall_ontime = (
            (
                suppliers["on_time_percent"]
                * suppliers["total_deliveries"]
            ).sum()
            / max(total_deliveries, 1)
        )

        lowest = suppliers.iloc[0]
        highest = suppliers.iloc[-1]

        st.subheader("Supplier Overview")

        c1, c2, c3, c4 = st.columns(4)

        c1.metric("Total Suppliers", len(suppliers))

        c2.metric(
            "Total Deliveries",
            total_deliveries
        )

        c3.metric(
            "Overall On-Time Rate",
            f"{overall_ontime:.2f}%"
        )

        c4.metric(
            "Lowest On-Time Rate",
            f"{float(lowest['on_time_percent']):.2f}%"
        )

        st.divider()

        st.subheader("On-Time Delivery Ranking")

        st.bar_chart(
            suppliers.set_index("supplier_name")[
                ["on_time_percent"]
            ],
            horizontal=True,
            height=330
        )

        st.subheader("Expected vs Actual Lead Times")

        st.bar_chart(
            suppliers.set_index("supplier_name")[
                [
                    "expected_lead_days",
                    "avg_actual_lead_days"
                ]
            ],
            height=330
        )

        st.subheader("Average Delivery Delay")

        st.bar_chart(
            suppliers.set_index("supplier_name")[
                ["avg_delay_days"]
            ],
            height=280
        )

        st.subheader("Supplier Details")

        st.dataframe(
            suppliers,
            width="stretch",
            hide_index=True
        )

        c1, c2 = st.columns(2)

        with c1:
            st.warning(
                f"Lowest reliability: {lowest['supplier_name']} "
                f"({float(lowest['on_time_percent']):.2f}%)"
            )

        with c2:
            st.success(
                f"Highest reliability: {highest['supplier_name']} "
                f"({float(highest['on_time_percent']):.2f}%)"
            )


# ============================================================
# TAB 4 â€” ANOMALY DETECTION
# ============================================================

with tab4:

    st.header("Anomaly Detection & Alerts")

    st.caption(
        "Historical demand spikes and simulated supplier "
        "delay records. These are not current live alerts."
    )

    anomalies = data["anomalies"].copy()

    if anomalies.empty:
        st.warning("No anomaly data available.")

    else:

        anomalies["event_date"] = pd.to_datetime(
            anomalies["event_date"],
            errors="coerce"
        )

        supplier_mask = (
            anomalies["anomaly_type"]
            .fillna("")
            .str.contains("SUPPLIER", case=False)
        )

        demand_mask = (
            anomalies["anomaly_type"] == "DEMAND_SPIKE"
        )

        critical_mask = (
            anomalies["severity"] == "CRITICAL"
        )

        warning_mask = (
            anomalies["severity"] == "WARNING"
        )

        st.subheader("Anomaly Overview")

        c1, c2, c3, c4 = st.columns(4)

        c1.metric("All Anomalies", len(anomalies))

        c2.metric(
            "Demand Spikes",
            int(demand_mask.sum())
        )

        c3.metric(
            "Critical Supplier Delays",
            int((supplier_mask & critical_mask).sum())
        )

        c4.metric(
            "Supplier Warnings",
            int((supplier_mask & warning_mask).sum())
        )

        st.divider()

        st.subheader("Anomaly Summary")

        summary = (
            anomalies.groupby(
                [
                    "anomaly_type",
                    "data_source",
                    "severity"
                ],
                dropna=False
            )
            .agg(
                total_anomalies=("anomaly_type", "size"),
                first_event=("event_date", "min"),
                latest_event=("event_date", "max")
            )
            .reset_index()
        )

        st.dataframe(
            summary,
            width="stretch",
            hide_index=True
        )

        st.subheader("Anomaly Distribution")

        type_counts = (
            anomalies["anomaly_type"]
            .value_counts()
            .rename_axis("Anomaly Type")
            .reset_index(name="Count")
        )

        st.bar_chart(
            type_counts.set_index("Anomaly Type"),
            height=280
        )

        st.subheader("Monthly Anomaly Trend")

        trend = anomalies.dropna(
            subset=["event_date"]
        ).copy()

        trend["month"] = (
            trend["event_date"]
            .dt.to_period("M")
            .dt.to_timestamp()
        )

        monthly = (
            trend.groupby(["month", "anomaly_type"])
            .size()
            .unstack(fill_value=0)
            .sort_index()
        )

        if not monthly.empty:
            st.line_chart(monthly, height=320)

        st.divider()

        st.subheader("Supplier Delay Alerts")

        supplier_alerts = anomalies[supplier_mask].copy()

        if not supplier_alerts.empty:

            st.dataframe(
                supplier_alerts,
                width="stretch",
                hide_index=True
            )

        st.divider()

        st.subheader("Anomaly Explorer")

        c1, c2, c3 = st.columns(3)

        type_options = [
            "All"
        ] + sorted(
            anomalies["anomaly_type"]
            .dropna().unique().tolist()
        )

        severity_options = [
            "All"
        ] + sorted(
            anomalies["severity"]
            .dropna().unique().tolist()
        )

        source_options = [
            "All"
        ] + sorted(
            anomalies["data_source"]
            .dropna().unique().tolist()
        )

        with c1:
            selected_type = st.selectbox(
                "Anomaly Type",
                type_options
            )

        with c2:
            selected_severity = st.selectbox(
                "Severity",
                severity_options
            )

        with c3:
            selected_source = st.selectbox(
                "Data Source",
                source_options
            )

        filtered = anomalies.copy()

        if selected_type != "All":
            filtered = filtered[
                filtered["anomaly_type"] == selected_type
            ]

        if selected_severity != "All":
            filtered = filtered[
                filtered["severity"] == selected_severity
            ]

        if selected_source != "All":
            filtered = filtered[
                filtered["data_source"] == selected_source
            ]

        st.write(f"Showing {len(filtered):,} anomaly records")

        st.dataframe(
            filtered,
            width="stretch",
            hide_index=True
        )

        st.download_button(
            "Download Anomalies CSV",
            data=filtered.to_csv(index=False).encode("utf-8"),
            file_name="nexvia_anomalies.csv",
            mime="text/csv"
        )


# ============================================================
# TAB 5 â€” DEMAND FORECASTING
# ============================================================

with tab5:

    st.header("Demand Forecasting & Model Evaluation")

    st.caption(
        "Historical DataCo forecasting experiments. "
        "Forecast period: February 1 â€“ March 2, 2018."
    )

    aggregate = data["aggregate_forecasts"].copy()
    products = data["product_forecasts"].copy()

    # --------------------------------------------------------
    # 5A. FORECAST OVERVIEW
    # --------------------------------------------------------

    st.subheader("Forecasting Overview")

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Aggregate Forecast Models",
        aggregate["model_name"].nunique()
    )

    c2.metric(
        "Aggregate Forecast Rows",
        len(aggregate)
    )

    c3.metric(
        "Products Forecasted",
        products["product_id"].nunique()
    )

    c4.metric(
        "Product Forecast Rows",
        f"{len(products):,}"
    )

    st.divider()

    # --------------------------------------------------------
    # 5B. AGGREGATE PROPHET FORECASTS
    # --------------------------------------------------------

    st.subheader("Aggregate Prophet Demand Forecast")

    if aggregate.empty:

        st.warning("No aggregate forecast data available.")

    else:

        aggregate["forecast_date"] = pd.to_datetime(
            aggregate["forecast_date"]
        )

        for column in [
            "predicted_units",
            "lower_bound",
            "upper_bound"
        ]:
            aggregate[column] = pd.to_numeric(
                aggregate[column],
                errors="coerce"
            )

        model_names = sorted(
            aggregate["model_name"].unique().tolist()
        )

        selected_model = st.selectbox(
            "Select Prophet Model",
            model_names,
            key="prophet_model_selector"
        )

        selected_forecast = aggregate[
            aggregate["model_name"] == selected_model
        ].sort_values("forecast_date").copy()

        total_demand = selected_forecast[
            "predicted_units"
        ].sum()

        avg_demand = selected_forecast[
            "predicted_units"
        ].mean()

        c1, c2, c3 = st.columns(3)

        c1.metric(
            "Selected Model",
            selected_model
        )

        c2.metric(
            "30-Day Forecast",
            f"{total_demand:,.2f} units"
        )

        c3.metric(
            "Average Daily Forecast",
            f"{avg_demand:,.2f} units"
        )

        fig = go.Figure()

        # Add prediction interval only when valid.
        bounds = selected_forecast.dropna(
            subset=["lower_bound", "upper_bound"]
        )

        bounds_valid = (
            len(bounds) == len(selected_forecast)
            and (
                bounds["lower_bound"]
                <= bounds["upper_bound"]
            ).all()
        )

        if bounds_valid:

            fig.add_trace(
                go.Scatter(
                    x=bounds["forecast_date"],
                    y=bounds["upper_bound"],
                    mode="lines",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip"
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=bounds["forecast_date"],
                    y=bounds["lower_bound"],
                    mode="lines",
                    fill="tonexty",
                    line=dict(width=0),
                    name="Prediction interval",
                    hoverinfo="skip"
                )
            )

        fig.add_trace(
            go.Scatter(
                x=selected_forecast["forecast_date"],
                y=selected_forecast["predicted_units"],
                mode="lines+markers",
                name="Predicted demand"
            )
        )

        fig.update_layout(
            title="30-Day Prophet Forecast",
            xaxis_title="Forecast Date",
            yaxis_title="Predicted Units",
            height=390
        )

        st.plotly_chart(
            fig,
            width="stretch"
        )

        st.caption(
            "Prediction intervals are displayed only when "
            "both bounds are available and valid. Their "
            "presence does not establish forecast accuracy."
        )

        st.subheader("Compare All Prophet Models")

        comparison = aggregate.pivot_table(
            index="forecast_date",
            columns="model_name",
            values="predicted_units",
            aggfunc="mean"
        ).sort_index()

        st.line_chart(
            comparison,
            height=350
        )

        aggregate_summary = (
            aggregate.groupby("model_name")
            .agg(
                forecast_days=("forecast_date", "nunique"),
                total_predicted_units=("predicted_units", "sum"),
                avg_daily_units=("predicted_units", "mean")
            )
            .reset_index()
        )

        st.dataframe(
            aggregate_summary,
            width="stretch",
            hide_index=True
        )

        st.warning(
            "The saved Prophet models produce substantially "
            "different demand totals. These are alternative "
            "experiments, not forecasts to be added together. "
            "The zero-demand model needs further evaluation."
        )

    st.divider()

    # --------------------------------------------------------
    # 5C. PRODUCT-LEVEL DEMAND FORECASTS
    # --------------------------------------------------------

    st.subheader("Product-Level Demand Forecasting")

    if products.empty:

        st.warning("No product-level forecasts available.")

    else:

        products["forecast_date"] = pd.to_datetime(
            products["forecast_date"]
        )

        products["predicted_units"] = pd.to_numeric(
            products["predicted_units"],
            errors="coerce"
        )

        product_names = (
            products.drop_duplicates("product_id")
            .set_index("product_id")["product_name"]
            .to_dict()
        )

        product_ids = sorted(
            products["product_id"].dropna().unique().tolist()
        )

        selected_product = st.selectbox(
            "Select Product",
            product_ids,
            format_func=lambda pid: (
                f"{pid} â€” {product_names.get(pid, 'Unknown')}"
            ),
            key="product_forecast_selector"
        )

        product_data = products[
            products["product_id"] == selected_product
        ].sort_values("forecast_date").copy()

        product_model = (
            ", ".join(
                sorted(
                    product_data["model_name"]
                    .dropna().unique().tolist()
                )
            )
        )

        product_total = product_data[
            "predicted_units"
        ].sum()

        c1, c2, c3 = st.columns(3)

        c1.metric(
            "Product ID",
            selected_product
        )

        c2.metric(
            "Forecast Model",
            product_model
        )

        c3.metric(
            "30-Day Demand",
            f"{product_total:.2f} units"
        )

        st.line_chart(
            product_data.set_index("forecast_date")[
                ["predicted_units"]
            ],
            height=330
        )

        st.dataframe(
            product_data,
            width="stretch",
            hide_index=True
        )

        st.download_button(
            "Download Selected Product Forecast",
            data=product_data.to_csv(
                index=False
            ).encode("utf-8"),
            file_name=(
                f"product_{selected_product}_forecast.csv"
            ),
            mime="text/csv"
        )

        st.caption(
            "Croston-SBA produces a constant expected daily "
            "demand for this forecast horizon. A zero "
            "baseline means the model predicts zero, not "
            "that the product cannot experience demand."
        )

    st.divider()

    # --------------------------------------------------------
    # 5D. FOUR-MODEL EVALUATION
    # --------------------------------------------------------

    st.subheader("Forecast Model Evaluation")

    overall_eval = evaluations["overall"]
    active_eval = evaluations["active"]

    if overall_eval.empty or active_eval.empty:

        st.warning(
            "Model evaluation CSV files are missing. "
            "Run scripts/forecast_product_demand.py "
            "to regenerate them."
        )

    else:

        overall_eval = overall_eval.copy()
        active_eval = active_eval.copy()

        st.write("Overall 118-Product Evaluation")

        st.dataframe(
            overall_eval,
            width="stretch",
            hide_index=True
        )

        st.write("Active-Product Evaluation")

        st.caption(
            "Evaluation on the nine products that had "
            "demand in the January 2018 test period."
        )

        st.dataframe(
            active_eval,
            width="stretch",
            hide_index=True
        )

        active_eval["mae"] = pd.to_numeric(
            active_eval["mae"],
            errors="coerce"
        )

        active_eval["wape"] = pd.to_numeric(
            active_eval["wape"],
            errors="coerce"
        )

        st.subheader("Active-Product MAE Comparison")

        st.bar_chart(
            active_eval.set_index("model")[["mae"]],
            height=320
        )

        st.subheader("Active-Product WAPE Comparison")

        st.bar_chart(
            active_eval.set_index("model")[["wape"]],
            height=320
        )

        st.info(
            "Lower MAE and WAPE indicate lower numerical "
            "forecast error. However, the zero baseline "
            "missed all actual demand. Forecast selection "
            "must also consider stockout and business costs."
        )

    st.divider()

    # --------------------------------------------------------
    # 5E. HISTORICAL BACKTEST EXPLORER
    # --------------------------------------------------------

    st.subheader("Historical Backtest Explorer")

    backtest = evaluations["backtest"]

    if backtest.empty:

        st.warning(
            "Daily backtest CSV not available."
        )

    else:

        backtest = backtest.copy()

        backtest["demand_date"] = pd.to_datetime(
            backtest["demand_date"]
        )

        backtest_ids = sorted(
            backtest["product_id"].unique().tolist()
        )

        backtest_product = st.selectbox(
            "Select Backtest Product",
            backtest_ids,
            key="backtest_product_selector"
        )

        selected_backtest = backtest[
            backtest["product_id"] == backtest_product
        ].sort_values("demand_date").copy()

        prediction_columns = [
            "actual_units",
            "croston_prediction",
            "zero_prediction",
            "naive_prediction",
            "recent_14d_prediction"
        ]

        st.line_chart(
            selected_backtest.set_index("demand_date")[
                prediction_columns
            ],
            height=370
        )

        st.dataframe(
            selected_backtest,
            width="stretch",
            hide_index=True
        )

        st.caption(
            "Backtest: January 2â€“31, 2018. "
            "This compares actual historical demand "
            "with forecasts generated from training data."
        )

# ============================================================
# OPERATIONAL DEMAND FORECASTING - STAGE 5.6
# ============================================================

with tab5:

    st.divider()

    st.header("Operational 24-Hour Demand Forecast")

    st.caption(
        "Next 24 simulated hours, based on historical "
        "SIMULATED_HOURLY inventory demand. "
        "Separate from the 2018 DataCo forecasts."
    )

    operational = data["operational_forecasts"].copy()

    if operational.empty:

        st.warning(
            "No operational forecasts available. "
            "Run the operational forecasting Airflow DAG."
        )

    else:

        operational["asof_hour"] = pd.to_datetime(
            operational["asof_hour"]
        )

        operational["forecast_hour"] = pd.to_datetime(
            operational["forecast_hour"]
        )

        operational["predicted_units"] = pd.to_numeric(
            operational["predicted_units"],
            errors="coerce"
        ).fillna(0)

        latest_cutoff = operational["asof_hour"].max()

        total_products = operational[
            "product_id"
        ].nunique()

        forecast_hours = operational[
            "forecast_hour"
        ].nunique()

        positive_products = operational.loc[
            operational["predicted_units"] > 0,
            "product_id"
        ].nunique()

        total_forecast = operational[
            "predicted_units"
        ].sum()

        st.caption(
            f"Latest forecast cutoff: {latest_cutoff}"
        )

        # Check whether a newer inventory snapshot exists.
        if not data["hourly"].empty:

            latest_inventory = pd.to_datetime(
                data["hourly"]["snapshot_hour"]
            ).max()

            if latest_inventory > latest_cutoff:

                st.warning(
                    "The available inventory data is newer "
                    "than this forecast. Refresh the "
                    "operational forecasting DAG."
                )

        # ----------------------------------------------------
        # KPI CARDS
        # ----------------------------------------------------

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Products Forecasted",
            f"{total_products:,}"
        )

        c2.metric(
            "Forecast Hours",
            forecast_hours
        )

        c3.metric(
            "Products with Positive Forecast",
            positive_products
        )

        c4.metric(
            "Predicted 24h Demand",
            f"{total_forecast:.2f} units"
        )

        st.divider()

        # ----------------------------------------------------
        # TOTAL OPERATIONAL DEMAND TREND
        # ----------------------------------------------------

        st.subheader("Forecast Demand by Hour")

        hourly_forecast = (
            operational.groupby(
                "forecast_hour",
                as_index=False
            )["predicted_units"]
            .sum()
            .sort_values("forecast_hour")
        )

        st.line_chart(
            hourly_forecast.set_index(
                "forecast_hour"
            )[["predicted_units"]],
            height=350
        )

        st.caption(
            "The mean baseline predicts constant average "
            "hourly demand, so the aggregate forecast "
            "may appear as a flat line."
        )

        st.divider()

        # ----------------------------------------------------
        # TOP FORECASTED PRODUCTS
        # ----------------------------------------------------

        st.subheader("Top Products by Forecast Demand")

        product_totals = (
            operational.groupby(
                ["product_id", "product_name"],
                dropna=False,
                as_index=False
            )["predicted_units"]
            .sum()
        )

        product_totals = product_totals.sort_values(
            "predicted_units",
            ascending=False
        )

        top_products = product_totals.head(15).copy()

        top_products["product_label"] = (
            top_products["product_id"].astype(str)
            + " - "
            + top_products["product_name"].fillna("Unknown")
        )

        st.bar_chart(
            top_products.set_index("product_label")[
                ["predicted_units"]
            ],
            horizontal=True,
            height=450
        )

        st.divider()

        # ----------------------------------------------------
        # INDIVIDUAL PRODUCT FORECAST
        # ----------------------------------------------------

        st.subheader("Individual Product Forecast")

        product_names = (
            operational.drop_duplicates("product_id")
            .set_index("product_id")["product_name"]
            .to_dict()
        )

        product_ids = sorted(
            operational["product_id"].unique().tolist()
        )

        selected_product = st.selectbox(
            "Select operational product",
            product_ids,
            format_func=lambda pid: (
                f"{pid} - "
                f"{product_names.get(pid, 'Unknown')}"
            ),
            key="operational_product_selector"
        )

        selected_data = operational[
            operational["product_id"] == selected_product
        ].sort_values("forecast_hour")

        product_forecast_total = selected_data[
            "predicted_units"
        ].sum()

        st.metric(
            "Selected Product - 24h Predicted Demand",
            f"{product_forecast_total:.2f} units"
        )

        st.line_chart(
            selected_data.set_index("forecast_hour")[
                ["predicted_units"]
            ],
            height=320
        )

        st.dataframe(
            selected_data,
            width="stretch",
            hide_index=True
        )

        # ----------------------------------------------------
        # DOWNLOAD FORECAST
        # ----------------------------------------------------

        st.download_button(
            "Download Operational Forecast CSV",
            data=operational.to_csv(
                index=False
            ).encode("utf-8"),
            file_name="nexvia_operational_forecasts.csv",
            mime="text/csv"
        )

        st.info(
            "HOURLY_MEAN_BASELINE_V1 is a simple average "
            "demand baseline. It has not yet been validated "
            "for long-term forecasting accuracy."
        )

# ============================================================
# TAB 6 â€” RISK-AWARE OPERATIONAL REPLENISHMENT
# ============================================================

with tab6:

    st.header("Risk-Aware Replenishment")

    st.caption(
        "Simulated operational planning scenarios using "
        "supplier delays, warehouse stock availability, "
        "inter-warehouse transfers and emergency procurement."
    )

    actions = data["operational_actions"].copy()
    sync = data["operational_sync"]

    if sync.empty:
        st.error("Inventory/forecast timestamps unavailable.")

    else:
        latest_inventory = pd.to_datetime(
            sync.iloc[0]["latest_inventory_hour"]
        )

        latest_forecast = pd.to_datetime(
            sync.iloc[0]["latest_forecast_hour"]
        )

        if pd.isna(latest_inventory) or pd.isna(latest_forecast):
            st.warning(
                "Operational inventory or forecast data is missing."
            )

        elif latest_inventory != latest_forecast:
            st.warning(
                "Inventory and demand forecasts are not synchronized. "
                "Run the operational forecasting DAG after the inventory DAG."
            )

            st.write("Latest inventory:", latest_inventory)
            st.write("Latest forecast:", latest_forecast)

        elif actions.empty:
            st.success(
                "No replenishment actions are currently projected "
                "under the configured delay scenarios."
            )

        else:
            st.success("Inventory and forecasts are synchronized.")

            st.caption(
                f"Simulation reference hour: {latest_inventory}"
            )

            # ------------------------------------------------
            # SELECT SUPPLIER DELAY SCENARIO
            # ------------------------------------------------

            delay_options = sorted(
                actions["extra_delay_days"]
                .dropna()
                .astype(int)
                .unique()
                .tolist()
            )

            selected_delay = st.selectbox(
                "Additional Supplier Delay",
                delay_options,
                format_func=lambda x: f"+{x} Days",
                key="replenishment_delay_filter"
            )

            selected = actions[
                actions["extra_delay_days"] == selected_delay
            ].copy()

            numeric_columns = [
                "original_shortage",
                "recommended_transfer_units",
                "emergency_procurement_units"
            ]

            for column in numeric_columns:
                selected[column] = pd.to_numeric(
                    selected[column],
                    errors="coerce"
                ).fillna(0)

            # ------------------------------------------------
            # EXECUTIVE METRICS
            # ------------------------------------------------

            st.subheader("Replenishment Risk Overview")

            locations_at_risk = len(selected)

            original_shortage = int(
                selected["original_shortage"].sum()
            )

            transfer_units = int(
                selected["recommended_transfer_units"].sum()
            )

            emergency_units = int(
                selected["emergency_procurement_units"].sum()
            )

            # Two rows ensure complete KPI labels are visible in Chrome.
            row1_col1, row1_col2 = st.columns(2)

            with row1_col1:
                st.metric(
                    "Locations at Risk",
                    f"{locations_at_risk:,}"
                )

            with row1_col2:
                st.metric(
                    "Projected Shortage",
                    f"{original_shortage:,} units"
                )

            row2_col1, row2_col2 = st.columns(2)

            with row2_col1:
                st.metric(
                    "Transferable Stock",
                    f"{transfer_units:,} units"
                )

            with row2_col2:
                st.metric(
                    "Emergency Procurement",
                    f"{emergency_units:,} units"
                )

            st.divider()

            # ------------------------------------------------
            # SHORTAGE MITIGATION COMPARISON
            # ------------------------------------------------

            st.subheader("Shortage Mitigation")

            mitigation = pd.DataFrame({
                "Stage": [
                    "Before Transfers",
                    "After Transfers"
                ],
                "Shortage Units": [
                    original_shortage,
                    emergency_units
                ]
            })

            st.bar_chart(
                mitigation.set_index("Stage"),
                height=300
            )

            st.divider()

            # ------------------------------------------------
            # PRODUCT-LEVEL RISK
            # ------------------------------------------------

            st.subheader("Products Requiring Intervention")

            product_summary = (
                selected.groupby(
                    ["product_id", "product_name"],
                    as_index=False
                )[
                    [
                        "original_shortage",
                        "recommended_transfer_units",
                        "emergency_procurement_units"
                    ]
                ]
                .sum()
            )

            st.dataframe(
                product_summary,
                width="stretch",
                hide_index=True
            )

            st.divider()

            # ------------------------------------------------
            # WAREHOUSE TRANSFER RECOMMENDATIONS
            # ------------------------------------------------

            st.subheader("Warehouse Transfer Plan")

            transfers = selected[
                selected["recommended_transfer_units"] > 0
            ].copy()

            if transfers.empty:
                st.info(
                    "No inter-warehouse transfers recommended."
                )

            else:
                st.dataframe(
                    transfers[
                        [
                            "product_id",
                            "product_name",
                            "destination_warehouse",
                            "transfer_sources",
                            "recommended_transfer_units"
                        ]
                    ],
                    width="stretch",
                    hide_index=True
                )

            st.divider()

            # ------------------------------------------------
            # EMERGENCY PROCUREMENT RECOMMENDATIONS
            # ------------------------------------------------

            st.subheader("Emergency Procurement Requirements")

            emergency = selected[
                selected["emergency_procurement_units"] > 0
            ].copy()

            if emergency.empty:
                st.success(
                    "Potential shortages can be covered "
                    "through available inter-warehouse transfers."
                )

            else:
                st.warning(
                    f"{emergency_units} units may require "
                    "emergency procurement or another intervention."
                )

                st.dataframe(
                    emergency[
                        [
                            "product_id",
                            "product_name",
                            "destination_warehouse",
                            "original_shortage",
                            "recommended_transfer_units",
                            "emergency_procurement_units"
                        ]
                    ],
                    width="stretch",
                    hide_index=True
                )

            st.divider()

            # ------------------------------------------------
            # COMPLETE ACTION TABLE
            # ------------------------------------------------

            st.subheader("Complete Replenishment Action Plan")

            st.dataframe(
                selected,
                width="stretch",
                hide_index=True
            )

            st.download_button(
                "Download Replenishment Recommendations",
                data=selected.to_csv(
                    index=False
                ).encode("utf-8"),
                file_name=(
                    f"nexvia_replenishment_{selected_delay}d.csv"
                ),
                mime="text/csv"
            )

            st.info(
                "These recommendations are based on simulated "
                "supplier-delay scenarios. They do not execute "
                "purchases or stock transfers. Transport timing, "
                "supplier availability and actual procurement "
                "costs require separate validation."
            )




# ============================================================
# 4. FOOTER
# ============================================================

st.divider()

st.caption(
    "NEXVIA Retail & Supply Chain Control Tower | "
    "PostgreSQL â€¢ Python â€¢ Streamlit â€¢ Prophet â€¢ "
    "Croston-SBA â€¢ Anomaly Detection"
)
