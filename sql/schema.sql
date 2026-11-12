--
-- PostgreSQL database dump
--

\restrict bb3k3mNRLD2JulFlzGMEmW3RCCWof8n1ul8lHYWrwmzH3u2E9LmHgiemHrZGTqy

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: analytics; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA analytics;


--
-- Name: dataco; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA dataco;


--
-- Name: ops; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ops;


--
-- Name: advance_simulated_hour(timestamp with time zone); Type: FUNCTION; Schema: ops; Owner: -
--

CREATE FUNCTION ops.advance_simulated_hour(p_run_at timestamp with time zone DEFAULT now()) RETURNS TABLE(simulated_hour timestamp with time zone, inventory_rows integer, units_received bigint, shipments_created integer, already_processed boolean)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_run_hour TIMESTAMPTZ := date_trunc('hour', p_run_at);
    v_first TIMESTAMPTZ;
    v_last TIMESTAMPTZ;
    v_next TIMESTAMPTZ;
    v_hour_index INT;
    v_rows INT;
    v_expected INT;
    v_received BIGINT;
    v_orders INT;
BEGIN
    -- Prevent simultaneous processing
    PERFORM pg_advisory_xact_lock(5231, 2026);

    -- Don't process an execution hour twice
    IF EXISTS (
        SELECT 1
        FROM ops.hourly_simulation_runs r
        WHERE r.run_hour = v_run_hour
    ) THEN
        RETURN QUERY
        SELECT
            r.simulated_hour,
            r.inventory_rows,
            r.received_units,
            r.shipments_created,
            TRUE
        FROM ops.hourly_simulation_runs r
        WHERE r.run_hour = v_run_hour;
        RETURN;
    END IF;

    SELECT MIN(snapshot_hour), MAX(snapshot_hour)
    INTO v_first, v_last
    FROM ops.live_inventory_hourly;

    IF v_last IS NULL THEN
        RAISE EXCEPTION 'Initialize inventory first';
    END IF;

    v_next := v_last + INTERVAL '1 hour';

    v_hour_index := (
        EXTRACT(EPOCH FROM (v_next - v_first)) / 3600
    )::INT;

    SELECT COUNT(*) INTO v_expected
    FROM ops.live_inventory_hourly
    WHERE snapshot_hour = v_last;

    -- Mark due shipments as delivered
    CREATE TEMP TABLE temp_hourly_receipts
    ON COMMIT DROP AS
    WITH delivered AS (
        UPDATE ops.live_supplier_shipments
        SET
            shipment_status = 'DELIVERED',
            actual_arrival_at = v_next
        WHERE shipment_status = 'IN_TRANSIT'
          AND expected_arrival_at <= v_next
        RETURNING
            product_id,
            warehouse_id,
            quantity_ordered
    )
    SELECT
        product_id,
        warehouse_id,
        SUM(quantity_ordered)::INT AS received_units
    FROM delivered
    GROUP BY product_id, warehouse_id;

    SELECT COALESCE(SUM(r.received_units), 0)
    INTO v_received
    FROM temp_hourly_receipts r;

    -- Calculate next hour's inventory
    WITH previous AS (
        SELECT DISTINCT ON (
            product_id, warehouse_id
        )
            product_id,
            warehouse_id,
            closing_stock
        FROM ops.live_inventory_hourly
        ORDER BY
            product_id,
            warehouse_id,
            snapshot_hour DESC
    ),
    generated AS (
        SELECT
            p.product_id,
            p.warehouse_id,
            p.closing_stock AS opening_stock,
            COALESCE(r.received_units, 0) AS receipts,

            GREATEST(
                0,
                (
                    FLOOR(
                        (v_hour_index +
                         MOD(p.product_id + p.warehouse_id, 24))
                        * d.avg_hourly_demand
                    )
                    -
                    FLOOR(
                        (v_hour_index - 1 +
                         MOD(p.product_id + p.warehouse_id, 24))
                        * d.avg_hourly_demand
                    )
                )::INT
            ) AS demand

        FROM previous p
        JOIN ops.v_hourly_demand_profile d
            ON d.product_id = p.product_id
           AND d.warehouse_id = p.warehouse_id
        LEFT JOIN temp_hourly_receipts r
            ON r.product_id = p.product_id
           AND r.warehouse_id = p.warehouse_id
    )

    INSERT INTO ops.live_inventory_hourly (
        product_id,
        warehouse_id,
        snapshot_hour,
        opening_stock,
        received_units,
        demand_units,
        fulfilled_units,
        unmet_units,
        closing_stock,
        data_source
    )
    SELECT
        product_id,
        warehouse_id,
        v_next,
        opening_stock,
        receipts,
        demand,

        LEAST(demand, opening_stock + receipts),

        GREATEST(
            demand - opening_stock - receipts, 0
        ),

        GREATEST(
            opening_stock + receipts - demand, 0
        ),

        'SIMULATED_HOURLY'
    FROM generated
    ON CONFLICT (
        product_id, warehouse_id, snapshot_hour
    ) DO NOTHING;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows <> v_expected THEN
        RAISE EXCEPTION
            'Incomplete inventory update: % of % rows',
            v_rows, v_expected;
    END IF;

    -- Generate new replenishment orders
    INSERT INTO ops.live_supplier_shipments (
        product_id,
        warehouse_id,
        supplier_id,
        ordered_at,
        expected_arrival_at,
        quantity_ordered,
        shipment_status,
        data_source
    )
    SELECT
        i.product_id,
        i.warehouse_id,
        ps.supplier_id,
        v_next,

        v_next +
            s.expected_lead_days * INTERVAL '1 day',

        CEIL(
            d.avg_hourly_demand * 24 *
            (s.expected_lead_days + 14)
        )::INT - i.closing_stock,

        'IN_TRANSIT',
        'SIMULATED_HOURLY'

    FROM ops.live_inventory_hourly i
    JOIN ops.v_hourly_demand_profile d
        ON d.product_id = i.product_id
       AND d.warehouse_id = i.warehouse_id
    JOIN ops.product_suppliers ps
        ON ps.product_id = i.product_id
    JOIN ops.suppliers s
        ON s.supplier_id = ps.supplier_id

    WHERE i.snapshot_hour = v_next
      AND d.avg_hourly_demand > 0
      AND i.closing_stock < CEIL(
          d.avg_hourly_demand * 24 *
          (s.expected_lead_days + 7)
      )
      AND NOT EXISTS (
          SELECT 1
          FROM ops.live_supplier_shipments sh
          WHERE sh.product_id = i.product_id
            AND sh.warehouse_id = i.warehouse_id
            AND sh.shipment_status = 'IN_TRANSIT'
      )

    ON CONFLICT (
        product_id, warehouse_id, ordered_at
    ) DO NOTHING;

    GET DIAGNOSTICS v_orders = ROW_COUNT;

    -- Record the completed simulation run
    INSERT INTO ops.hourly_simulation_runs (
        run_hour,
        simulated_hour,
        inventory_rows,
        received_units,
        shipments_created
    )
    VALUES (
        v_run_hour,
        v_next,
        v_rows,
        v_received,
        v_orders
    );

    RETURN QUERY
    SELECT v_next, v_rows, v_received, v_orders, FALSE;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: demand_forecasts; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.demand_forecasts (
    model_name text NOT NULL,
    forecast_date date NOT NULL,
    predicted_units numeric(12,2) NOT NULL,
    lower_bound numeric(12,2),
    upper_bound numeric(12,2),
    trained_through date NOT NULL,
    data_source text DEFAULT 'DATACO_HISTORICAL'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: demand_spike_anomalies; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.demand_spike_anomalies (
    product_id integer NOT NULL,
    demand_date date NOT NULL,
    demand_units integer NOT NULL,
    baseline_mean numeric(12,3),
    baseline_std numeric(12,3),
    z_score numeric(12,3),
    detection_method text DEFAULT 'ROLLING_ZSCORE'::text NOT NULL,
    data_source text DEFAULT 'DATACO_HISTORICAL'::text NOT NULL,
    detected_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hourly_anomaly_snapshots; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.hourly_anomaly_snapshots (
    snapshot_hour timestamp with time zone NOT NULL,
    historical_demand_spikes bigint NOT NULL,
    simulated_supplier_delays bigint NOT NULL,
    supplier_warnings bigint NOT NULL,
    supplier_critical bigint NOT NULL,
    checked_at timestamp with time zone DEFAULT now() NOT NULL,
    data_source text DEFAULT 'HISTORICAL_DATACO_AND_SIMULATED_SUPPLIERS'::text NOT NULL
);


--
-- Name: operational_demand_forecasts; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.operational_demand_forecasts (
    product_id integer NOT NULL,
    model_name text NOT NULL,
    asof_hour timestamp with time zone NOT NULL,
    forecast_hour timestamp with time zone NOT NULL,
    predicted_units numeric(12,4) NOT NULL,
    history_hours integer NOT NULL,
    data_source text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT operational_demand_forecasts_predicted_units_check CHECK ((predicted_units >= (0)::numeric))
);


--
-- Name: product_demand_forecasts; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.product_demand_forecasts (
    product_id integer NOT NULL,
    model_name text NOT NULL,
    forecast_date date NOT NULL,
    predicted_units numeric(12,4) NOT NULL,
    lower_bound numeric(12,4),
    upper_bound numeric(12,4),
    trained_through date NOT NULL,
    data_source text DEFAULT 'DATACO_HISTORICAL'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_predicted_units_nonnegative CHECK ((predicted_units >= (0)::numeric))
);


--
-- Name: purchase_orders; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.purchase_orders (
    po_id bigint NOT NULL,
    product_id integer NOT NULL,
    supplier_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    ordered_on date NOT NULL,
    expected_on date NOT NULL,
    received_on date,
    quantity_ordered integer NOT NULL,
    order_status character varying(20) NOT NULL,
    data_source character varying(20) DEFAULT 'SIMULATED'::character varying,
    CONSTRAINT purchase_orders_quantity_ordered_check CHECK ((quantity_ordered > 0))
);


--
-- Name: suppliers; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.suppliers (
    supplier_id integer NOT NULL,
    supplier_name character varying(100) NOT NULL,
    expected_lead_days integer NOT NULL,
    data_source character varying(20) DEFAULT 'SIMULATED'::character varying,
    CONSTRAINT suppliers_expected_lead_days_check CHECK ((expected_lead_days > 0))
);


--
-- Name: v_supplier_delay_anomalies; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_supplier_delay_anomalies AS
 WITH deliveries AS (
         SELECT po.po_id,
            po.supplier_id,
            s.supplier_name,
            po.product_id,
            po.ordered_on,
            po.expected_on,
            po.received_on,
            (po.received_on - po.expected_on) AS delay_days
           FROM (ops.purchase_orders po
             JOIN ops.suppliers s ON ((po.supplier_id = s.supplier_id)))
          WHERE (((po.order_status)::text = 'DELIVERED'::text) AND (po.received_on IS NOT NULL))
        ), supplier_stats AS (
         SELECT deliveries.po_id,
            deliveries.supplier_id,
            deliveries.supplier_name,
            deliveries.product_id,
            deliveries.ordered_on,
            deliveries.expected_on,
            deliveries.received_on,
            deliveries.delay_days,
            round(avg((deliveries.delay_days)::numeric) OVER (PARTITION BY deliveries.supplier_id), 2) AS avg_supplier_delay,
            round(stddev_pop((deliveries.delay_days)::numeric) OVER (PARTITION BY deliveries.supplier_id), 2) AS std_supplier_delay,
            round((((deliveries.delay_days)::numeric - avg((deliveries.delay_days)::numeric) OVER (PARTITION BY deliveries.supplier_id)) / NULLIF(stddev_pop((deliveries.delay_days)::numeric) OVER (PARTITION BY deliveries.supplier_id), (0)::numeric)), 3) AS delay_z_score
           FROM deliveries
        )
 SELECT po_id,
    supplier_id,
    supplier_name,
    product_id,
    ordered_on,
    expected_on,
    received_on,
    delay_days,
    avg_supplier_delay,
    std_supplier_delay,
    delay_z_score,
        CASE
            WHEN (delay_days >= 3) THEN 'CRITICAL'::text
            WHEN (delay_days >= 2) THEN 'WARNING'::text
            ELSE NULL::text
        END AS anomaly_severity,
    'SIMULATED'::text AS data_source
   FROM supplier_stats
  WHERE (delay_days >= 2);


--
-- Name: products; Type: TABLE; Schema: dataco; Owner: -
--

CREATE TABLE dataco.products (
    product_id integer NOT NULL,
    product_name text NOT NULL,
    category_id integer,
    category_name text,
    product_price numeric(12,2)
);


--
-- Name: v_control_tower_anomalies; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_control_tower_anomalies AS
 SELECT 'DEMAND_SPIKE'::text AS anomaly_type,
    a.demand_date AS event_date,
    p.product_name AS entity_name,
    'ROLLING_ZSCORE'::text AS detection_method,
    (a.demand_units)::numeric AS observed_value,
    a.baseline_mean AS reference_value,
    a.z_score,
    'NOT_REVIEWED'::text AS severity,
    a.data_source
   FROM (analytics.demand_spike_anomalies a
     JOIN dataco.products p ON ((a.product_id = p.product_id)))
UNION ALL
 SELECT 'SUPPLIER_DELAY'::text AS anomaly_type,
    s.received_on AS event_date,
    s.supplier_name AS entity_name,
    'DELAY_THRESHOLD'::text AS detection_method,
    (s.delay_days)::numeric AS observed_value,
    s.avg_supplier_delay AS reference_value,
    s.delay_z_score AS z_score,
    s.anomaly_severity AS severity,
    s.data_source
   FROM analytics.v_supplier_delay_anomalies s;


--
-- Name: customer_orders; Type: TABLE; Schema: dataco; Owner: -
--

CREATE TABLE dataco.customer_orders (
    order_id bigint NOT NULL,
    customer_id bigint,
    order_at timestamp without time zone,
    shipped_at timestamp without time zone,
    order_status text,
    delivery_status text,
    shipping_mode text,
    actual_shipping_days integer,
    scheduled_shipping_days integer,
    late_delivery_risk integer,
    order_region text
);


--
-- Name: order_items; Type: TABLE; Schema: dataco; Owner: -
--

CREATE TABLE dataco.order_items (
    order_item_id bigint NOT NULL,
    order_id bigint,
    product_id integer,
    quantity integer,
    unit_price numeric(12,2),
    discount numeric(12,2),
    revenue numeric(12,2)
);


--
-- Name: live_inventory_hourly; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.live_inventory_hourly (
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    snapshot_hour timestamp with time zone NOT NULL,
    opening_stock integer NOT NULL,
    received_units integer DEFAULT 0 NOT NULL,
    demand_units integer DEFAULT 0 NOT NULL,
    fulfilled_units integer DEFAULT 0 NOT NULL,
    unmet_units integer DEFAULT 0 NOT NULL,
    closing_stock integer NOT NULL,
    data_source text DEFAULT 'SIMULATED'::text NOT NULL,
    CONSTRAINT live_inventory_hourly_check CHECK ((demand_units = (fulfilled_units + unmet_units))),
    CONSTRAINT live_inventory_hourly_check1 CHECK ((closing_stock = ((opening_stock + received_units) - fulfilled_units))),
    CONSTRAINT live_inventory_hourly_closing_stock_check CHECK ((closing_stock >= 0)),
    CONSTRAINT live_inventory_hourly_demand_units_check CHECK ((demand_units >= 0)),
    CONSTRAINT live_inventory_hourly_fulfilled_units_check CHECK ((fulfilled_units >= 0)),
    CONSTRAINT live_inventory_hourly_opening_stock_check CHECK ((opening_stock >= 0)),
    CONSTRAINT live_inventory_hourly_received_units_check CHECK ((received_units >= 0)),
    CONSTRAINT live_inventory_hourly_unmet_units_check CHECK ((unmet_units >= 0))
);


--
-- Name: live_supplier_shipments; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.live_supplier_shipments (
    shipment_id bigint NOT NULL,
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    supplier_id integer NOT NULL,
    ordered_at timestamp with time zone NOT NULL,
    expected_arrival_at timestamp with time zone NOT NULL,
    actual_arrival_at timestamp with time zone,
    quantity_ordered integer NOT NULL,
    shipment_status character varying(20) NOT NULL,
    data_source text DEFAULT 'SIMULATED'::text NOT NULL,
    CONSTRAINT live_supplier_shipments_check CHECK ((expected_arrival_at > ordered_at)),
    CONSTRAINT live_supplier_shipments_check1 CHECK (((((shipment_status)::text = 'IN_TRANSIT'::text) AND (actual_arrival_at IS NULL)) OR (((shipment_status)::text = 'DELIVERED'::text) AND (actual_arrival_at IS NOT NULL)))),
    CONSTRAINT live_supplier_shipments_quantity_ordered_check CHECK ((quantity_ordered > 0)),
    CONSTRAINT live_supplier_shipments_shipment_status_check CHECK (((shipment_status)::text = ANY ((ARRAY['IN_TRANSIT'::character varying, 'DELIVERED'::character varying])::text[])))
);


--
-- Name: product_suppliers; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.product_suppliers (
    product_id integer NOT NULL,
    supplier_id integer NOT NULL
);


--
-- Name: warehouses; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.warehouses (
    warehouse_id integer NOT NULL,
    warehouse_name character varying(100) NOT NULL,
    city character varying(50) NOT NULL
);


--
-- Name: v_daily_demand; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_daily_demand AS
 WITH warehouse_numbered AS (
         SELECT warehouses.warehouse_id,
            row_number() OVER (ORDER BY warehouses.warehouse_id) AS slot
           FROM ops.warehouses
        ), warehouse_count AS (
         SELECT count(*) AS total
           FROM ops.warehouses
        )
 SELECT (co.order_at)::date AS demand_date,
    oi.product_id,
    w.warehouse_id,
    sum(oi.quantity) AS demand_units
   FROM (((dataco.order_items oi
     JOIN dataco.customer_orders co ON ((oi.order_id = co.order_id)))
     CROSS JOIN warehouse_count wc)
     JOIN warehouse_numbered w ON ((w.slot = (1 + mod(co.order_id, wc.total)))))
  WHERE ((co.order_at >= '2017-11-01 00:00:00'::timestamp without time zone) AND (co.order_at < '2018-02-01 00:00:00'::timestamp without time zone) AND (co.order_status <> ALL (ARRAY['CANCELED'::text, 'SUSPECTED_FRAUD'::text])))
  GROUP BY ((co.order_at)::date), oi.product_id, w.warehouse_id;


--
-- Name: v_hourly_demand_profile; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_hourly_demand_profile AS
 SELECT p.product_id,
    w.warehouse_id,
    round((COALESCE(sum(d.demand_units), (0)::numeric) / (92.0 * (24)::numeric)), 6) AS avg_hourly_demand
   FROM ((dataco.products p
     CROSS JOIN ops.warehouses w)
     LEFT JOIN ops.v_daily_demand d ON (((d.product_id = p.product_id) AND (d.warehouse_id = w.warehouse_id))))
  GROUP BY p.product_id, w.warehouse_id;


--
-- Name: v_live_operational_alerts; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_live_operational_alerts AS
 WITH current_hour AS (
         SELECT max(live_inventory_hourly.snapshot_hour) AS simulated_hour
           FROM ops.live_inventory_hourly
        )
 SELECT i.snapshot_hour AS simulated_hour,
    i.product_id,
    i.warehouse_id,
        CASE
            WHEN (i.unmet_units > 0) THEN 'STOCKOUT'::text
            ELSE 'LOW_STOCK'::text
        END AS alert_type,
        CASE
            WHEN (i.unmet_units > 0) THEN 'CRITICAL'::text
            ELSE 'WARNING'::text
        END AS severity,
        CASE
            WHEN (i.unmet_units > 0) THEN (i.unmet_units)::numeric
            ELSE (i.closing_stock)::numeric
        END AS observed_value,
        CASE
            WHEN (i.unmet_units > 0) THEN (0)::numeric
            ELSE ceil(((d.avg_hourly_demand * (24)::numeric) * ((s.expected_lead_days + 7))::numeric))
        END AS threshold_value,
    'SIMULATED_HOURLY'::text AS data_source
   FROM ((((ops.live_inventory_hourly i
     JOIN current_hour h ON ((i.snapshot_hour = h.simulated_hour)))
     JOIN ops.v_hourly_demand_profile d ON (((i.product_id = d.product_id) AND (i.warehouse_id = d.warehouse_id))))
     JOIN ops.product_suppliers ps ON ((i.product_id = ps.product_id)))
     JOIN ops.suppliers s ON ((ps.supplier_id = s.supplier_id)))
  WHERE ((i.unmet_units > 0) OR ((d.avg_hourly_demand > (0)::numeric) AND ((i.closing_stock)::numeric < ceil(((d.avg_hourly_demand * (24)::numeric) * ((s.expected_lead_days + 7))::numeric)))))
UNION ALL
 SELECT h.simulated_hour,
    sh.product_id,
    sh.warehouse_id,
    'SUPPLIER_OVERDUE'::text AS alert_type,
    'CRITICAL'::text AS severity,
    (EXTRACT(epoch FROM (h.simulated_hour - sh.expected_arrival_at)) / (3600)::numeric) AS observed_value,
    (0)::numeric AS threshold_value,
    'SIMULATED_HOURLY'::text AS data_source
   FROM (ops.live_supplier_shipments sh
     CROSS JOIN current_hour h)
  WHERE (((sh.shipment_status)::text = 'IN_TRANSIT'::text) AND (sh.expected_arrival_at < h.simulated_hour));


--
-- Name: v_operational_replenishment_actions; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_operational_replenishment_actions AS
 WITH latest AS (
         SELECT ( SELECT max(live_inventory_hourly.snapshot_hour) AS max
                   FROM ops.live_inventory_hourly) AS inventory_hour,
            ( SELECT max(operational_demand_forecasts.asof_hour) AS max
                   FROM analytics.operational_demand_forecasts
                  WHERE ((operational_demand_forecasts.model_name = 'HOURLY_MEAN_BASELINE_V1'::text) AND (operational_demand_forecasts.data_source = 'SIMULATED_HOURLY'::text))) AS forecast_hour
        ), active_products AS (
         SELECT operational_demand_forecasts.product_id
           FROM analytics.operational_demand_forecasts
          WHERE ((operational_demand_forecasts.model_name = 'HOURLY_MEAN_BASELINE_V1'::text) AND (operational_demand_forecasts.data_source = 'SIMULATED_HOURLY'::text) AND (operational_demand_forecasts.asof_hour = ( SELECT latest.forecast_hour
                   FROM latest)))
          GROUP BY operational_demand_forecasts.product_id
         HAVING (sum(operational_demand_forecasts.predicted_units) > (0)::numeric)
        ), warehouse_history AS (
         SELECT live_inventory_hourly.product_id,
            live_inventory_hourly.warehouse_id,
            (sum(live_inventory_hourly.demand_units))::numeric AS total_demand,
            count(DISTINCT live_inventory_hourly.snapshot_hour) AS history_hours
           FROM ops.live_inventory_hourly
          WHERE (live_inventory_hourly.snapshot_hour <= ( SELECT latest.inventory_hour
                   FROM latest))
          GROUP BY live_inventory_hourly.product_id, live_inventory_hourly.warehouse_id
        ), supplier_lead AS (
         SELECT ps.product_id,
            min(s.expected_lead_days) AS lead_days
           FROM (ops.product_suppliers ps
             JOIN ops.suppliers s ON ((ps.supplier_id = s.supplier_id)))
          GROUP BY ps.product_id
        ), requirements AS (
         SELECT l.inventory_hour AS asof_hour,
            d.extra_delay_days,
            i.product_id,
            p.product_name,
            i.warehouse_id,
            i.closing_stock AS available_stock,
            (ceil((((h.total_demand * 24.0) / (NULLIF(h.history_hours, 0))::numeric) * ((s.lead_days + d.extra_delay_days))::numeric)))::integer AS required_stock
           FROM ((((((ops.live_inventory_hourly i
             JOIN active_products a ON ((a.product_id = i.product_id)))
             JOIN dataco.products p ON ((p.product_id = i.product_id)))
             JOIN warehouse_history h ON (((h.product_id = i.product_id) AND (h.warehouse_id = i.warehouse_id))))
             JOIN supplier_lead s ON ((s.product_id = i.product_id)))
             CROSS JOIN ( VALUES (14), (30)) d(extra_delay_days))
             CROSS JOIN latest l)
          WHERE ((i.snapshot_hour = l.inventory_hour) AND (l.inventory_hour = l.forecast_hour))
        ), gaps AS (
         SELECT requirements.asof_hour,
            requirements.extra_delay_days,
            requirements.product_id,
            requirements.product_name,
            requirements.warehouse_id,
            requirements.available_stock,
            requirements.required_stock,
            GREATEST((requirements.required_stock - requirements.available_stock), 0) AS shortage_units,
            GREATEST((requirements.available_stock - requirements.required_stock), 0) AS surplus_units
           FROM requirements
        ), receivers AS (
         SELECT gaps.asof_hour,
            gaps.extra_delay_days,
            gaps.product_id,
            gaps.product_name,
            gaps.warehouse_id,
            gaps.available_stock,
            gaps.required_stock,
            gaps.shortage_units,
            gaps.surplus_units,
            sum(gaps.shortage_units) OVER (PARTITION BY gaps.extra_delay_days, gaps.product_id ORDER BY gaps.warehouse_id) AS cumulative_need
           FROM gaps
          WHERE (gaps.shortage_units > 0)
        ), donors AS (
         SELECT gaps.asof_hour,
            gaps.extra_delay_days,
            gaps.product_id,
            gaps.product_name,
            gaps.warehouse_id,
            gaps.available_stock,
            gaps.required_stock,
            gaps.shortage_units,
            gaps.surplus_units,
            sum(gaps.surplus_units) OVER (PARTITION BY gaps.extra_delay_days, gaps.product_id ORDER BY gaps.warehouse_id) AS cumulative_supply
           FROM gaps
          WHERE (gaps.surplus_units > 0)
        ), allocations AS (
         SELECT r_1.extra_delay_days,
            r_1.product_id,
            r_1.warehouse_id AS destination_warehouse,
            d.warehouse_id AS source_warehouse,
            (GREATEST((0)::bigint, (LEAST(r_1.cumulative_need, d.cumulative_supply) - GREATEST((r_1.cumulative_need - r_1.shortage_units), (d.cumulative_supply - d.surplus_units)))))::integer AS transfer_units
           FROM (receivers r_1
             JOIN donors d ON (((d.extra_delay_days = r_1.extra_delay_days) AND (d.product_id = r_1.product_id) AND (d.warehouse_id <> r_1.warehouse_id))))
        ), transfer_summary AS (
         SELECT allocations.extra_delay_days,
            allocations.product_id,
            allocations.destination_warehouse,
            (sum(allocations.transfer_units))::integer AS transferred_units,
            string_agg((((('W'::text || allocations.source_warehouse) || ': '::text) || allocations.transfer_units) || ' units'::text), ', '::text ORDER BY allocations.source_warehouse) FILTER (WHERE (allocations.transfer_units > 0)) AS transfer_sources
           FROM allocations
          GROUP BY allocations.extra_delay_days, allocations.product_id, allocations.destination_warehouse
        )
 SELECT r.asof_hour,
    r.extra_delay_days,
    r.product_id,
    r.product_name,
    r.warehouse_id AS destination_warehouse,
    r.available_stock,
    r.required_stock,
    r.shortage_units AS original_shortage,
    COALESCE(t.transferred_units, 0) AS recommended_transfer_units,
    COALESCE(t.transfer_sources, 'NONE'::text) AS transfer_sources,
    GREATEST((r.shortage_units - COALESCE(t.transferred_units, 0)), 0) AS emergency_procurement_units,
        CASE
            WHEN ((COALESCE(t.transferred_units, 0) > 0) AND (r.shortage_units > COALESCE(t.transferred_units, 0))) THEN 'TRANSFER + EMERGENCY PROCUREMENT'::text
            WHEN (COALESCE(t.transferred_units, 0) > 0) THEN 'INTER-WAREHOUSE TRANSFER'::text
            ELSE 'EMERGENCY PROCUREMENT'::text
        END AS recommended_action
   FROM (receivers r
     LEFT JOIN transfer_summary t ON (((t.extra_delay_days = r.extra_delay_days) AND (t.product_id = r.product_id) AND (t.destination_warehouse = r.warehouse_id))));


--
-- Name: v_product_daily_demand_historical; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_product_daily_demand_historical AS
 SELECT oi.product_id,
    p.product_name,
    (c.order_at)::date AS demand_date,
    (sum(oi.quantity))::integer AS demand_units,
    'DATACO_HISTORICAL'::text AS data_source
   FROM ((dataco.customer_orders c
     JOIN dataco.order_items oi ON ((c.order_id = oi.order_id)))
     JOIN dataco.products p ON ((oi.product_id = p.product_id)))
  GROUP BY oi.product_id, p.product_name, ((c.order_at)::date);


--
-- Name: v_product_daily_demand_filled; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_product_daily_demand_filled AS
 WITH product_dates AS (
         SELECT v_product_daily_demand_historical.product_id,
            v_product_daily_demand_historical.product_name,
            min(v_product_daily_demand_historical.demand_date) AS first_date,
            max(v_product_daily_demand_historical.demand_date) AS last_date
           FROM analytics.v_product_daily_demand_historical
          GROUP BY v_product_daily_demand_historical.product_id, v_product_daily_demand_historical.product_name
        ), date_spine AS (
         SELECT p.product_id,
            p.product_name,
            (d.d)::date AS demand_date
           FROM (product_dates p
             CROSS JOIN LATERAL generate_series((p.first_date)::timestamp with time zone, (p.last_date)::timestamp with time zone, '1 day'::interval) d(d))
        )
 SELECT s.product_id,
    s.product_name,
    s.demand_date,
    COALESCE(h.demand_units, 0) AS demand_units,
    'DATACO_HISTORICAL'::text AS data_source
   FROM (date_spine s
     LEFT JOIN analytics.v_product_daily_demand_historical h ON (((s.product_id = h.product_id) AND (s.demand_date = h.demand_date))));


--
-- Name: inventory_snapshots; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.inventory_snapshots (
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    snapshot_at timestamp without time zone NOT NULL,
    stock_on_hand integer NOT NULL,
    reserved_qty integer DEFAULT 0 NOT NULL,
    data_source character varying(20) DEFAULT 'SIMULATED'::character varying,
    CONSTRAINT inventory_snapshots_check CHECK ((reserved_qty <= stock_on_hand)),
    CONSTRAINT inventory_snapshots_reserved_qty_check CHECK ((reserved_qty >= 0)),
    CONSTRAINT inventory_snapshots_stock_on_hand_check CHECK ((stock_on_hand >= 0))
);


--
-- Name: v_product_stockout_risk_30d; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_product_stockout_risk_30d AS
 WITH latest_training AS (
         SELECT max(product_demand_forecasts.trained_through) AS asof_date
           FROM analytics.product_demand_forecasts
          WHERE ((product_demand_forecasts.data_source = 'DATACO_HISTORICAL'::text) AND (product_demand_forecasts.model_name = ANY (ARRAY['CROSTON_SBA_V1'::text, 'ZERO_BASELINE_V1'::text])))
        ), ranked_inventory AS (
         SELECT i.product_id,
            i.warehouse_id,
            i.stock_on_hand,
            i.reserved_qty,
            row_number() OVER (PARTITION BY i.product_id, i.warehouse_id ORDER BY i.snapshot_at DESC) AS rn
           FROM (ops.inventory_snapshots i
             CROSS JOIN latest_training lt)
          WHERE (i.snapshot_at < (lt.asof_date + '1 day'::interval))
        ), product_stock AS (
         SELECT ranked_inventory.product_id,
            sum(GREATEST((COALESCE(ranked_inventory.stock_on_hand, 0) - COALESCE(ranked_inventory.reserved_qty, 0)), 0)) AS available_stock
           FROM ranked_inventory
          WHERE (ranked_inventory.rn = 1)
          GROUP BY ranked_inventory.product_id
        ), product_forecast AS (
         SELECT f_1.product_id,
            min(f_1.trained_through) AS trained_through,
            count(DISTINCT f_1.forecast_date) AS forecast_days,
            sum(f_1.predicted_units) AS forecast_30d
           FROM (analytics.product_demand_forecasts f_1
             JOIN latest_training lt ON ((f_1.trained_through = lt.asof_date)))
          WHERE ((f_1.data_source = 'DATACO_HISTORICAL'::text) AND (f_1.model_name = ANY (ARRAY['CROSTON_SBA_V1'::text, 'ZERO_BASELINE_V1'::text])) AND (f_1.forecast_date > f_1.trained_through) AND (f_1.forecast_date <= (f_1.trained_through + 30)))
          GROUP BY f_1.product_id
         HAVING ((count(DISTINCT f_1.forecast_date) = 30) AND (count(*) = 30))
        )
 SELECT p.product_id,
    p.product_name,
    f.trained_through,
    COALESCE(s.available_stock, (0)::bigint) AS available_stock,
    round(f.forecast_30d, 2) AS forecast_30d,
    round(GREATEST((f.forecast_30d - (COALESCE(s.available_stock, (0)::bigint))::numeric), (0)::numeric), 2) AS projected_shortage,
        CASE
            WHEN (f.forecast_30d = (0)::numeric) THEN 'NO FORECAST DEMAND'::text
            WHEN (f.forecast_30d > (COALESCE(s.available_stock, (0)::bigint))::numeric) THEN 'AT RISK'::text
            ELSE 'COVERED'::text
        END AS stockout_risk
   FROM ((product_forecast f
     JOIN dataco.products p ON ((p.product_id = f.product_id)))
     LEFT JOIN product_stock s ON ((s.product_id = f.product_id)));


--
-- Name: v_supplier_leadtime_kpi; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_supplier_leadtime_kpi AS
 SELECT s.supplier_id,
    s.supplier_name,
    s.expected_lead_days,
    count(po.po_id) AS total_deliveries,
    round(avg((po.received_on - po.ordered_on)), 2) AS avg_actual_lead_days,
    round(avg((po.received_on - po.expected_on)), 2) AS avg_delay_days,
    round(((100.0 * (count(*) FILTER (WHERE (po.received_on <= po.expected_on)))::numeric) / (NULLIF(count(*), 0))::numeric), 2) AS on_time_percent
   FROM (ops.purchase_orders po
     JOIN ops.suppliers s ON ((po.supplier_id = s.supplier_id)))
  WHERE (((po.order_status)::text = 'DELIVERED'::text) AND (po.received_on IS NOT NULL))
  GROUP BY s.supplier_id, s.supplier_name, s.expected_lead_days;


--
-- Name: v_replenishment_policy_comparison_30d; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_replenishment_policy_comparison_30d AS
 WITH base AS (
         SELECT r.product_id,
            r.product_name,
            (r.available_stock)::numeric AS opening_stock,
            r.forecast_30d,
            (ceil(GREATEST((s.expected_lead_days)::numeric, COALESCE(k.avg_actual_lead_days, (s.expected_lead_days)::numeric))))::integer AS lead_days
           FROM (((analytics.v_product_stockout_risk_30d r
             JOIN ops.product_suppliers ps ON ((ps.product_id = r.product_id)))
             JOIN ops.suppliers s ON ((s.supplier_id = ps.supplier_id)))
             LEFT JOIN ops.v_supplier_leadtime_kpi k ON ((k.supplier_id = ps.supplier_id)))
          WHERE (r.stockout_risk = 'AT RISK'::text)
        ), planning AS (
         SELECT base.product_id,
            base.product_name,
            base.opening_stock,
            base.forecast_30d,
            base.lead_days,
            (base.forecast_30d / 30.0) AS daily_demand,
            ceil(((base.forecast_30d / 30.0) * (2)::numeric)) AS safety_stock
           FROM base
        ), orders AS (
         SELECT planning.product_id,
            planning.product_name,
            planning.opening_stock,
            planning.forecast_30d,
            planning.lead_days,
            planning.daily_demand,
            planning.safety_stock,
            (ceil(GREATEST(((planning.forecast_30d + planning.safety_stock) - planning.opening_stock), (0)::numeric)))::integer AS total_order_qty,
            (ceil(GREATEST(((planning.daily_demand * (planning.lead_days)::numeric) - planning.opening_stock), (0)::numeric)))::integer AS emergency_qty
           FROM planning
        ), split_orders AS (
         SELECT orders.product_id,
            orders.product_name,
            orders.opening_stock,
            orders.forecast_30d,
            orders.lead_days,
            orders.daily_demand,
            orders.safety_stock,
            orders.total_order_qty,
            orders.emergency_qty,
            GREATEST((orders.total_order_qty - orders.emergency_qty), 0) AS regular_qty
           FROM orders
        ), simulation AS (
         SELECT split_orders.product_id,
            split_orders.product_name,
            split_orders.opening_stock,
            split_orders.forecast_30d,
            split_orders.lead_days,
            split_orders.daily_demand,
            split_orders.safety_stock,
            split_orders.total_order_qty,
            split_orders.emergency_qty,
            split_orders.regular_qty,
            GREATEST((split_orders.forecast_30d - split_orders.opening_stock), (0)::numeric) AS baseline_shortage,
            GREATEST(((split_orders.daily_demand * (split_orders.lead_days)::numeric) - split_orders.opening_stock), (0)::numeric) AS normal_shortage_before_arrival,
            GREATEST((split_orders.opening_stock - (split_orders.daily_demand * (split_orders.lead_days)::numeric)), (0)::numeric) AS normal_stock_at_arrival,
            GREATEST((((split_orders.daily_demand * (split_orders.lead_days)::numeric) - split_orders.opening_stock) - (split_orders.emergency_qty)::numeric), (0)::numeric) AS emergency_shortage_before_arrival,
            GREATEST(((split_orders.opening_stock + (split_orders.emergency_qty)::numeric) - (split_orders.daily_demand * (split_orders.lead_days)::numeric)), (0)::numeric) AS emergency_stock_at_arrival,
            (split_orders.daily_demand * ((30 - split_orders.lead_days))::numeric) AS demand_after_arrival
           FROM split_orders
        )
 SELECT product_id,
    product_name,
    opening_stock,
    forecast_30d,
    lead_days,
    total_order_qty,
    emergency_qty,
    regular_qty,
    round(baseline_shortage, 2) AS baseline_shortage,
    round((normal_shortage_before_arrival + GREATEST(((demand_after_arrival - normal_stock_at_arrival) - (total_order_qty)::numeric), (0)::numeric)), 2) AS normal_replenishment_shortage,
    round((emergency_shortage_before_arrival + GREATEST(((demand_after_arrival - emergency_stock_at_arrival) - (regular_qty)::numeric), (0)::numeric)), 2) AS emergency_replenishment_shortage
   FROM simulation;


--
-- Name: v_replenishment_policy_kpis_30d; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.v_replenishment_policy_kpis_30d AS
 WITH risk_summary AS (
         SELECT count(*) AS total_products,
            count(*) FILTER (WHERE (v_product_stockout_risk_30d.stockout_risk = 'AT RISK'::text)) AS at_risk_products,
            count(*) FILTER (WHERE (v_product_stockout_risk_30d.stockout_risk = 'COVERED'::text)) AS covered_products,
            count(*) FILTER (WHERE (v_product_stockout_risk_30d.stockout_risk = 'NO FORECAST DEMAND'::text)) AS no_forecast_demand_products
           FROM analytics.v_product_stockout_risk_30d
        ), policy_summary AS (
         SELECT count(*) AS products_evaluated,
            sum(v_replenishment_policy_comparison_30d.total_order_qty) AS total_order_units,
            sum(v_replenishment_policy_comparison_30d.emergency_qty) AS emergency_units,
            sum(v_replenishment_policy_comparison_30d.regular_qty) AS regular_units,
            round(sum(v_replenishment_policy_comparison_30d.baseline_shortage), 2) AS baseline_shortage,
            round(sum(v_replenishment_policy_comparison_30d.normal_replenishment_shortage), 2) AS normal_policy_shortage,
            round(sum(v_replenishment_policy_comparison_30d.emergency_replenishment_shortage), 2) AS emergency_policy_shortage
           FROM analytics.v_replenishment_policy_comparison_30d
        )
 SELECT r.total_products,
    r.at_risk_products,
    r.covered_products,
    r.no_forecast_demand_products,
    p.products_evaluated,
    p.total_order_units,
    p.emergency_units,
    p.regular_units,
    p.baseline_shortage,
    p.normal_policy_shortage,
    p.emergency_policy_shortage,
    round(((100.0 * (p.baseline_shortage - p.normal_policy_shortage)) / NULLIF(p.baseline_shortage, (0)::numeric)), 2) AS normal_reduction_percent,
    round(((100.0 * (p.baseline_shortage - p.emergency_policy_shortage)) / NULLIF(p.baseline_shortage, (0)::numeric)), 2) AS emergency_reduction_percent
   FROM (risk_summary r
     CROSS JOIN policy_summary p);


--
-- Name: hourly_kpi_snapshots; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.hourly_kpi_snapshots (
    snapshot_hour timestamp with time zone NOT NULL,
    stockout_rate numeric(6,2),
    customer_shipping_pct numeric(6,2),
    supplier_on_time_pct numeric(6,2),
    avg_supplier_lead_days numeric(8,2),
    days_of_inventory numeric(8,2),
    inventory_as_of date,
    refreshed_at timestamp with time zone DEFAULT now() NOT NULL,
    source_note text NOT NULL
);


--
-- Name: hourly_operational_kpi_snapshots; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.hourly_operational_kpi_snapshots (
    simulated_hour timestamp with time zone NOT NULL,
    total_demand bigint NOT NULL,
    fulfilled_units bigint NOT NULL,
    unmet_units bigint NOT NULL,
    closing_stock bigint NOT NULL,
    stockout_rate_percent numeric(6,2),
    fulfillment_rate_percent numeric(6,2),
    projected_days_of_inventory numeric(12,2),
    captured_at timestamp with time zone DEFAULT now() NOT NULL,
    data_source text DEFAULT 'SIMULATED_HOURLY'::text NOT NULL
);


--
-- Name: hourly_simulation_runs; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.hourly_simulation_runs (
    run_hour timestamp with time zone NOT NULL,
    simulated_hour timestamp with time zone NOT NULL,
    inventory_rows integer NOT NULL,
    received_units bigint NOT NULL,
    shipments_created integer NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: inventory_ledger_daily; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.inventory_ledger_daily (
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    stock_date date NOT NULL,
    opening_stock integer NOT NULL,
    received_units integer NOT NULL,
    demand_units integer NOT NULL,
    fulfilled_units integer NOT NULL,
    unmet_units integer NOT NULL,
    closing_stock integer NOT NULL,
    data_source text DEFAULT 'SIMULATED'::text NOT NULL,
    CONSTRAINT inventory_ledger_daily_check CHECK ((demand_units = (fulfilled_units + unmet_units))),
    CONSTRAINT inventory_ledger_daily_check1 CHECK ((closing_stock = ((opening_stock + received_units) - fulfilled_units))),
    CONSTRAINT inventory_ledger_daily_closing_stock_check CHECK ((closing_stock >= 0)),
    CONSTRAINT inventory_ledger_daily_demand_units_check CHECK ((demand_units >= 0)),
    CONSTRAINT inventory_ledger_daily_fulfilled_units_check CHECK ((fulfilled_units >= 0)),
    CONSTRAINT inventory_ledger_daily_opening_stock_check CHECK ((opening_stock >= 0)),
    CONSTRAINT inventory_ledger_daily_received_units_check CHECK ((received_units >= 0)),
    CONSTRAINT inventory_ledger_daily_unmet_units_check CHECK ((unmet_units >= 0))
);


--
-- Name: inventory_ledger_reorder_scenario; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.inventory_ledger_reorder_scenario (
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    stock_date date NOT NULL,
    opening_stock integer NOT NULL,
    baseline_receipts integer NOT NULL,
    policy_receipts integer NOT NULL,
    demand_units integer NOT NULL,
    fulfilled_units integer NOT NULL,
    unmet_units integer NOT NULL,
    closing_stock integer NOT NULL,
    order_placed_units integer NOT NULL,
    pending_order_units integer NOT NULL,
    pending_arrival_date date,
    CONSTRAINT inventory_ledger_reorder_scenario_check CHECK ((demand_units = (fulfilled_units + unmet_units))),
    CONSTRAINT inventory_ledger_reorder_scenario_check1 CHECK ((closing_stock = (((opening_stock + baseline_receipts) + policy_receipts) - fulfilled_units))),
    CONSTRAINT inventory_ledger_reorder_scenario_closing_stock_check CHECK ((closing_stock >= 0)),
    CONSTRAINT inventory_ledger_reorder_scenario_opening_stock_check CHECK ((opening_stock >= 0))
);


--
-- Name: live_supplier_shipments_shipment_id_seq; Type: SEQUENCE; Schema: ops; Owner: -
--

ALTER TABLE ops.live_supplier_shipments ALTER COLUMN shipment_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME ops.live_supplier_shipments_shipment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: mv_replenishment_inputs; Type: MATERIALIZED VIEW; Schema: ops; Owner: -
--

CREATE MATERIALIZED VIEW ops.mv_replenishment_inputs AS
 SELECT i.product_id,
    i.warehouse_id,
    i.stock_date,
    i.demand_units,
    i.received_units AS baseline_receipts,
    s.expected_lead_days,
    COALESCE(avg(i.demand_units) OVER (PARTITION BY i.product_id, i.warehouse_id ORDER BY i.stock_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), (0)::numeric) AS prior_7day_avg_demand
   FROM ((ops.inventory_ledger_daily i
     JOIN ops.product_suppliers ps ON ((i.product_id = ps.product_id)))
     JOIN ops.suppliers s ON ((ps.supplier_id = s.supplier_id)))
  WITH NO DATA;


--
-- Name: purchase_orders_po_id_seq; Type: SEQUENCE; Schema: ops; Owner: -
--

ALTER TABLE ops.purchase_orders ALTER COLUMN po_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME ops.purchase_orders_po_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: suppliers_supplier_id_seq; Type: SEQUENCE; Schema: ops; Owner: -
--

ALTER TABLE ops.suppliers ALTER COLUMN supplier_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME ops.suppliers_supplier_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: v_daily_receipts; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_daily_receipts AS
 SELECT received_on AS receipt_date,
    product_id,
    warehouse_id,
    sum(quantity_ordered) AS received_units,
    count(*) AS total_purchase_orders
   FROM ops.purchase_orders
  WHERE (((order_status)::text = 'DELIVERED'::text) AND (received_on IS NOT NULL) AND (received_on >= '2017-11-01'::date) AND (received_on < '2018-02-01'::date))
  GROUP BY received_on, product_id, warehouse_id;


--
-- Name: v_daily_stockout_kpi; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_daily_stockout_kpi AS
 SELECT stock_date,
    count(*) FILTER (WHERE (demand_units > 0)) AS active_sku_warehouse_days,
    count(*) FILTER (WHERE ((demand_units > 0) AND (unmet_units > 0))) AS stockout_events,
    sum(demand_units) AS total_demand,
    sum(unmet_units) AS total_unmet_demand,
    round(((100.0 * (count(*) FILTER (WHERE ((demand_units > 0) AND (unmet_units > 0))))::numeric) / (NULLIF(count(*) FILTER (WHERE (demand_units > 0)), 0))::numeric), 2) AS stockout_rate_percent
   FROM ops.inventory_ledger_daily
  GROUP BY stock_date;


--
-- Name: v_days_of_inventory_kpi; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_days_of_inventory_kpi AS
 WITH latest_date AS (
         SELECT max(inventory_ledger_daily.stock_date) AS as_of_date
           FROM ops.inventory_ledger_daily
        ), recent_demand AS (
         SELECT i_1.product_id,
            i_1.warehouse_id,
            sum(i_1.demand_units) AS demand_30d
           FROM (ops.inventory_ledger_daily i_1
             CROSS JOIN latest_date ld_1)
          WHERE ((i_1.stock_date >= (ld_1.as_of_date - 29)) AND (i_1.stock_date <= ld_1.as_of_date))
          GROUP BY i_1.product_id, i_1.warehouse_id
        )
 SELECT i.product_id,
    p.product_name,
    i.warehouse_id,
    w.warehouse_name,
    i.stock_date AS as_of_date,
    i.closing_stock,
    COALESCE(d.demand_30d, (0)::bigint) AS demand_30d,
    round(((COALESCE(d.demand_30d, (0)::bigint))::numeric / 30.0), 2) AS avg_daily_demand,
    round((((i.closing_stock)::numeric * 30.0) / (NULLIF(d.demand_30d, 0))::numeric), 2) AS days_of_inventory
   FROM ((((ops.inventory_ledger_daily i
     CROSS JOIN latest_date ld)
     JOIN dataco.products p ON ((i.product_id = p.product_id)))
     JOIN ops.warehouses w ON ((i.warehouse_id = w.warehouse_id)))
     LEFT JOIN recent_demand d ON (((i.product_id = d.product_id) AND (i.warehouse_id = d.warehouse_id))))
  WHERE (i.stock_date = ld.as_of_date);


--
-- Name: v_inventory_scenario_comparison; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_inventory_scenario_comparison AS
 WITH scenarios AS (
         SELECT 'Baseline'::text AS scenario,
            inventory_ledger_daily.demand_units,
            inventory_ledger_daily.fulfilled_units,
            inventory_ledger_daily.unmet_units,
            (0)::bigint AS additional_receipts
           FROM ops.inventory_ledger_daily
        UNION ALL
         SELECT 'Demand-Aware Replenishment'::text,
            inventory_ledger_reorder_scenario.demand_units,
            inventory_ledger_reorder_scenario.fulfilled_units,
            inventory_ledger_reorder_scenario.unmet_units,
            inventory_ledger_reorder_scenario.policy_receipts
           FROM ops.inventory_ledger_reorder_scenario
        )
 SELECT scenario,
    sum(demand_units) AS total_demand,
    sum(fulfilled_units) AS fulfilled_units,
    sum(unmet_units) AS unmet_units,
    sum(additional_receipts) AS additional_receipts,
    round(((100.0 * (count(*) FILTER (WHERE ((demand_units > 0) AND (unmet_units > 0))))::numeric) / (NULLIF(count(*) FILTER (WHERE (demand_units > 0)), 0))::numeric), 2) AS stockout_rate
   FROM scenarios
  GROUP BY scenario;


--
-- Name: v_kpi_overview; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_kpi_overview AS
 WITH stockout AS (
         SELECT round(((100.0 * (count(*) FILTER (WHERE ((inventory_ledger_daily.demand_units > 0) AND (inventory_ledger_daily.unmet_units > 0))))::numeric) / (NULLIF(count(*) FILTER (WHERE (inventory_ledger_daily.demand_units > 0)), 0))::numeric), 2) AS stockout_rate
           FROM ops.inventory_ledger_daily
        ), shipping AS (
         SELECT round(((100.0 * (count(*) FILTER (WHERE (customer_orders.delivery_status = ANY (ARRAY['Advance shipping'::text, 'Shipping on time'::text]))))::numeric) / (NULLIF(count(*) FILTER (WHERE (customer_orders.delivery_status <> 'Shipping canceled'::text)), 0))::numeric), 2) AS customer_shipping_pct
           FROM dataco.customer_orders
        ), supplier AS (
         SELECT round(avg((purchase_orders.received_on - purchase_orders.ordered_on)), 2) AS avg_supplier_lead_days,
            round(((100.0 * (count(*) FILTER (WHERE (purchase_orders.received_on <= purchase_orders.expected_on)))::numeric) / (NULLIF(count(*), 0))::numeric), 2) AS supplier_on_time_pct
           FROM ops.purchase_orders
          WHERE (((purchase_orders.order_status)::text = 'DELIVERED'::text) AND (purchase_orders.received_on IS NOT NULL))
        ), inventory AS (
         SELECT max(v_days_of_inventory_kpi.as_of_date) AS inventory_as_of,
            round(((30.0 * (sum(v_days_of_inventory_kpi.closing_stock) FILTER (WHERE (v_days_of_inventory_kpi.demand_30d > 0)))::numeric) / NULLIF(sum(v_days_of_inventory_kpi.demand_30d), (0)::numeric)), 2) AS days_of_inventory
           FROM ops.v_days_of_inventory_kpi
        )
 SELECT s.stockout_rate,
    sh.customer_shipping_pct,
    sp.supplier_on_time_pct,
    sp.avg_supplier_lead_days,
    i.days_of_inventory,
    i.inventory_as_of
   FROM (((stockout s
     CROSS JOIN shipping sh)
     CROSS JOIN supplier sp)
     CROSS JOIN inventory i);


--
-- Name: v_live_hourly_kpis; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_live_hourly_kpis AS
 WITH inventory_data AS (
         SELECT i.snapshot_hour,
            i.product_id,
            i.warehouse_id,
            i.opening_stock,
            i.received_units,
            i.demand_units,
            i.fulfilled_units,
            i.unmet_units,
            i.closing_stock,
            d.avg_hourly_demand
           FROM (ops.live_inventory_hourly i
             JOIN ops.v_hourly_demand_profile d ON (((i.product_id = d.product_id) AND (i.warehouse_id = d.warehouse_id))))
        )
 SELECT snapshot_hour,
    count(*) AS total_product_warehouse_pairs,
    sum(opening_stock) AS opening_stock,
    sum(received_units) AS received_units,
    sum(demand_units) AS total_demand,
    sum(fulfilled_units) AS fulfilled_units,
    sum(unmet_units) AS unmet_units,
    sum(closing_stock) AS closing_stock,
    count(*) FILTER (WHERE (demand_units > 0)) AS active_pairs,
    count(*) FILTER (WHERE ((demand_units > 0) AND (unmet_units > 0))) AS stockout_events,
    round(((100.0 * (count(*) FILTER (WHERE ((demand_units > 0) AND (unmet_units > 0))))::numeric) / (NULLIF(count(*) FILTER (WHERE (demand_units > 0)), 0))::numeric), 2) AS stockout_rate_percent,
    round(((100.0 * (sum(fulfilled_units))::numeric) / (NULLIF(sum(demand_units), 0))::numeric), 2) AS fulfillment_rate_percent,
    round(((sum(closing_stock) FILTER (WHERE (avg_hourly_demand > (0)::numeric)))::numeric / NULLIF((sum(avg_hourly_demand) FILTER (WHERE (avg_hourly_demand > (0)::numeric)) * (24)::numeric), (0)::numeric)), 2) AS projected_days_of_inventory
   FROM inventory_data
  GROUP BY snapshot_hour;


--
-- Name: v_replenishment_recommendations; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.v_replenishment_recommendations AS
 WITH inventory_data AS (
         SELECT d.product_id,
            d.product_name,
            d.warehouse_id,
            d.warehouse_name,
            d.as_of_date,
            d.closing_stock,
            d.demand_30d,
            ((d.demand_30d)::numeric / 30.0) AS avg_daily_demand,
            s.supplier_name,
            s.expected_lead_days
           FROM ((ops.v_days_of_inventory_kpi d
             JOIN ops.product_suppliers ps ON ((d.product_id = ps.product_id)))
             JOIN ops.suppliers s ON ((ps.supplier_id = s.supplier_id)))
        ), stock_targets AS (
         SELECT inventory_data.product_id,
            inventory_data.product_name,
            inventory_data.warehouse_id,
            inventory_data.warehouse_name,
            inventory_data.as_of_date,
            inventory_data.closing_stock,
            inventory_data.demand_30d,
            inventory_data.avg_daily_demand,
            inventory_data.supplier_name,
            inventory_data.expected_lead_days,
            (ceil((inventory_data.avg_daily_demand * ((inventory_data.expected_lead_days + 7))::numeric)))::integer AS reorder_point,
            (ceil((inventory_data.avg_daily_demand * ((inventory_data.expected_lead_days + 14))::numeric)))::integer AS target_stock
           FROM inventory_data
        )
 SELECT product_id,
    product_name,
    warehouse_id,
    warehouse_name,
    as_of_date,
    supplier_name,
    expected_lead_days,
    closing_stock,
    demand_30d,
    round(avg_daily_demand, 2) AS avg_daily_demand,
    reorder_point,
    target_stock,
        CASE
            WHEN (demand_30d = 0) THEN 'NO RECENT DEMAND'::text
            WHEN (closing_stock < reorder_point) THEN 'REORDER'::text
            ELSE 'SUFFICIENT'::text
        END AS inventory_status,
        CASE
            WHEN ((demand_30d > 0) AND (closing_stock < reorder_point)) THEN GREATEST(0, (target_stock - closing_stock))
            ELSE 0
        END AS suggested_order_units
   FROM stock_targets;


--
-- Name: warehouses_warehouse_id_seq; Type: SEQUENCE; Schema: ops; Owner: -
--

ALTER TABLE ops.warehouses ALTER COLUMN warehouse_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME ops.warehouses_warehouse_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: demand_forecasts demand_forecasts_pkey; Type: CONSTRAINT; Schema: analytics; Owner: -
--

ALTER TABLE ONLY analytics.demand_forecasts
    ADD CONSTRAINT demand_forecasts_pkey PRIMARY KEY (model_name, forecast_date);


--
-- Name: demand_spike_anomalies demand_spike_anomalies_pkey; Type: CONSTRAINT; Schema: analytics; Owner: -
--

ALTER TABLE ONLY analytics.demand_spike_anomalies
    ADD CONSTRAINT demand_spike_anomalies_pkey PRIMARY KEY (product_id, demand_date);


--
-- Name: hourly_anomaly_snapshots hourly_anomaly_snapshots_pkey; Type: CONSTRAINT; Schema: analytics; Owner: -
--

ALTER TABLE ONLY analytics.hourly_anomaly_snapshots
    ADD CONSTRAINT hourly_anomaly_snapshots_pkey PRIMARY KEY (snapshot_hour);


--
-- Name: operational_demand_forecasts operational_demand_forecasts_pkey; Type: CONSTRAINT; Schema: analytics; Owner: -
--

ALTER TABLE ONLY analytics.operational_demand_forecasts
    ADD CONSTRAINT operational_demand_forecasts_pkey PRIMARY KEY (product_id, model_name, asof_hour, forecast_hour);


--
-- Name: product_demand_forecasts uq_product_demand_forecast; Type: CONSTRAINT; Schema: analytics; Owner: -
--

ALTER TABLE ONLY analytics.product_demand_forecasts
    ADD CONSTRAINT uq_product_demand_forecast UNIQUE (product_id, model_name, forecast_date);


--
-- Name: customer_orders customer_orders_pkey; Type: CONSTRAINT; Schema: dataco; Owner: -
--

ALTER TABLE ONLY dataco.customer_orders
    ADD CONSTRAINT customer_orders_pkey PRIMARY KEY (order_id);


--
-- Name: order_items order_items_pkey; Type: CONSTRAINT; Schema: dataco; Owner: -
--

ALTER TABLE ONLY dataco.order_items
    ADD CONSTRAINT order_items_pkey PRIMARY KEY (order_item_id);


--
-- Name: products products_pkey; Type: CONSTRAINT; Schema: dataco; Owner: -
--

ALTER TABLE ONLY dataco.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (product_id);


--
-- Name: hourly_kpi_snapshots hourly_kpi_snapshots_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.hourly_kpi_snapshots
    ADD CONSTRAINT hourly_kpi_snapshots_pkey PRIMARY KEY (snapshot_hour);


--
-- Name: hourly_operational_kpi_snapshots hourly_operational_kpi_snapshots_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.hourly_operational_kpi_snapshots
    ADD CONSTRAINT hourly_operational_kpi_snapshots_pkey PRIMARY KEY (simulated_hour);


--
-- Name: hourly_simulation_runs hourly_simulation_runs_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.hourly_simulation_runs
    ADD CONSTRAINT hourly_simulation_runs_pkey PRIMARY KEY (run_hour);


--
-- Name: hourly_simulation_runs hourly_simulation_runs_simulated_hour_key; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.hourly_simulation_runs
    ADD CONSTRAINT hourly_simulation_runs_simulated_hour_key UNIQUE (simulated_hour);


--
-- Name: inventory_ledger_daily inventory_ledger_daily_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_ledger_daily
    ADD CONSTRAINT inventory_ledger_daily_pkey PRIMARY KEY (product_id, warehouse_id, stock_date);


--
-- Name: inventory_ledger_reorder_scenario inventory_ledger_reorder_scenario_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_ledger_reorder_scenario
    ADD CONSTRAINT inventory_ledger_reorder_scenario_pkey PRIMARY KEY (product_id, warehouse_id, stock_date);


--
-- Name: inventory_snapshots inventory_snapshots_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_snapshots
    ADD CONSTRAINT inventory_snapshots_pkey PRIMARY KEY (product_id, warehouse_id, snapshot_at);


--
-- Name: live_inventory_hourly live_inventory_hourly_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_inventory_hourly
    ADD CONSTRAINT live_inventory_hourly_pkey PRIMARY KEY (product_id, warehouse_id, snapshot_hour);


--
-- Name: live_supplier_shipments live_supplier_shipments_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_supplier_shipments
    ADD CONSTRAINT live_supplier_shipments_pkey PRIMARY KEY (shipment_id);


--
-- Name: live_supplier_shipments live_supplier_shipments_product_id_warehouse_id_ordered_at_key; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_supplier_shipments
    ADD CONSTRAINT live_supplier_shipments_product_id_warehouse_id_ordered_at_key UNIQUE (product_id, warehouse_id, ordered_at);


--
-- Name: product_suppliers product_suppliers_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.product_suppliers
    ADD CONSTRAINT product_suppliers_pkey PRIMARY KEY (product_id);


--
-- Name: purchase_orders purchase_orders_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.purchase_orders
    ADD CONSTRAINT purchase_orders_pkey PRIMARY KEY (po_id);


--
-- Name: purchase_orders purchase_orders_product_id_warehouse_id_ordered_on_supplier_key; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.purchase_orders
    ADD CONSTRAINT purchase_orders_product_id_warehouse_id_ordered_on_supplier_key UNIQUE (product_id, warehouse_id, ordered_on, supplier_id);


--
-- Name: suppliers suppliers_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.suppliers
    ADD CONSTRAINT suppliers_pkey PRIMARY KEY (supplier_id);


--
-- Name: suppliers suppliers_supplier_name_key; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.suppliers
    ADD CONSTRAINT suppliers_supplier_name_key UNIQUE (supplier_name);


--
-- Name: warehouses warehouses_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.warehouses
    ADD CONSTRAINT warehouses_pkey PRIMARY KEY (warehouse_id);


--
-- Name: warehouses warehouses_warehouse_name_key; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.warehouses
    ADD CONSTRAINT warehouses_warehouse_name_key UNIQUE (warehouse_name);


--
-- Name: idx_mv_replenishment_inputs; Type: INDEX; Schema: ops; Owner: -
--

CREATE UNIQUE INDEX idx_mv_replenishment_inputs ON ops.mv_replenishment_inputs USING btree (product_id, warehouse_id, stock_date);


--
-- Name: demand_spike_anomalies demand_spike_anomalies_product_id_fkey; Type: FK CONSTRAINT; Schema: analytics; Owner: -
--

ALTER TABLE ONLY analytics.demand_spike_anomalies
    ADD CONSTRAINT demand_spike_anomalies_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: order_items order_items_order_id_fkey; Type: FK CONSTRAINT; Schema: dataco; Owner: -
--

ALTER TABLE ONLY dataco.order_items
    ADD CONSTRAINT order_items_order_id_fkey FOREIGN KEY (order_id) REFERENCES dataco.customer_orders(order_id);


--
-- Name: order_items order_items_product_id_fkey; Type: FK CONSTRAINT; Schema: dataco; Owner: -
--

ALTER TABLE ONLY dataco.order_items
    ADD CONSTRAINT order_items_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: inventory_ledger_daily inventory_ledger_daily_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_ledger_daily
    ADD CONSTRAINT inventory_ledger_daily_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: inventory_ledger_daily inventory_ledger_daily_warehouse_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_ledger_daily
    ADD CONSTRAINT inventory_ledger_daily_warehouse_id_fkey FOREIGN KEY (warehouse_id) REFERENCES ops.warehouses(warehouse_id);


--
-- Name: inventory_ledger_reorder_scenario inventory_ledger_reorder_scenario_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_ledger_reorder_scenario
    ADD CONSTRAINT inventory_ledger_reorder_scenario_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: inventory_ledger_reorder_scenario inventory_ledger_reorder_scenario_warehouse_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_ledger_reorder_scenario
    ADD CONSTRAINT inventory_ledger_reorder_scenario_warehouse_id_fkey FOREIGN KEY (warehouse_id) REFERENCES ops.warehouses(warehouse_id);


--
-- Name: inventory_snapshots inventory_snapshots_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_snapshots
    ADD CONSTRAINT inventory_snapshots_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: inventory_snapshots inventory_snapshots_warehouse_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.inventory_snapshots
    ADD CONSTRAINT inventory_snapshots_warehouse_id_fkey FOREIGN KEY (warehouse_id) REFERENCES ops.warehouses(warehouse_id);


--
-- Name: live_inventory_hourly live_inventory_hourly_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_inventory_hourly
    ADD CONSTRAINT live_inventory_hourly_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: live_inventory_hourly live_inventory_hourly_warehouse_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_inventory_hourly
    ADD CONSTRAINT live_inventory_hourly_warehouse_id_fkey FOREIGN KEY (warehouse_id) REFERENCES ops.warehouses(warehouse_id);


--
-- Name: live_supplier_shipments live_supplier_shipments_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_supplier_shipments
    ADD CONSTRAINT live_supplier_shipments_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: live_supplier_shipments live_supplier_shipments_supplier_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_supplier_shipments
    ADD CONSTRAINT live_supplier_shipments_supplier_id_fkey FOREIGN KEY (supplier_id) REFERENCES ops.suppliers(supplier_id);


--
-- Name: live_supplier_shipments live_supplier_shipments_warehouse_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.live_supplier_shipments
    ADD CONSTRAINT live_supplier_shipments_warehouse_id_fkey FOREIGN KEY (warehouse_id) REFERENCES ops.warehouses(warehouse_id);


--
-- Name: product_suppliers product_suppliers_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.product_suppliers
    ADD CONSTRAINT product_suppliers_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: product_suppliers product_suppliers_supplier_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.product_suppliers
    ADD CONSTRAINT product_suppliers_supplier_id_fkey FOREIGN KEY (supplier_id) REFERENCES ops.suppliers(supplier_id);


--
-- Name: purchase_orders purchase_orders_product_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.purchase_orders
    ADD CONSTRAINT purchase_orders_product_id_fkey FOREIGN KEY (product_id) REFERENCES dataco.products(product_id);


--
-- Name: purchase_orders purchase_orders_supplier_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.purchase_orders
    ADD CONSTRAINT purchase_orders_supplier_id_fkey FOREIGN KEY (supplier_id) REFERENCES ops.suppliers(supplier_id);


--
-- Name: purchase_orders purchase_orders_warehouse_id_fkey; Type: FK CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.purchase_orders
    ADD CONSTRAINT purchase_orders_warehouse_id_fkey FOREIGN KEY (warehouse_id) REFERENCES ops.warehouses(warehouse_id);


--
-- PostgreSQL database dump complete
--

\unrestrict bb3k3mNRLD2JulFlzGMEmW3RCCWof8n1ul8lHYWrwmzH3u2E9LmHgiemHrZGTqy
