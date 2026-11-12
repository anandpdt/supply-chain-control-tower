
-- ==================================================
-- NEXVIA - SYNTHETIC DEMO DATABASE SEED
-- Run only in a disposable demonstration database.
-- ==================================================

BEGIN;

DO $$
BEGIN
    IF current_database() = 'supply_chain_db' THEN
        RAISE EXCEPTION
            'Do not seed the original NEXVIA database';
    END IF;
END;
$$;

-- 1. PRODUCTS
INSERT INTO dataco.products
(product_id, product_name, category_id, category_name, product_price)
VALUES
(900001, 'Demo Gaming Console', 901, 'Electronics', 299),
(900002, 'Demo Notebook', 902, 'Stationery', 12),
(900003, 'Demo Water Bottle', 903, 'Accessories', 25),
(900004, 'Demo First Aid Kit', 904, 'Healthcare', 45),
(900005, 'Demo USB Cable', 901, 'Electronics', 15)
ON CONFLICT (product_id) DO NOTHING;

-- 2. SUPPLIERS
INSERT INTO ops.suppliers
(supplier_name, expected_lead_days, data_source)
VALUES
('Demo Supplier A', 5, 'SYNTHETIC_DEMO'),
('Demo Supplier B', 7, 'SYNTHETIC_DEMO'),
('Demo Supplier C', 9, 'SYNTHETIC_DEMO')
ON CONFLICT (supplier_name) DO NOTHING;

-- 3. WAREHOUSES
INSERT INTO ops.warehouses
(warehouse_name, city)
VALUES
('Demo Warehouse North', 'Delhi'),
('Demo Warehouse East', 'Patna'),
('Demo Warehouse West', 'Mumbai')
ON CONFLICT (warehouse_name) DO NOTHING;

-- 4. PRODUCT-SUPPLIER MAPPINGS
INSERT INTO ops.product_suppliers
(product_id, supplier_id)
SELECT m.product_id, s.supplier_id
FROM (
    VALUES
    (900001, 'Demo Supplier A'),
    (900002, 'Demo Supplier B'),
    (900003, 'Demo Supplier A'),
    (900004, 'Demo Supplier C'),
    (900005, 'Demo Supplier B')
) AS m(product_id, supplier_name)
JOIN ops.suppliers s
    ON s.supplier_name = m.supplier_name
ON CONFLICT (product_id) DO NOTHING;

-- 5. PURCHASE ORDERS
INSERT INTO ops.purchase_orders
(
    product_id,
    supplier_id,
    warehouse_id,
    ordered_on,
    expected_on,
    quantity_ordered,
    order_status
)
SELECT
    ps.product_id,
    ps.supplier_id,
    w.warehouse_id,
    DATE '2026-11-01',
    DATE '2026-11-01' + s.expected_lead_days,
    20,
    'ORDERED'
FROM ops.product_suppliers ps
JOIN ops.suppliers s
    ON s.supplier_id = ps.supplier_id
CROSS JOIN ops.warehouses w
WHERE ps.product_id BETWEEN 900001 AND 900005
  AND w.warehouse_name = 'Demo Warehouse East'
ON CONFLICT
(product_id, warehouse_id, ordered_on, supplier_id)
DO NOTHING;

-- 6. DAILY INVENTORY
INSERT INTO ops.inventory_ledger_daily
(
    product_id,
    warehouse_id,
    stock_date,
    opening_stock,
    received_units,
    demand_units,
    fulfilled_units,
    unmet_units,
    closing_stock,
    data_source
)
SELECT
    p.product_id,
    w.warehouse_id,
    DATE '2026-11-01',
    50,
    0,
    5,
    5,
    0,
    45,
    'SYNTHETIC_DEMO'
FROM dataco.products p
CROSS JOIN ops.warehouses w
WHERE p.product_id BETWEEN 900001 AND 900005
  AND w.warehouse_name IN (
      'Demo Warehouse North',
      'Demo Warehouse East',
      'Demo Warehouse West'
  )
ON CONFLICT
(product_id, warehouse_id, stock_date)
DO NOTHING;

-- 7. INVENTORY SNAPSHOTS
INSERT INTO ops.inventory_snapshots
(
    product_id,
    warehouse_id,
    snapshot_at,
    stock_on_hand,
    reserved_qty
)
SELECT
    l.product_id,
    l.warehouse_id,
    TIMESTAMPTZ '2026-11-01 23:00:00+05:30',
    l.closing_stock,
    0
FROM ops.inventory_ledger_daily l
WHERE l.product_id BETWEEN 900001 AND 900005
  AND l.stock_date = DATE '2026-11-01'
ON CONFLICT
(product_id, warehouse_id, snapshot_at)
DO NOTHING;

COMMIT;

-- FINAL VALIDATION
SELECT 'Products' AS component, COUNT(*) AS total
FROM dataco.products
WHERE product_id BETWEEN 900001 AND 900005

UNION ALL
SELECT 'Suppliers', COUNT(*)
FROM ops.suppliers
WHERE data_source = 'SYNTHETIC_DEMO'

UNION ALL
SELECT 'Warehouses', COUNT(*)
FROM ops.warehouses
WHERE warehouse_name LIKE 'Demo Warehouse %'

UNION ALL
SELECT 'Mappings', COUNT(*)
FROM ops.product_suppliers
WHERE product_id BETWEEN 900001 AND 900005

UNION ALL
SELECT 'Purchase Orders', COUNT(*)
FROM ops.purchase_orders
WHERE product_id BETWEEN 900001 AND 900005

UNION ALL
SELECT 'Inventory Ledger', COUNT(*)
FROM ops.inventory_ledger_daily
WHERE product_id BETWEEN 900001 AND 900005

UNION ALL
SELECT 'Inventory Snapshots', COUNT(*)
FROM ops.inventory_snapshots
WHERE product_id BETWEEN 900001 AND 900005;
