-- Ingestion contract enforcement for Baserow (contracts/baserow_ecommerce.yaml).
-- Landing table: raw rows as extracted. The Null engine stores nothing and only fires the views below.
CREATE TABLE IF NOT EXISTS medallion_warehouse.landing_baserow_rows
(
    table_name String,
    row_id UInt64,
    payload String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = Null;

-- Checked table: the same rows plus the list of rules each row failed. Also Null.
CREATE TABLE IF NOT EXISTS medallion_warehouse.checked_baserow_rows
(
    table_name String,
    row_id UInt64,
    payload String,
    extracted_at DateTime64(3, 'UTC'),
    failed_rules Array(String)
)
ENGINE = Null;

-- Quarantine: rejected rows with the reasons, kept for 90 days.
CREATE TABLE IF NOT EXISTS medallion_warehouse.quarantine_baserow_rows
(
    quarantined_at DateTime64(3, 'UTC') DEFAULT now64(3),
    contract LowCardinality(String) DEFAULT 'baserow_ecommerce',
    table_name LowCardinality(String),
    row_id UInt64,
    payload String,
    extracted_at DateTime64(3, 'UTC'),
    failed_rules Array(String)
)
ENGINE = MergeTree
ORDER BY (quarantined_at, table_name)
TTL toDateTime(quarantined_at) + INTERVAL 90 DAY;

-- The rules per entity. Rule names match the field names in the contract.
CREATE MATERIALIZED VIEW IF NOT EXISTS medallion_warehouse.mv_baserow_check
TO medallion_warehouse.checked_baserow_rows AS
SELECT
    table_name,
    row_id,
    payload,
    extracted_at,
    arrayFilter(x -> x != '', multiIf(
        table_name = 'customers', [
            if(match(f_customer_code, '^C[0-9]{4}$'), '', 'customer_code_format'),
            if(notEmpty(trim(f_full_name)), '', 'full_name_required'),
            if(match(f_email, '^[^@ ]+@[^@ ]+[.][^@ ]+$'), '', 'email_format'),
            if(d_signup IS NOT NULL AND d_signup <= today(), '', 'signup_date_valid'),
            if(lower(trim(f_is_active)) IN ('true', 'false'), '', 'is_active_allowed')
        ],
        table_name = 'products', [
            if(match(f_product_code, '^P[0-9]{3}$'), '', 'product_code_format'),
            if(notEmpty(trim(f_product_name)), '', 'product_name_required'),
            if(notEmpty(trim(f_category)), '', 'category_required'),
            if(n_price IS NOT NULL AND isFinite(n_price) AND n_price > 0, '', 'unit_price_positive')
        ],
        table_name = 'orders', [
            if(match(f_order_code, '^O[0-9]{5}$'), '', 'order_code_format'),
            if(match(f_customer_code, '^C[0-9]{4}$'), '', 'customer_code_format'),
            if(match(f_product_code, '^P[0-9]{3}$'), '', 'product_code_format'),
            if(n_qty IS NOT NULL AND n_qty > 0 AND n_qty <= 100, '', 'quantity_range'),
            if(d_order IS NOT NULL AND d_order <= today() AND d_order >= today() - 730, '', 'order_date_valid'),
            if(lower(trim(f_status)) IN ('delivered', 'shipped', 'pending', 'cancelled', 'returned'), '', 'status_allowed'),
            if(n_disc IS NOT NULL AND isFinite(n_disc) AND n_disc >= 0 AND n_disc <= 100, '', 'discount_range')
        ],
        ['unknown_table']
    )) AS failed_rules
FROM
(
    SELECT
        *,
        JSONExtractString(payload, 'customer_code') AS f_customer_code,
        JSONExtractString(payload, 'full_name') AS f_full_name,
        JSONExtractString(payload, 'email') AS f_email,
        JSONExtractString(payload, 'signup_date') AS f_signup_date,
        JSONExtractString(payload, 'is_active') AS f_is_active,
        JSONExtractString(payload, 'product_code') AS f_product_code,
        JSONExtractString(payload, 'product_name') AS f_product_name,
        JSONExtractString(payload, 'category') AS f_category,
        JSONExtractString(payload, 'unit_price') AS f_unit_price,
        JSONExtractString(payload, 'order_code') AS f_order_code,
        JSONExtractString(payload, 'quantity') AS f_quantity,
        JSONExtractString(payload, 'order_date') AS f_order_date,
        JSONExtractString(payload, 'status') AS f_status,
        JSONExtractString(payload, 'discount_pct') AS f_discount_pct,
        toDateOrNull(f_signup_date) AS d_signup,
        toFloat64OrNull(f_unit_price) AS n_price,
        toInt32OrNull(f_quantity) AS n_qty,
        toDateOrNull(f_order_date) AS d_order,
        toFloat64OrNull(f_discount_pct) AS n_disc
    FROM medallion_warehouse.landing_baserow_rows
);

-- Route 1: rows that passed every rule go to bronze.
CREATE MATERIALIZED VIEW IF NOT EXISTS medallion_warehouse.mv_baserow_to_bronze
TO medallion_warehouse.bronze_baserow_rows AS
SELECT table_name, row_id, payload, extracted_at
FROM medallion_warehouse.checked_baserow_rows
WHERE empty(failed_rules);

-- Route 2: rows that failed any rule go to quarantine with the reasons.
CREATE MATERIALIZED VIEW IF NOT EXISTS medallion_warehouse.mv_baserow_to_quarantine
TO medallion_warehouse.quarantine_baserow_rows AS
SELECT table_name, row_id, payload, extracted_at, failed_rules
FROM medallion_warehouse.checked_baserow_rows
WHERE notEmpty(failed_rules);