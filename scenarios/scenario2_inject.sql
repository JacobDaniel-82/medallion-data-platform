-- Scenario 2: send crafted bad rows through the Baserow ingestion contract.
-- Row ids start at 900001 so they are easy to find and remove.
-- O90001 is a valid control row. The other four each break one rule.
INSERT INTO medallion_warehouse.landing_baserow_rows (table_name, row_id, payload, extracted_at) VALUES
('orders', 900001, '{"id":900001,"order_code":"O90001","customer_code":"C0001","product_code":"P001","quantity":"2","order_date":"2026-11-01","status":"Delivered","discount_pct":"0"}', now64(3)),
('orders', 900002, '{"id":900002,"order_code":"O90002","customer_code":"C0001","product_code":"P001","quantity":"-5","order_date":"2026-11-01","status":"Delivered","discount_pct":"0"}', now64(3)),
('orders', 900003, '{"id":900003,"order_code":"O90003","customer_code":"C0001","product_code":"P001","quantity":"2","order_date":"2026-11-01","status":"weird","discount_pct":"0"}', now64(3)),
('orders', 900004, '{"id":900004,"order_code":"O90004","customer_code":"C0001","product_code":"P001","quantity":"2","order_date":"2026-11-01","status":"Delivered","discount_pct":"150"}', now64(3)),
('customers', 900005, '{"id":900005,"customer_code":"C9005","full_name":"Test User","email":"not-an-email","country":"India","signup_date":"2026-01-01","is_active":"true"}', now64(3));