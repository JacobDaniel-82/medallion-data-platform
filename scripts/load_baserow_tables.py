import json
import os
import urllib.request
from datetime import datetime, timezone

import clickhouse_connect

BASE_URL = os.environ.get("BASEROW_API_URL", "https://api.baserow.io").rstrip("/")
TOKEN = os.environ["BASEROW_TOKEN"]
PAGE_SIZE = 200

TABLES = {
    "customers": os.environ["BASEROW_CUSTOMERS_TABLE_ID"],
    "products": os.environ["BASEROW_PRODUCTS_TABLE_ID"],
    "orders": os.environ["BASEROW_ORDERS_TABLE_ID"],
}

CREATE_BRONZE = """
CREATE TABLE IF NOT EXISTS medallion_warehouse.bronze_baserow_rows
(
    table_name LowCardinality(String),
    row_id UInt64,
    payload String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (table_name, row_id)
"""


def fetch_all_rows(table_id):
    rows = []
    page = 1
    while True:
        url = (
            f"{BASE_URL}/api/database/rows/table/{table_id}/"
            f"?user_field_names=true&size={PAGE_SIZE}&page={page}"
        )
        request = urllib.request.Request(url, headers={"Authorization": f"Token {TOKEN}"})
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        rows.extend(data["results"])
        if not data.get("next"):
            break
        page += 1

    if len(rows) != data["count"]:
        raise RuntimeError(f"Table {table_id}: fetched {len(rows)} rows but Baserow reports {data['count']}.")
    return rows


def main():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="medallion_warehouse",
    )
    client.command(CREATE_BRONZE)

    extracted_at = datetime.now(timezone.utc)

    # Fetch everything first, so a failed API call never wipes existing bronze data.
    fetched = {name: fetch_all_rows(table_id) for name, table_id in TABLES.items()}

    for name, rows in fetched.items():
        client.command(
            "ALTER TABLE bronze_baserow_rows DELETE WHERE table_name = {t:String} SETTINGS mutations_sync = 1",
            parameters={"t": name},
        )
        client.insert(
            "bronze_baserow_rows",
            [(name, int(row["id"]), json.dumps(row, ensure_ascii=False), extracted_at) for row in rows],
            column_names=["table_name", "row_id", "payload", "extracted_at"],
        )
        print(f"Loaded {len(rows)} rows from Baserow table '{name}' into bronze.")


if __name__ == "__main__":
    main()