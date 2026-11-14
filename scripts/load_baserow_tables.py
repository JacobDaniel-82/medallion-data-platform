import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
import yaml

BASE_URL = os.environ.get("BASEROW_API_URL", "https://api.baserow.io").rstrip("/")
TOKEN = os.environ["BASEROW_TOKEN"]
PAGE_SIZE = 200

TABLES = {
    "customers": os.environ["BASEROW_CUSTOMERS_TABLE_ID"],
    "products": os.environ["BASEROW_PRODUCTS_TABLE_ID"],
    "orders": os.environ["BASEROW_ORDERS_TABLE_ID"],
}

CONTRACT_PATH = Path(os.getenv("CONTRACTS_DIR", "/opt/airflow/contracts")) / "baserow_ecommerce.yaml"


def load_contract_limits():
    with open(CONTRACT_PATH, encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    sla = contract["sla"]
    return sla["volume"]["min_rows"], sla["quarantine"]["max_reject_rate"]


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
    min_rows, max_reject_rate = load_contract_limits()

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="medallion_warehouse",
    )

    # Fetch everything first, so a failed API call never touches the warehouse.
    fetched = {name: fetch_all_rows(table_id) for name, table_id in TABLES.items()}

    now = datetime.now(timezone.utc)
    extracted_at = now.replace(microsecond=(now.microsecond // 1000) * 1000)

    landing_rows = [
        (name, int(row["id"]), json.dumps(row, ensure_ascii=False), extracted_at)
        for name, rows in fetched.items()
        for row in rows
    ]

    # The contract rules run inside ClickHouse: valid rows reach bronze, invalid rows reach quarantine.
    client.insert(
        "landing_baserow_rows",
        landing_rows,
        column_names=["table_name", "row_id", "payload", "extracted_at"],
    )

    rejected = dict(
        client.query(
            "SELECT table_name, count() FROM quarantine_baserow_rows "
            "WHERE extracted_at = {ts:DateTime64(3)} GROUP BY table_name",
            parameters={"ts": extracted_at},
        ).result_rows
    )

    problems = []
    for name, rows in fetched.items():
        n_rejected = rejected.get(name, 0)
        loaded = len(rows) - n_rejected
        reject_rate = n_rejected / len(rows) if rows else 1.0
        print(f"{name}: fetched {len(rows)}, accepted {loaded}, quarantined {n_rejected} (reject rate {reject_rate:.2%}).")
        if reject_rate > max_reject_rate:
            problems.append(f"{name}: reject rate {reject_rate:.2%} is above the contract limit {max_reject_rate:.0%}")
        if loaded < min_rows[name]:
            problems.append(f"{name}: only {loaded} valid rows, the contract requires at least {min_rows[name]}")

    if problems:
        # Remove what this run loaded and keep the previous bronze data untouched.
        client.command(
            "ALTER TABLE bronze_baserow_rows DELETE WHERE extracted_at = {ts:DateTime64(3)} SETTINGS mutations_sync = 1",
            parameters={"ts": extracted_at},
        )
        raise RuntimeError("Contract breach, load rolled back: " + "; ".join(problems))

    # The new load passed, so replace the previous one.
    client.command(
        "ALTER TABLE bronze_baserow_rows DELETE WHERE extracted_at < {ts:DateTime64(3)} SETTINGS mutations_sync = 1",
        parameters={"ts": extracted_at},
    )
    print("Baserow bronze reloaded. Previous rows replaced.")


if __name__ == "__main__":
    main()