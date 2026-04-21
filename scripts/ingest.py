"""
Description :  POC ingestion de données d'un dataset OpenData VELIB dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, json, requests, snowflake.connector
from datetime import datetime, timezone

DATASETS = {
    "station_information": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
    "station_status":      "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
}

SF = dict(
    account   = os.environ["SF_ACCOUNT"],
    user      = os.environ["SF_USER"],
    password  = os.environ["SF_PASSWORD"],
    database  = os.environ["SF_DATABASE"],
    schema    = "RAW",
    warehouse = os.environ["SF_WAREHOUSE"],
    role      = "INGESTION_ROLE",
)
RUN_ID = os.environ.get("GITHUB_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))

def fetch(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Les données GBFS sont dans data.data.<clé>[]
    for key in data.get("data", {}).values():
        if isinstance(key, list):
            return key
    return []

def load(cur, dataset, rows):
    table = dataset.upper()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS RAW.{table} (
            _run_id    VARCHAR,
            _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            _dataset   VARCHAR,
            _raw       VARIANT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW.AUDIT (
            run_id VARCHAR, dataset VARCHAR,
            started_at TIMESTAMP_TZ, finished_at TIMESTAMP_TZ,
            rows_inserted INT, status VARCHAR, error VARCHAR
        )
    """)
    batch = [(RUN_ID, dataset, json.dumps(r)) for r in rows]
    cur.executemany(
        f"INSERT INTO RAW.{table}(_run_id,_dataset,_raw) SELECT %s,%s,PARSE_JSON(%s)",
        batch
    )
    return len(batch)

def main():
    started = datetime.now(timezone.utc)
    conn = snowflake.connector.connect(**SF)
    cur  = conn.cursor()
    try:
        total = 0
        for dataset, url in DATASETS.items():
            print(f"Fetching {dataset}...")
            rows = fetch(url)
            print(f"  → {len(rows)} records")
            n = load(cur, dataset, rows)
            conn.commit()
            cur.execute(
                "INSERT INTO RAW.AUDIT VALUES(%s,%s,%s,%s,%s,%s,%s)",
                (RUN_ID, dataset, started, datetime.now(timezone.utc), n, "SUCCESS", None)
            )
            conn.commit()
            total += n
        print(f"✅ {total} rows inserted")
    except Exception as e:
        conn.rollback()
        cur.execute(
            "INSERT INTO RAW.AUDIT VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (RUN_ID, "all", started, datetime.now(timezone.utc), 0, "FAILED", str(e))
        )
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()