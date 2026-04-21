"""
Description :  POC ingestion de données d'un dataset OpenData VELIB dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, json, requests, snowflake.connector
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────
DOMAIN  = os.environ["OPVELIB_DOMAIN"]
DATASET = os.environ["OPVELIB_DATASET"]
APIKEY  = os.environ.get("OPVELIB_API_KEY", "")
PAGE    = int(os.environ.get("PAGE_SIZE", 100))

SF = dict(
    account   = os.environ["SF_ACCOUNT"],
    user      = os.environ["SF_USER"],
    password  = os.environ["SF_PASSWORD"],
    database  = os.environ["SF_DATABASE"],
    schema    = "RAW",
    warehouse = os.environ["SF_WAREHOUSE"],
    role      = "INGESTION_ROLE",   # moindre privilège : INSERT/SELECT sur RAW uniquement
)
TABLE  = DATASET.upper().replace("-", "_")
RUN_ID = os.environ.get("GITHUB_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))

# ── Extract ──────────────────────────────────────
def fetch():
    url     = f"https://{DOMAIN}/api/explore/v2.1/catalog/datasets/{DATASET}/records"
    headers = {"Authorization": f"Apikey {APIKEY}"} if APIKEY else {}
    offset, total = 0, None

    while True:
        r = requests.get(url, headers=headers, params={"limit": PAGE, "offset": offset}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if total is None:
            total = data["total_count"]
            print(f"Total records: {total}")
        rows = data.get("results", [])
        if not rows:
            break
        yield from rows
        offset += PAGE
        if offset >= total:
            break

# ── Load ─────────────────────────────────────────
def load(cur, rows):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS RAW.{TABLE} (
            _run_id     VARCHAR,
            _loaded_at  TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            _dataset    VARCHAR,
            _raw        VARIANT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS RAW.AUDIT (
            run_id VARCHAR, dataset VARCHAR,
            started_at TIMESTAMP_TZ, finished_at TIMESTAMP_TZ,
            rows_inserted INT, status VARCHAR, error VARCHAR
        )
    """)
    batch, n = [], 0
    for row in rows:
        batch.append((RUN_ID, DATASET, json.dumps(row)))
        if len(batch) >= PAGE:
            cur.executemany(
                f"INSERT INTO RAW.{TABLE}(_run_id,_dataset,_raw) SELECT %s,%s,PARSE_JSON(%s)",
                batch
            )
            n += len(batch)
            batch = []
    if batch:
        cur.executemany(
            f"INSERT INTO RAW.{TABLE}(_run_id,_dataset,_raw) SELECT %s,%s,PARSE_JSON(%s)",
            batch
        )
        n += len(batch)
    return n

# ── Main ─────────────────────────────────────────
def main():
    started = datetime.now(timezone.utc)
    conn = snowflake.connector.connect(**SF)
    cur  = conn.cursor()
    try:
        n = load(cur, fetch())
        conn.commit()
        cur.execute(
            "INSERT INTO RAW.AUDIT VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (RUN_ID, DATASET, started, datetime.now(timezone.utc), n, "SUCCESS", None)
        )
        conn.commit()
        print(f"✅ {n} rows inserted into RAW.{TABLE}")
    except Exception as e:
        conn.rollback()
        cur.execute(
            "INSERT INTO RAW.AUDIT VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (RUN_ID, DATASET, started, datetime.now(timezone.utc), 0, "FAILED", str(e))
        )
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()