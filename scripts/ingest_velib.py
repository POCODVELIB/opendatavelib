"""
Description :  POC ingestion de données d'un dataset OpenData VELIB dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, json, requests
from sf_connect import get_connection

DATASETS = {
    "station_information": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
    "station_status":      "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
}

RUN_ID = os.environ.get("GITHUB_RUN_ID")
conn = get_connection()
cur  = conn.cursor()

for dataset, url in DATASETS.items():
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    rows = [v for v in r.json().get("data", {}).values() if isinstance(v, list)][0]
    print(f"{dataset} → {len(rows)} records")
    for row in rows:
        cur.execute(
            f"INSERT INTO RAW.{dataset.upper()}(_run_id, _raw) SELECT %s, %s::VARIANT",
            (RUN_ID, json.dumps(row))
        )
    conn.commit()

cur.close()
conn.close()