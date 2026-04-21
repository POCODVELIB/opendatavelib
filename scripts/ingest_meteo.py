"""
Description :  POC ingestion de données météo à Paris dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, json, requests, snowflake.connector
from datetime import datetime, timezone

URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=48.8566&longitude=2.3522"
    "&current=temperature_2m,precipitation,windspeed_10m,weathercode,relativehumidity_2m"
    "&timezone=Europe%2FParis"
)

SF = dict(
    account=os.environ["SF_ACCOUNT"],
    user=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    database=os.environ["SF_DATABASE"],
    schema="RAW",
    warehouse=os.environ["SF_WAREHOUSE"],
    role="INGESTION_ROLE",
)
RUN_ID = os.environ.get(
    "GITHUB_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
)


def main():
    # Extract
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    data = r.json().get("current", {})
    print(f"  → {data}")

    # Load
    conn = snowflake.connector.connect(**SF)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO RAW.METEO_PARIS(_run_id, _raw) SELECT %s, %s::VARIANT",
        (RUN_ID, json.dumps(data)),
    )
    conn.commit()
    print("✅ Météo insérée")
    cur.close()
    conn.close()
