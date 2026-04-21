"""
Description :  POC ingestion de données météo à Paris dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, json, requests
from sf_connect import get_connection

URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=48.8566&longitude=2.3522"
    "&current=temperature_2m,precipitation,windspeed_10m,weathercode,relativehumidity_2m"
    "&timezone=Europe%2FParis"
)

RUN_ID = os.environ.get("GITHUB_RUN_ID")

r = requests.get(URL, timeout=30)
r.raise_for_status()
data = r.json().get("current", {})
print(f"  → {data}")

conn = get_connection()
cur  = conn.cursor()
cur.execute(
    "INSERT INTO RAW.METEO_PARIS(_run_id, _raw) SELECT %s, %s::VARIANT",
    (RUN_ID, json.dumps(data))
)
conn.commit()
print("✅ Météo de Paris insérée")
cur.close()
conn.close()