"""
Description :  POC ingestion de données météo à Paris dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, json, requests
from sf_connect import get_connection

POINTS = [
    {"nom": "Paris 1",   "lat": 48.8606, "lon": 2.3477},
    {"nom": "Paris 2",  "lat": 48.8669, "lon": 2.3431},
    {"nom": "Paris 3",  "lat": 48.8637, "lon": 2.3606},
    {"nom": "Paris 4",  "lat": 48.8534, "lon": 2.3517},
    {"nom": "Paris 5",  "lat": 48.8462, "lon": 2.3508},
    {"nom": "Paris 6",  "lat": 48.8496, "lon": 2.3331},
    {"nom": "Paris 7",  "lat": 48.8566, "lon": 2.3100},
    {"nom": "Paris 8",  "lat": 48.8752, "lon": 2.3088},
    {"nom": "Paris 9",  "lat": 48.8765, "lon": 2.3390},
    {"nom": "Paris 10", "lat": 48.8760, "lon": 2.3590},
    {"nom": "Paris 11", "lat": 48.8589, "lon": 2.3794},
    {"nom": "Paris 12", "lat": 48.8448, "lon": 2.3893},
    {"nom": "Paris 13", "lat": 48.8322, "lon": 2.3561},
    {"nom": "Paris 14", "lat": 48.8330, "lon": 2.3272},
    {"nom": "Paris 15", "lat": 48.8418, "lon": 2.2986},
    {"nom": "Paris 16", "lat": 48.8637, "lon": 2.2769},
    {"nom": "Paris 17", "lat": 48.8848, "lon": 2.3170},
    {"nom": "Paris 18", "lat": 48.8927, "lon": 2.3444},
    {"nom": "Paris 19", "lat": 48.8799, "lon": 2.3810},
    {"nom": "Paris 20", "lat": 48.8640, "lon": 2.4014},
]

RUN_ID = os.environ.get("GITHUB_RUN_ID")

lats = ",".join(str(p["lat"]) for p in POINTS)
lons = ",".join(str(p["lon"]) for p in POINTS)

URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={lats}&longitude={lons}"
    "&current=temperature_2m,precipitation,windspeed_10m,weathercode,relativehumidity_2m"
    "&timezone=Europe%2FParis"
)

r = requests.get(URL, timeout=30)
r.raise_for_status()
results = r.json()

if isinstance(results, dict):
    results = [results]

conn = get_connection()
cur  = conn.cursor()

for i, data in enumerate(results):
    row = {
        "nom": POINTS[i]["nom"],
        "lat": POINTS[i]["lat"],
        "lon": POINTS[i]["lon"],
        **data.get("current", {})
    }
    cur.execute(
        "INSERT INTO RAW.METEO_PARIS(_run_id, _raw) SELECT %s, %s::VARIANT",
        (RUN_ID, json.dumps(row))
    )

conn.commit()
print(f" {len(results)} arrondissements météo insérés")
cur.close()
conn.close()