"""
Description :  POC ingestion de données météo à Paris dans Snowflake
-------------------------------------------------------------------------------------
Author      : Said HOUSSEINE   
Created     : 2026-04-21
-------------------------------------------------------------------------------------
"""

import os, snowflake.connector

def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SF_ACCOUNT"],
        user      = os.environ["SF_USER"],
        password  = os.environ["SF_PASSWORD"],
        database  = os.environ["SF_DATABASE"],
        schema    = "RAW",
        warehouse = os.environ["SF_WAREHOUSE"],
        role      = "INGESTION_ROLE",
    )