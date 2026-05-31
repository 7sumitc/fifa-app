from dotenv import load_dotenv
import os

load_dotenv()

BASE_URL = os.getenv(
    "API_BASE_URL",
    "https://v3.football.api-sports.io"
)

API_KEY = os.getenv("API_FOOTBALL_KEY")

if not API_KEY:
    try:
        from dbruntime.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        dbutils = DBUtils(spark)

        API_KEY = dbutils.secrets.get(
            scope="football-api",
            key="api-football-key"
        )

    except Exception as e:
        raise ValueError(
            f"Unable to retrieve API key: {e}"
        )

if not API_KEY:
    raise ValueError("API_FOOTBALL_KEY not found")
