from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv('API_FOOTBALL_KEY')
BASE_URL = os.getenv('API_BASE_URL')

# Databricks fallback
if not API_KEY:
    try:
        API_KEY = dbutils.secrets.get(
            scope="football-api",
            key="api-football-key"
        )
    except:
        pass

if not API_KEY:
    raise ValueError("API_FOOTBALL_KEY not found")

if not BASE_URL:
    raise ValueError("API_BASE_URL not found")
