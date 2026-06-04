import sys
from pathlib import Path

project_root = Path.cwd().parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.environment import BASE_URL, API_KEY

if not API_KEY:
    API_KEY = dbutils.secrets.get(
        scope="football-api",
        key="api-football-key"
    )

print(BASE_URL)
print(API_KEY[:5])