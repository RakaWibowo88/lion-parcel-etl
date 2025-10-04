import os
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
os.environ['DBT_PROJECT_PATH'] = str(project_root / "dbt")

# DB WAREHOUSE (pakai Postgres lokal/container)
APP_ENV = os.getenv("APP_ENV", "local")

if APP_ENV == "local":
    # Running langsung di laptop
    os.environ['DWH_HOST'] = 'localhost'
    os.environ['DWH_PORT'] = '5432'
    os.environ['DWH_DATABASE'] = 'postgres'
    os.environ['DWH_USER'] = 'postgres'
    os.environ['DWH_PASSWORD'] = 'postgres'
    os.environ['DWH_ENGINE'] = 'postgresql+psycopg2'
else:
    # Running di dalam container (pakai env dari docker-compose)
    # -> jangan override, biar ambil dari environment di compose
    pass
