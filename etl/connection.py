import os, logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

# opsional: kalau etl.config hanya untuk local dev, boleh dibiarkan
try:
    from etl import config  # ini kalau memang tugasnya set os.environ saat running lokal
except Exception:
    pass

DWH_DB_CONFIG = {
    "ENGINE":   os.getenv("DWH_ENGINE",   "postgresql+psycopg2"),
    "USER":     os.getenv("DWH_USER",     "airflow"),
    "PASSWORD": os.getenv("DWH_PASSWORD", "airflow"),
    # default host 'postgres' agar jalan di container; lokal bisa override jadi 'localhost'
    "HOST":     os.getenv("DWH_HOST",     "postgres"),
    "PORT":     os.getenv("DWH_PORT",     "5432"),
    "DATABASE": os.getenv("DWH_DATABASE", "airflow"),
}

def dwh_create_db_connection():
    """Create a database connection using SQLAlchemy."""
    try:
        conn_str = (
            f"{DWH_DB_CONFIG['ENGINE']}://{DWH_DB_CONFIG['USER']}:"
            f"{quote_plus(DWH_DB_CONFIG['PASSWORD'])}@"
            f"{DWH_DB_CONFIG['HOST']}:{DWH_DB_CONFIG['PORT']}/"
            f"{DWH_DB_CONFIG['DATABASE']}"
        )
        engine = create_engine(conn_str)
        logging.info("Database connection established successfully.")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Error creating database connection: {e}")
        raise
