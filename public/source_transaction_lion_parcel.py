import logging
from pandas_gbq import to_gbq
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from google.oauth2 import service_account
# from framework import config, connection
import os
import sys
import asyncio
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl import connection
from datetime import datetime
import pytz
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

##target
table = 'stg_retail_transactions'
schema = 'public'
fullStrTable = f'{schema}.{table}'


def extract(engine):

    # SQL query
    sql = """
  select
	id,
	customer_id,
	last_status,
	pos_origin,
	pos_destination,
	created_at,
	updated_at,
	current_timestamp as loaded_at
from
	schema_punten.source_transaction_lion_parcel
    """
    dfsource = pd.read_sql(sql, con=engine)
    return dfsource    

def transform(df):

    return df

def load(df,knbistg_engine):

    connection = knbistg_engine.raw_connection()
    curr = connection.cursor()

    # sql = f"TRUNCATE TABLE {schema}.{table}"
    # curr.execute(sql)
    # connection.commit()

    curr.close()
    connection.close()

    df.to_sql(
        name=table, 
        con=knbistg_engine, 
        schema=schema, 
        if_exists='replace',
        index=False
    )
    
def main():
    """Main function to execute the script."""
    try:
        ##open connection
        mssql_sferps_engine = connection.dwh_create_db_connection()
        knbistg_engine = connection.dwh_create_db_connection()

        ##extract
        dfextract = extract(mssql_sferps_engine)
        dfextract.info(memory_usage='deep')

        ##transform
        dftransform = transform(dfextract)
        dftransform.info(memory_usage='deep')

        ##load
        load(dftransform,knbistg_engine)


        del [[dfextract]]
        del [[dftransform]]
        gc.collect()
        mssql_sferps_engine.dispose()
        knbistg_engine.dispose()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":

    main()