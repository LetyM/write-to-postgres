import psycopg2
import os
import traceback
import logging
from dotenv import load_dotenv

load_dotenv('/home/lety/Git/write-to-postgres/.venv/.env')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


def postgres_connection():
    try:
        conn = psycopg2.connect(
            host=os.environ['POSTGRES_HOST'],
            database=os.environ['POSTGRES_DATABASE'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=os.environ['POSTGRES_PORT']
        )
        logging.info('Postgres server connection is successful')
        return conn
    except Exception as e:
        traceback.print_exc()
        logging.error("Couldn't create the Postgres connection")
