import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from conect_postgre_sql import DBConnect

connector   = psycopg2
dbname      = "python"
user        = "postgres"
password    = "postgres"
host        = "192.168.0.112"
port        = "5432"


conexao_sql_alchemy = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
engine = create_engine(conexao_sql_alchemy)



# with DBConnect(connector=connector, dbname=dbname, user=user, password=password, host=host, port=port) as db_conn:
#     query = "select * from public.mapeamento_anac limit 100"
#     df = pd.read_sql_query(query, db_conn.db_connect())
#     print(df.head(5))