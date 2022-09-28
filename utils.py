import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import pickle
from sqlalchemy import create_engine

### can be replace with other approaches e.g. AWS Secret Manager
secret = pickle.load(open(r'./secret.pickle', 'rb'))

def get_api_key():
    api_key = secret['API_KEY']
    return api_key

def get_conn(db_name):
    conn = None
    host = secret['pg_host']
    database = db_name
    user = secret['pg_user']
    password = secret['pg_password']
    try:
        conn_string = f'postgresql://{user}:{password}@{host}:5432/{database}'
        db = create_engine(conn_string)
        conn = db.connect()
        print('Connected to PostgresDB')
        return conn
    except Exception as e:
        print(f'Cannot Connect to Database. Error : {e}')
        raise

def create_table(conn, table_name):
    file = open(f'./sql/create_table/{table_name}.sql', 'r')
    sql = file.read()
    conn.execute(sql)
    print(f'Table {table_name} Created')

def insert_to_table(conn, df, schema, table_name):
    df.to_sql(schema=schema, name=table_name, con=conn, if_exists='append',index=False)
    print(f'Data Inserted into {table_name}')

def deduplicate(conn, schema, table_name, primary_keys):
    pk_query = ''
    for pk in primary_keys:
        pk_query += f'a."{pk}" = b."{pk}"'
        if pk != primary_keys[-1]:
            pk_query += ' and '

    sql = f"""
        delete from {schema}.{table_name}
        where ctid in (
        select a.ctid from {schema}.{table_name} a
            left join {schema}.{table_name} b 
            on {pk_query}
            where a.ctid > b.ctid
            and a.updated_time >= b.updated_time
         )
         """
    conn.execute(sql)
    print(f'Table {schema}.{table_name} is Deduplicated')
