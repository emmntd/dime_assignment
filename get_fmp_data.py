import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import requests
import json
import pandas as pd
import re
from datetime import datetime
from glob import glob
from utils import get_api_key, get_conn, create_table, insert_to_table, deduplicate

### should parse execution_date from scheduler instead
# These are used for sub_folder path for saving result data that can be used for partitioning / API query for specific date ( If available )
year = datetime.utcnow().strftime('%Y')
month = datetime.utcnow().strftime('%m')
day = datetime.utcnow().strftime('%d')

def convert_time(value):
    try:
        value = datetime.strptime(value,'%Y-%m-%d')
    except:
        value = None
    return value

def get_historical_dividends(stock_symbol, api_key):
    historical_dividends_url = f'https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{stock_symbol}?apikey={api_key}'
    response = requests.get(url=historical_dividends_url)
    results = json.loads(response.text)
    if results != {}:
        df = pd.json_normalize(results['historical'])
        df.insert(loc=0, column='symbol', value=stock_symbol)
        date_columns = ['date', 'recordDate', 'paymentDate', 'declarationDate']
        for date_col in date_columns:
            df[date_col] = df[date_col].apply(lambda x: convert_time(x))  # Convert from string date to datetime dtype
        df['updated_time'] = datetime.utcnow()  # stamp current time as updated_time
        df.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]  # Convert column name from camelCase to snake_case
        file_path = f'./data/historical_dividends/{year}/{month}/{day}/'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        file_name = f'{stock_symbol}_historical_dividends_data.parquet'
        full_path = file_path + file_name
        df.to_parquet(full_path, index=False)
        print(f'{stock_symbol} Historical Dividends Data Saved Data into {full_path}')
    else:
        print(f'No Historical Data for {stock_symbol}')

def get_delisted_companies(api_key):
    page = 0
    results = []
    delisted_companies_url = f'https://financialmodelingprep.com/api/v3/delisted-companies?page={page}&apikey={api_key}'
    while delisted_companies_url:
        response = requests.get(url=delisted_companies_url)
        delisted_companies_json = json.loads(response.text)
        if len(delisted_companies_json) > 0:
            results.extend(delisted_companies_json)
            print(f'Page: {page} , Total:', len(results))
            page += 1
            delisted_companies_url = f'https://financialmodelingprep.com/api/v3/delisted-companies?page={page}&apikey={api_key}'
        else:
            delisted_companies_url = False
    df = pd.json_normalize(results)
    df['ipoDate'] = df['ipoDate'].apply(lambda x: convert_time(x))  # Convert from string date to datetime dtype
    df['delistedDate'] = df['delistedDate'].apply(lambda x: convert_time(x))
    df['updated_time'] = datetime.utcnow()  # stamp current time as updated_time
    df.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]  # Convert column name from camelCase to snake_case
    file_path = f'./data/delisted_companies/{year}/{month}/{day}/'
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    file_name = f'delisted_companies_data.parquet'
    full_path = file_path + file_name
    df.to_parquet(full_path, index=False)
    print(f'Delisted Companies Data Saved Data into {full_path}')

def main():
    ## Get API Key from function in utils
    api_key = get_api_key()

    ## Get Historical Dividends Data
    companies_list = ['AAPL', 'MSFT', 'AMZN', 'TSLA', 'PEP'] ## Picked These 5 Stocks for examples ( cannot get all the stocks due to the API Calls limit )
    for comp_symbol in companies_list:
        get_historical_dividends(stock_symbol=comp_symbol, api_key=api_key)

    ## Get Delisted Companies Data
    get_delisted_companies(api_key)

    ## Ingest to Local PostgresDB
    tables_and_pks = {'historical_dividends': ['symbol', 'date'],
                      'delisted_companies': ['symbol']
                     }
    conn = get_conn(db_name='dime') # db name in my local postgresDB is 'dime'
    for table, primary_keys in tables_and_pks.items():
        for parquet_file in glob(f'./data/{table}/{year}/{month}/{day}/*.parquet'):
            print(f'Ingesting {parquet_file}')
            create_table(conn=conn, table_name=table)
            df = pd.read_parquet(parquet_file)
            insert_to_table(conn=conn, df=df, schema='fmp',table_name=table)
        deduplicate(conn=conn, schema='fmp', table_name=table, primary_keys=primary_keys)
    conn.close()

if __name__ == '__main__':
    main()