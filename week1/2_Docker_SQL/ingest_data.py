import pandas as pd
from sqlalchemy import create_engine
import argparse
import os



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    url2 = params.url
    table_name_2= params.table_name_2

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    #read the yellow taxi data from NYC TLC site in parquet form
    df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet',
                         engine='pyarrow')
    #process the timestamps
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #read the first row to extract column names for schema
    cols = df.head(0)
    cols.to_sql(name=table_name,con=engine,if_exists='replace')
    #add the data to table
    df.to_sql(name=table_name,
                con=engine,if_exists='append',
                    chunksize=150000)
    #read lookup data
    df_lk = pd.read_csv("https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv")
    df_lk.to_sql(name=table_name_2,con=engine, if_exists='replace')

if __name__ == '__main__':
    #define arg parser
    parser = argparse.ArgumentParser(description='Ingest Data to Postgres')
    
    #arguments needed
    parser.add_argument('--user',help='user name for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table_name',help='table name for postgres')
    parser.add_argument('--url',help='url for csv')

    #args for zones table
    parser.add_argument('--url2',help="url for data")
    parser.add_argument('--table_name_2',help="second table name")
    args = parser.parse_args()

    #pass in the main function
    main(args)