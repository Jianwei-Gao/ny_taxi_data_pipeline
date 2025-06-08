import pandas as pd
import numpy as np
import argparse
import os 
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy import text
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.tb
    url = params.url
    data_file = 'output.parquet'
    os.system(f"wget {url} -O {data_file}")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    connection = engine.connect()
        
    pq_file = pq.ParquetFile(data_file)
    iterator = pq_file.iter_batches(batch_size=100000)
    head = next(iterator).to_pandas()
    head.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    for batch in iterator:
        df = batch.to_pandas()
        begin = time()
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        end = time()
        print(f'inserted chunk: took {end-begin:.3f} seconds')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data into Postgres")
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--tb', help='table name for postgres')
    parser.add_argument('--url', help='url for postgres')

    args = parser.parse_args()
    main(args)

