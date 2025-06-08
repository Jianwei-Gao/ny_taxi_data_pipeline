import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time

def ingest_callable(user, password, host, port, db, table_name, file):
    print(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    connection = engine.connect()
    print("Connection established, inserting data")
    pq_file = pq.ParquetFile(file)
    iterator = pq_file.iter_batches(batch_size=100000)
    head = next(iterator).to_pandas()
    head.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    for batch in iterator:
        df = batch.to_pandas()
        begin = time()
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        end = time()
        print(f'inserted chunk: took {end-begin:.3f} seconds')

