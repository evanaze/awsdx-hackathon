import duckdb
import pandas as pd

con = duckdb.connect()

# Read in part of the
chunksize = 10**6
with pd.read_csv("data/opportunity_atlas.csv", chunksize=chunksize) as reader:
    data_sample = pd.DataFrame(reader[0])

print(data_sample.head())
