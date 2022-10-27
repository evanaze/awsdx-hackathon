import duckdb
import pandas as pd

con = duckdb.connect()

("SELECT * FROM read_csv_auto('data/opportunity_atlas.csv)'")
