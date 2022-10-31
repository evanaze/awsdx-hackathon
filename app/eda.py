import duckdb
import pandas as pd

con = duckdb.connect()

opp_atlas = con.execute(
    "SELECT * FROM read_csv_auto('data/opportunity_atlas.csv)'"
).df()
