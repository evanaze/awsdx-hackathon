import duckdb
import pandas as pd

con = duckdb.connect()

# ("SELECT * FROM read_csv_auto('data/opportunity_atlas.csv)'")
tract_to_zip = pd.read_excel("data/ZIP_TRACT_122021.xlsx")
tract_to_zip.to_parquet("data/zip_to_tract_q42021.parquet")
