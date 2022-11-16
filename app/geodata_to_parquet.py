"""Convert GeoData to Parquet files using Geopandas"""
import os
from geopandas import read_file

DATA_DIR = "data"
ZIP_DIR = os.path.join(DATA_DIR, "zipfiles")
PARQUET_DIR = os.path.join(DATA_DIR, "tract_parquet")


def geodata_to_parquet() -> None:
    """Read the zipfiles and save to Parquet"""
    zipfiles = os.listdir(ZIP_DIR)
    for zipfile in zipfiles:
        inpath = os.path.join(ZIP_DIR, zipfile)
        filename = zipfile[:-4]
        outpath = os.path.join(PARQUET_DIR, filename + ".parquet")
        (
            read_file(inpath)
            .astype({"INTPTLAT": float, "INTPTLON": float, "GEOID": "category"})
            .drop(
                [
                    "STATEFP",
                    "COUNTYFP",
                    "TRACTCE",
                    "ALAND",
                    "AWATER",
                    "NAME",
                    "NAMELSAD",
                    "MTFCC",
                    "FUNCSTAT",
                ],
                axis=1,
            )
            .rename({"INTPTLAT": "lat", "INTPTLON": "lon", "GEOID": "geoid"}, axis=1)
            .to_parquet(outpath, engine="pyarrow")
        )


if __name__ == "__main__":
    geodata_to_parquet()
