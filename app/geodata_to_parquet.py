"""Convert GeoData to Parquet files using Geopandas"""
import os

from geopandas import read_file
from shapely.ops import transform
from shapely.geometry import mapping

from app.config import (ZIP_DIR, PARQUET_DIR)


def prepare_districts(gdf):
    """Loads a geojson files of polygon geometries and features,
    swaps the latitude and longitude andstores geojson"""    
    return (gdf
            .assign(geom_swap_geojson = lambda x: x["geometry"].map(lambda polygon: transform(
                       lambda x, y: (y, x), polygon)).apply(lambda y: mapping(y)))
            )

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
            .pipe(prepare_districts)
            .to_parquet(outpath, engine="pyarrow")
        )


if __name__ == "__main__":
    geodata_to_parquet()
