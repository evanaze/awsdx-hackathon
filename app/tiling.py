import os

import h3
import pandas as pd
import dask.dataframe as dd
from shapely.ops import transform
from shapely.geometry import mapping

from logger import get_logger
from app.config import PARQUET_DIR, TILED_CENSUS_DIR, DATA_DIR

LOGGER = get_logger(__name__)


def prepare_districts(gdf):
    """Loads a geojson files of polygon geometries and features,
    swaps the latitude and longitude and stores geojson"""
    return gdf.assign(
        geom_swap_geojson=lambda x: x["geometry"]
        .map(lambda polygon: transform(lambda x, y: (y, x), polygon))
        .apply(lambda y: mapping(y))
    )


def hex_fill_tract(geom_geojson: dict, res: int = 13, flag_swap: bool = False) -> set:
    """Fill a tract with small, res 13 hexagons.

    :param geom_geojson: The polygon to fill.
    :param res: The resolution to fill the polygons with.
    :param flag_swap: A flag indicating whether the polygon is geojson conformant or swapped.
    """
    try:
        set_hexagons = h3.compact(
            h3.polyfill(geom_geojson, res, geo_json_conformant=flag_swap)
        )
    except ValueError:
        LOGGER.debug("Error on data of type %s. Continuing.", geom_geojson["type"])
        return set()
    return list(set_hexagons)


def hex_fill_df(gdf):
    """Fill the tracts with hexagons."""
    return gdf.assign(hex_fill=gdf["geom_swap_geojson"].apply(hex_fill_tract))


def tile_partition(df: pd.DataFrame):
    """Tile a single tract."""
    return df.pipe(prepare_districts).pipe(hex_fill_df)


def tile_geodata():
    """Tile the Geodata."""
    # Get the list of files to read
    infiles = set(os.listdir(PARQUET_DIR))
    donefiles = set(os.listdir(TILED_CENSUS_DIR))
    files_todo = infiles.difference(donefiles)
    LOGGER.info("%s states to tile.", len(files_todo))

    # Process the data in parallel using Dask
    (
        dd.read_parquet([os.path.join(PARQUET_DIR, file) for file in files_todo])
        .map_partitions(tile_partition)
        .to_parquet(os.path.join(DATA_DIR, "all_tiled_tracts.parquet"))
    )


if __name__ == "__main__":
    main()
