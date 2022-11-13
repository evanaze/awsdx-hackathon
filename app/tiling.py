import os
import logging
import multiprocessing as mp

import h3
from geopandas import read_file
from shapely.ops import transform
from shapely.geometry import mapping

DATA_DIR = "data/zipfiles"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


def get_geodata(filepath: str):
    gdf = read_file(filepath)
    return (
        gdf.astype({"INTPTLAT": float, "INTPTLON": float, "GEOID": "category"})
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
    )


def prepare_districts(gdf_districts):
    """Loads a geojson files of polygon geometries and features,
    swaps the latitude and longitude andstores geojson"""
    return gdf_districts.assign(
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
        LOGGER.warning("Error on data of type %s. Continuing.", geom_geojson["type"])
        return set()
    return list(set_hexagons)


def hex_fill_df(gdf):
    """Fill the tracts with hexagons."""
    return gdf.assign(hex_fill=gdf["geom_swap_geojson"].apply(hex_fill_tract))


def write_gdf(gdf, filename: str):
    """Write the Dataframe to file."""
    gdf.to_parquet(
        f"data/tiled_states/{filename.rstrip('.zip')}.parquet", engine="pyarrow"
    )


def tile_state(zipfile):
    """Tile a single tract."""
    path = os.path.join(DATA_DIR, zipfile)
    LOGGER.info("Starting to tile state with filename %s", zipfile)
    (
        get_geodata(path)
        .pipe(prepare_districts)
        .pipe(hex_fill_df)
        .pipe(write_gdf, filename=zipfile)
    )
    LOGGER.info("Finished tiling state file filename %s", zipfile)


def main():
    """Tile all of the states."""
    zipfiles = os.listdir(DATA_DIR)
    pool = mp.Pool(processes=(mp.cpu_count() - 1))
    results = pool.map(tile_state, zipfiles)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
