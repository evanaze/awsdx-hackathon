import os
import multiprocessing as mp
from operator import methodcaller

import h3
from geopandas import read_file
from shapely.ops import transform
from shapely.geometry import mapping

from logger import get_logger

DATA_DIR = "data/zipfiles"
LOGGER = get_logger(__name__)


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


class TileGeoData:
    def __init__(self, filename: str):
        self.gdf = None
        self.filename = filename
        self.state = filename.rstrip(".zip")

    def get_geodata(self):
        self.gdf = read_file(self.filepath)
        self.gdf = (
            self.gdf.astype({"INTPTLAT": float, "INTPTLON": float, "GEOID": "category"})
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

    def prepare_districts(self):
        """Swap the latitude and longitude and store geojson"""
        self.gdf = self.gdf.assign(
            geom_swap_geojson=lambda x: x["geometry"]
            .map(lambda polygon: transform(lambda x, y: (y, x), polygon))
            .apply(lambda y: mapping(y))
        )

    def hex_fill_df(self):
        """Fill the tracts with hexagons."""
        self.gdf = self.gdf.assign(
            hex_fill=self.gdf["geom_swap_geojson"].apply(hex_fill_tract)
        )

    def write_gdf(self):
        """Write the Dataframe to file."""
        LOGGER.info("Finished tiling state %s", self.state)
        outfile = f"data/tiled_states/{self.filename.rstrip('.zip')}.parquet"
        LOGGER.info("Writing state %s to %s", self.state, outfile)
        self.gdf.to_parquet(
            outfile,
            engine="pyarrow",
        )

    def tile_state(self):
        """Tile a single tract."""
        self.filepath = os.path.join(DATA_DIR, self.filename)
        LOGGER.info("Starting to tile state %s", self.state)
        self.get_geodata()
        self.prepare_districts()
        self.hex_fill_df()
        self.write_gdf()


def main():
    """Tile all of the states."""
    zipfiles = os.listdir(DATA_DIR)
    pool = mp.Pool(processes=(mp.cpu_count() - 1))
    tile_objects = [TileGeoData(zipfile) for zipfile in zipfiles]
    results = pool.map(methodcaller("tile_state"), tile_objects)
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
