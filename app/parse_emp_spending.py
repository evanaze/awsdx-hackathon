import os
import json

import numpy as np
import pandas as pd

from logger import get_logger

DATA_DIR = "data/"
LOGGER = get_logger(__name__)


def make_data_dirs(data_name: str):
    """Make the data directories"""
    data_dir = os.path.join(DATA_DIR, data_name + "_county")
    os.makedirs(data_dir, exist_ok=True)


def load_data_file(filename: str) -> None:
    """Load the datafile from a filename."""
    LOGGER.info("Loading file %s", filename)
    filepath = os.path.join(DATA_DIR, filename + ".json")
    with open(filepath, "r") as infile:
        data = json.load(infile)
    return data


def parse_data(data, data_name):
    """Parses the dataframe and saves to parquet."""
    for series in data["series"]["docs"]:
        if (county := series["dimensions"]["county"]) != "NA":
            LOGGER.debug("Saving data for county %s", county)
            (
                pd.DataFrame(
                    index=series["period"], data=series["value"], columns=["value"]
                )
                .replace("NA", np.nan)
                .astype({"value": float})
                .to_parquet(
                    os.path.join(DATA_DIR, f"{data_name}_county/{county}.parquet"),
                    engine="pyarrow",
                )
            )


def main():
    """Run the main script."""
    for data_name in ["emp_data", "spending"]:
        make_data_dirs(data_name)
        data = load_data_file(data_name)
        parse_data(data, data_name)


if __name__ == "__main__":
    main()
