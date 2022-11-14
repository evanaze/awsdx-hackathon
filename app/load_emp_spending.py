import os
import json
from logging import getLogger

import pandas as pd

DATA_DIR = "data/"
LOGGER = getLogger(__name__)


def load_data_file(filename: str) -> None:
    """Load the datafile from a filename."""
    LOGGER.info("Loading file %s", filename)
    with open(filename, "r") as infile:
        data = json.load(infile)
    return data


def main():
    """Run the main script."""
    for data_name in ["emp_data", "spending"]:
        data_file = os.path.join(data_name, "spending.json")
        data = load_data_file(data_file)

        for series in data["series"]["docs"]:
            if (county := series["dimensions"]["county"]) != "NA":
                df = pd.DataFrame(
                    index=series["period"], data=series["value"], columns=["value"]
                )
                df.to_parquet(
                    os.path.join(DATA_DIR, f"{data_name}_county/{county}.parquet"),
                    engine="pyarrow",
                )


if __name__ == "__main__":
    main()
