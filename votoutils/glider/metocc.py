import xarray as xr
import pandas as pd
import yaml
from pathlib import Path


def create_csv(ds_file):
    # Open timeseries file as a dataset
    timeseries = xr.open_dataset(ds_file)
    # Extract only core variables of interest. Append units to the variable names
    data = {}
    for var in (
        "longitude",
        "latitude",
        "depth",
        "temperature",
        "conductivity",
        "salinity",
    ):
        name = f"{timeseries[var].attrs['standard_name']} ({timeseries[var].attrs['units']})"
        data[name] = timeseries[var].values
    # Create dataframe and add name for index
    df = pd.DataFrame(data, index=timeseries.time)
    df.index.name = "datetime"

    # Extract metadata from dataset. Change datatype to simple float for writing to text file
    meta = timeseries.attrs
    meta["geospatial_lat_max"] = float(meta["geospatial_lat_max"])
    meta["geospatial_lon_max"] = float(meta["geospatial_lon_max"])
    meta["geospatial_lat_min"] = float(meta["geospatial_lat_min"])
    meta["geospatial_lon_min"] = float(meta["geospatial_lon_min"])

    meta_ess = {}

    essential_vars = [
        "deployment_id",
        "acknowledgement",
        "ctd",
        "glider_model",
        "glider_serial",
        "geospatial_lat_max",
        "geospatial_lat_min",
        "geospatial_lat_units",
        "geospatial_lon_max",
        "geospatial_lon_min",
        "geospatial_lon_units",
        "time_coverage_start",
        "time_coverage_end",
        "processing_level",
        "references",
        "sea_name",
        "wmo_id",
    ]

    for key, val in meta.items():
        if key in essential_vars:
            meta_ess[key] = val

    # Create standard filenames
    file_name_base = f"SEA{meta['glider_serial']}_M{meta['deployment_id']}"
    sea_name = meta["sea_name"]
    sea_name_clean = sea_name.replace(",", "_").replace(" ", "")
    if "basin" in meta.keys():
        if len(meta["basin"]) > 5:
            basin = meta["basin"]
            sea_name_clean = basin.split(",")[0].replace(" ", "_")
    dir_name = f"/data/data_l0_pyglider/metocc/{sea_name_clean}"

    # Make output directory and parents if they don't already exist
    output_dir = Path(dir_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write dataframe and metadata to matching files
    df.to_csv(output_dir / f"{file_name_base}.csv")
    with open(output_dir / f"{file_name_base}.yml", "a", encoding="utf-8") as file:
        yaml.dump(meta_ess, file)
    return output_dir / file_name_base
