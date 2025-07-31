import geopandas as gp
import pandas as pd
import xarray as xr
import polars as pl
import logging
import pathlib
import argparse
import shutil
import numpy as np
from itertools import chain
from collections import Counter

_log = logging.getLogger(__name__)

comment = (
    "Data points for this variable that fall within Swedish territorial seas have been removed."
    " Territorial seas extents from:"
    "Flanders Marine Institute (2019). Maritime Boundaries Geodatabase: Territorial Seas (12NM),"
    " version 3. Available online at http://www.marineregions.org/. https://doi.org/10.14284/387."
)


def nmea2deg(nmea):
    """
    Convert a NMEA float to a decimal degree float.  e.g. -12640.3232 = -126.6721
    """
    deg = np.fix(nmea / 100) + np.sign(nmea) * np.remainder(np.abs(nmea), 100) / 60
    return deg


def get_seas(gridfile):
    ds = xr.open_dataset(gridfile)
    lon = ds.longitude
    lat = ds.latitude
    return locs_to_seas(lon, lat)


def nan_bad_locations(ds, max_flag=3):
    ds["longitude"][ds["longitude_qc"] > max_flag] = np.nan
    ds["latitude"][ds["latitude_qc"] > max_flag] = np.nan
    return ds


def flag_bad_locations(ds, threshold = 1, sus_threshold=0.6):
    # threshold is max speed in m/s
    lat_to_m = 111 * 1000
    lon_to_m = lat_to_m * np.cos(np.deg2rad(np.nanmean(ds.latitude.values)))
    seconds = ds.time.diff(dim='time').values / np.timedelta64(1, 's')
    speed_x = ds.longitude.diff(dim='time').values * lon_to_m / seconds
    speed_y = ds.latitude.diff(dim='time').values * lat_to_m / seconds
    speed = np.sqrt((speed_x ** 2 + speed_y ** 2))
    for varname in ['longitude', 'latitude']:
        ds[f'{varname}_qc'].values[:-1][speed > sus_threshold] = 3
        ds[f'{varname}_qc'].values[1:][speed > sus_threshold] = 3
        ds[f'{varname}_qc'].values[:-1][speed > threshold] = 4
        ds[f'{varname}_qc'].values[1:][speed > threshold] = 4
        dss = ds[varname].copy()
        dss[ds[f'{varname}_qc']!=1] = np.nan
        ds[f'{varname}_qc'].values[ds[varname].values < np.nanpercentile(dss, 0.1) - 0.1] = 4
        ds[f'{varname}_qc'].values[ds[varname].values > np.nanpercentile(dss, 99.9) + 0.1] = 4
    return ds


def get_seas_merged_nav_nc(navfile):
    df = pl.read_parquet(navfile)
    lon = nmea2deg(df.select("Lon").to_numpy()[:, 0])
    lat = nmea2deg(df.select("Lat").to_numpy()[:, 0])
    return locs_to_seas(lon, lat)


def locs_to_seas(lon, lat):
    df_helcom = gp.read_file("/data/third_party/helcom_plus_skag/helcom_plus_skag.shp")
    df_glider = pd.DataFrame({"lon": lon, "lat": lat})
    df_glider = gp.GeoDataFrame(
        df_glider,
        geometry=gp.points_from_xy(df_glider.lon, df_glider.lat),
    )
    df_glider = df_glider.set_crs(epsg=4326)
    df_glider = df_glider.to_crs(df_helcom.crs)
    polygons_contains = gp.sjoin(df_helcom, df_glider, predicate="contains")
    basin_points = polygons_contains.Name.values
    basin_counts = Counter(basin_points).most_common()
    if not basin_counts:
        return ""
    basins_ordered = [x[0] for x in basin_counts]
    basin_str = ", ".join(basins_ordered)
    return basin_str


def nc_add_sea(nc_path, basin_str, tempfile):
    _log.info(f"working on {nc_path}")
    ds = xr.open_dataset(nc_path)
    meta = ds.attrs
    meta["basin"] = basin_str
    ds.attrs = meta
    ds.to_netcdf(tempfile)
    shutil.move(tempfile, nc_path)


def update_ncs(glider, mission, sub_dir):
    root_dir = f"/data/data_l0_pyglider/{sub_dir}/SEA{str(glider)}/M{str(mission)}"
    _log.info(f"add basin to ncs in {root_dir}")
    nc_files = []
    for sub in ("profiles", "timeseries", "gridfiles"):
        nc_files.append(list(pathlib.Path(root_dir).glob(f"**/{sub}/*.nc")))
    nc_files_flat = list(chain.from_iterable(nc_files))
    if not nc_files:
        _log.error(f"no ncs found in path {root_dir}")
    _log.info(f"Found {len(nc_files_flat)}")
    gridfile_dir = pathlib.Path(
        f"/data/data_l0_pyglider/{sub_dir}/SEA{str(glider)}/M{str(mission)}/gridfiles",
    )
    gridfile = list(gridfile_dir.glob("*.nc"))[0]
    basin = get_seas(gridfile)
    _log.info(f"Basin: {basin}")
    for nc in nc_files_flat:
        temp_nc = pathlib.Path("/data/tmp") / pathlib.Path(nc).name
        nc_add_sea(nc, basin, temp_nc)
    _log.info(f"Updated all ncs in {root_dir}")
    _log.info("Success! added basin to all ncs")


def geocode_by_dives(ds):
    # read in shapefiles
    df_helcom = gp.read_file("/data/third_party/helcom_plus_skag/helcom_plus_skag.shp")
    df_12nm = gp.read_file("/data/third_party/eez_12nm/eez_12nm_filled.geojson")
    # extend the Swedish territorial waters by a buffer lenght
    df_12nm_extend = df_12nm.copy()
    df_12nm_extend = df_12nm_extend.to_crs("epsg:3152")
    # add buffer of 0.5 nm
    buffer_length_in_meters = 0.5 * 1852
    df_12nm_extend["geometry"] = df_12nm_extend.geometry.buffer(buffer_length_in_meters)
    df_12nm_extend_in = df_12nm_extend.to_crs(epsg=4326)
    # Create minimal dataset and group it by dives
    ds = ds[["longitude", "latitude", "dive_num"]]
    df_glider = ds.to_pandas().groupby("dive_num").mean()
    df_glider = gp.GeoDataFrame(
        df_glider,
        geometry=gp.points_from_xy(df_glider.longitude, df_glider.latitude),
    )
    # Align crs and check which dives fall within Swedish 12 nm waters and helcom polygons
    df_glider = df_glider.set_crs(epsg=4326)
    df_helcom = df_helcom.to_crs(df_glider.crs)
    df_helcom = gp.sjoin(df_helcom, df_glider, predicate="contains")
    df_12nm = df_12nm.to_crs(df_glider.crs)
    df_12nm = gp.sjoin(df_12nm, df_glider, predicate="contains")
    df_12nm_extend = df_12nm_extend_in.to_crs(df_glider.crs)
    df_12nm_extend = gp.sjoin(df_12nm_extend, df_glider, predicate="contains")
    df_glider.index.rename("index", inplace=True)
    df_glider["dive_num"] = df_glider.index
    df_12nm_id = df_12nm[["dive_num", "sovereign1"]]
    df_helcom_id = df_helcom[["dive_num", "Name"]]
    df_12nm_extend_id = df_12nm_extend[["dive_num", "sovereign1"]]
    df_12nm_extend_id = df_12nm_extend_id.rename(
        columns={
            "dive_num": "dive_num_extend",
            "sovereign1": "sovereign1_extend",
        },
    )
    # merge the resulting dataframes and check that dives numbers still align
    df_glider = pd.merge(
        df_glider,
        df_12nm_id,
        left_on="dive_num",
        right_on="dive_num",
        how="left",
    )
    df_glider = pd.merge(
        df_glider,
        df_helcom_id,
        left_on="dive_num",
        right_on="dive_num",
        how="left",
    )
    df_glider = pd.merge(
        df_glider,
        df_12nm_extend_id,
        left_on="dive_num",
        right_on="dive_num_extend",
        how="left",
    )
    assert not (df_glider.dive_num - df_glider.dive_num_extend).any()
    # clean up dataframe before returning
    df_glider.drop(["dive_num_extend"], axis=1, inplace=True)
    df_glider.rename(columns={"Name": "basin"}, inplace=True)
    df_glider.loc[df_glider.sovereign1 != "Sweden", "sovereign1"] = (
        "International waters"
    )
    df_glider.loc[df_glider.sovereign1_extend != "Sweden", "sovereign1_extend"] = (
        "International waters"
    )
    return df_glider


def identify_territorial_dives(ds, df_geocode):
    international_dives = df_geocode.loc[
        df_geocode.sovereign1_extend == "International waters",
        "dive_num",
    ]
    good_dives = np.empty((np.size(ds.dive_num.values)), dtype=bool)
    good_dives[:] = False
    good_dives[np.isin(ds.dive_num, international_dives)] = True
    return good_dives


def filter_territorial_data(ds):
    df_geocode = geocode_by_dives(ds)
    good_dives = identify_territorial_dives(ds, df_geocode)
    if all(good_dives):
        _log.info("No dives found within Swedish territorial waters")
        return ds
    else:
        percent_remove = sum(~good_dives) / len(good_dives) * 100
        _log.warning(
            f"Dives found within Swedish territorial seas. Will remove {int(percent_remove)} % of data",
        )
    flag_terms = [
        "adcp",
        "ad2cp",
        "altitude",
        "altimeter",
        "altim",
        "velocity",
        "amplitude",
        "bathy",
        "bathymetry",
        "seafloor",
    ]
    for var_name in list(ds):
        if not any(substring in var_name.lower() for substring in flag_terms):
            continue
        if ds[var_name].dtype == np.dtype("<M8[ns]"):
            _log.warning(
                f"Will not flag territorial seas for {var_name}. dtype is {ds[var_name].dtype}",
            )
            continue
        _log.info(f"Flag territorial seas for {var_name}")
        ds[var_name].values[~good_dives] = np.nan
        ds[var_name].attrs["comment"] = f'{ds[var_name].attrs["comment"]}. {comment}'
    return ds


def filter_adcp_data(ds, good_dives):
    if all(good_dives):
        _log.info("No dives found within Swedish territorial waters")
        return ds
    else:
        percent_remove = sum(~good_dives) / len(good_dives) * 100
        _log.warning(
            f"Dives found within Swedish territorial seas. Will remove {int(percent_remove)} % of data",
        )
    for var_name in list(ds):
        _log.info(f"Flag territorial seas for {var_name}")
        ds[var_name].values[~good_dives, :, :] = np.nan
        ds[var_name].attrs["comment"] = f'{ds[var_name].attrs["comment"]}. {comment}'
    return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="add sub basin data to netcdf files")
    parser.add_argument("glider", type=int, help="glider number, e.g. 70")
    parser.add_argument("mission", type=int, help="Mission number, e.g. 23")
    parser.add_argument(
        "--kind",
        type=str,
        help="Kind of input. Cana specify sub or raw",
    )
    args = parser.parse_args()
    if args.kind not in ["raw", "sub"]:
        raise ValueError("kind must be raw or sub")
    logf = f"/data/log/update_meta/SEA{str(args.glider)}_M{str(args.mission)}.log"
    logging.basicConfig(
        filename=logf,
        filemode="w",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.kind == "sub":
        sub_dir_in = "nrt"
    else:
        sub_dir_in = "complete_mission"
    update_ncs(args.glider, args.mission, sub_dir_in)
