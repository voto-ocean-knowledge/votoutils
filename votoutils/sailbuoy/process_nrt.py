import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import logging
import xarray as xr
from votoutils.utilities.geocode import locs_to_seas
from votoutils.sailbuoy.sailbuoy_functions import get_attrs, clean_names_nrt, add_sensors
from votoutils.utilities import vocabularies


_log = logging.getLogger(__name__)



def all_nrt_sailbuoys(full_dir, all_missions=False):
    _log.info(f"adding complete missions from {full_dir}")
    navs = list(full_dir.glob("*nav.csv"))
    plds = list(full_dir.glob("*pld.csv"))
    navs.sort()
    plds.sort()
    for nav, pld in zip(navs, plds):
        if nav.name[:6] != pld.name[:6]:
            raise ValueError(
                f"nav and pld filenames do not match {nav.name} {pld.name}"
            )
        sb_num = int(nav.name[2:6])
        _log.info(f"process {sb_num}")
        sb_mission_num = split_nrt_sailbuoy(nav, pld, sb_num, all_missions)
        pld_2 = Path(str(pld).replace("pld", "pld_2"))
        if pld_2.exists():
            _log.info(f"process {sb_num} pld2")
            split_nrt_sailbuoy(
                nav, pld_2, sb_num, all_missions, mission_num=sb_mission_num
            )
    _log.info("Finished processing nrt sailbuoy data")


def remove_test_missions(df):
    in_gbg = np.logical_and(
        np.logical_and(df.Lat > 57.6, df.Lat < 57.8),
        np.logical_and(df.Long > 11.7, df.Long < 12.1),
    )
    df = df[~in_gbg]
    df = df[df.Time < np.datetime64("2035-01-01")]
    df = df.sort_values("Time")
    df.index = np.arange(len(df))
    return df


def split_nrt_sailbuoy(
    nav,
    pld,
    sb_num,
    all_missions,
    max_nocomm_time=datetime.timedelta(hours=3),
    min_mission_time=datetime.timedelta(days=3),
    mission_num=1,
):
    try:
        df_nav = pd.read_csv(nav, sep="\t", parse_dates=["Time"])
        df_pld = pd.read_csv(pld, sep="\t", parse_dates=["Time"])
    except (pd.errors.ParserError, ValueError):
        _log.error(f"could not read one of {nav}, {pld}")
        return mission_num
    if len(df_nav) == 0 or len(df_pld) == 0:
        return mission_num
    df_combi = pd.merge_asof(
        df_pld,
        df_nav,
        on="Time",
        direction="nearest",
        tolerance=datetime.timedelta(minutes=30),
        suffixes=("_pld", ""),
    )
    df_combi = remove_test_missions(df_combi)
    df_combi["time_diff"] = df_combi.Time.diff()
    df_combi.index = np.arange(len(df_combi))
    start_i = 0
    for i, dt in zip(df_combi.index, df_combi.time_diff):
        if dt > max_nocomm_time:
            df_mission = df_combi[start_i:i]
            if sb_num == 2120:
                df_mission = df_mission[~np.logical_and(df_mission.Lat < 55.015, df_mission.Long < 13.23552)]

            df_clean = clean_sailbuoy_df(df_mission)
            if len(df_clean) == 0:
                _log.warning(f"no good data in SB{sb_num} mission, skipping")
                continue
            start_i = i
            if (
                df_mission.Time.iloc[-1] - df_mission.Time.iloc[0] > min_mission_time
                and len(df_mission) > 50
            ):
                if all_missions:
                    make_sailbuoy_ds(df_mission, sb_num, mission_num)
                mission_num += 1
    df_mission = df_combi[start_i:]
    long_mission = df_mission.Time.iloc[-1] - df_mission.Time.iloc[0] > min_mission_time
    now = datetime.datetime.now()
    live_mission = now - df_mission.Time.iloc[-1] < datetime.timedelta(hours=6)
    if long_mission and all_missions or live_mission:
        make_sailbuoy_ds(df_mission, sb_num, mission_num)
        make_sailbuoy_ds_legacy(df_mission, sb_num, mission_num)
    return mission_num


def make_sailbuoy_ds(df, sb, mission):
    ds = xr.Dataset()
    time_attr = {"name": "time"}
    ds["time"] = ("time", df.Time, time_attr)

    for col_name in list(df):
        if col_name in clean_names_nrt.keys():
            name = clean_names_nrt[col_name]
            ds[name] = ("time", df[col_name], vocabularies.vocab_attrs[name])
        else:
            print(f"fail for {col_name}")

    # cut dataset down to active deployed period
    # todo
    ds = get_attrs(ds, f'SB{sb}', postscript='R')
    ds = add_sensors(ds)
    ds.attrs["variables"] = list(ds.variables)
    ds["trajectory"] = xr.DataArray(1, attrs={"cf_role": "trajectory_id"})
    directory = Path(f"/data/sailbuoy/nrt/{ds.attrs['platform_serial']}")
    if not directory.exists():
        directory.mkdir(parents=True)
    ds.to_netcdf(directory / f"{ds.attrs['id']}.nc")


def make_sailbuoy_ds_legacy(df, sb, mission):
    df =  df.rename({'Time': 'time'}, axis=1)
    df.index = df.time
    ds = df.to_xarray()
    ds["longitude"] = ds.Long
    ds["latitude"] = ds.Lat
    drops = [ 'Unnamed: 45','Time', 'time_diff']
    to_drop = set(drops).intersection(list(ds))
    basin_str = locs_to_seas(ds.latitude, ds.longitude)
    ds = ds.drop_vars(to_drop)
    attrs = {
        "geospatial_lon_min": df.Lat.min(),
        "geospatial_lon_max": df.Lat.max(),
        "geospatial_lat_min": df.Long.min(),
        "geospatial_lat_max": df.Long.max(),
        "sea_name": "Baltic",
        "wmo_id": "0",
        "project": "SAMBA",
        "project_url": "https://voiceoftheocean.org/samba-smart-autonomous-monitoring-of-the-baltic-sea/",
        "platform_serial": f"SB{sb}",
        "deployment_id": mission,
        "basin": basin_str,
    }
    ds.attrs = attrs
    ds.to_netcdf(f"/data/sailbuoy/nrt_proc/SB{sb}_M{mission}.nc")


def clean_sailbuoy_df(df, speed_limit=4, max_distance=5000):
    df.index = np.arange(len(df))
    df = df[df.Time > datetime.datetime(2015, 1, 1)]
    if len(df) < 5:
        return pd.DataFrame()
    speed_rolling = df.Velocity.rolling(window=3).mean()

    if speed_rolling.max() > speed_limit:
        mid_index = int(df.index.max() / 2)
        bad_starts = speed_rolling[:mid_index].index[
            speed_rolling[:mid_index] > speed_limit
        ]
        bad_ends = speed_rolling[mid_index:].index[
            speed_rolling[mid_index:] > speed_limit
        ]
        start = 0
        end = len(df)
        if len(bad_starts) > 0:
            start = min((max(bad_starts), 50))
        if len(bad_ends) > 0:
            end = max((min(bad_ends)), len(df) - 50)
        df = df[start:end]
    distance_travelled = (
        np.abs(df["Lat_pld"].diff()) * 111000 + np.abs(df["Long_pld"].diff() * 111000)
    ) * np.cos(np.deg2rad(np.nanmean(df["Lat_pld"])))
    if np.nanmax(distance_travelled) > max_distance:
        mid_index = int(df.index.max() / 2)
        bad_starts = distance_travelled[:mid_index].index[
            distance_travelled[:mid_index] > max_distance
        ]
        bad_ends = distance_travelled[mid_index:].index[
            distance_travelled[mid_index:] > max_distance
        ]
        start = 0
        end = len(df)
        if len(bad_starts) > 0:
            start = min((max(bad_starts), 50))
        if len(bad_ends) > 0:
            end = max((min(bad_ends)), len(df) - 50)
        df = df[start:end]
    return df



if __name__ == '__main__':
    logging.basicConfig(
        filename=f"/data/log/sailbuoy_nrt.log",
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    all_nrt_sailbuoys(Path("/data/sailbuoy/raw"), all_missions=True)
