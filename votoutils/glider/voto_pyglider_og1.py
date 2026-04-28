import numpy as np
import polars as pl
import xarray as xr
import logging
from pathlib import Path

from votoutils.glider.process_pyglider_og1 import proc_pyglider_og1
from votoutils.utilities.geocode import get_seas_merged_nav_nc
from votoutils.utilities.utilities import encode_times, encode_times_og1


def set_profile_numbers(ds):
    ds["DIVE_NUMBER"] = np.around(ds["DIVE_NUMBER"]).astype(int)
    df = ds.to_pandas()
    df["profile_index"] = 1
    deepest_points = []
    dive_nums = np.unique(df.DIVE_NUMBER)
    for num in dive_nums:
        df_dive = df[df.DIVE_NUMBER == num]
        if np.isnan(df_dive.PRES).all():
            deep_inflect = df_dive.index[int(len(df_dive) / 2)]
        else:
            deep_inflect = df_dive[
                df_dive.PRES== df_dive.PRES.max()
            ].index.values[0]
        deepest_points.append(deep_inflect)

    previous_deep_inflect = deepest_points[0]
    df.loc[df.index[0] : previous_deep_inflect, "profile_index"] = 1
    num = 0
    for i, deep_inflect in enumerate(deepest_points[1:]):
        num = i + 1
        df_deep_to_deep = df.loc[previous_deep_inflect:deep_inflect]
        if np.isnan(df_deep_to_deep.PRES).all():
            shallow_inflect = df_deep_to_deep.index[int(len(df_deep_to_deep) / 2)]
        else:
            shallow_inflect = df_deep_to_deep[
                df_deep_to_deep.PRES== df_deep_to_deep.PRES.min()
            ].index.values[0]
        df.loc[previous_deep_inflect:shallow_inflect, "profile_index"] = num * 2
        df.loc[shallow_inflect:deep_inflect, "profile_index"] = num * 2 + 1
        previous_deep_inflect = deep_inflect
    df.loc[previous_deep_inflect : df.index[-1], "profile_index"] = num * 2 + 2

    df["profile_direction"] = 1
    df.loc[df.profile_index % 2 == 0, "profile_direction"] = -1
    ds["PROFILE_NUMBER"] = df.DIVE_NUMBER.copy()
    ds["PROFILE_DIRECTION"] = df.DIVE_NUMBER.copy()
    ds["PROFILE_NUMBER"].values = df.profile_index
    ds["PROFILE_DIRECTION"].values = df.profile_direction
    ds["PROFILE_NUMBER"].attrs = dict(long_name="profile number", units="1", sources="PRES, TIME, DIVE_NUMBER")
    ds["PROFILE_DIRECTION"].attrs = {"long_name": "profile direction", "units": "1",
                                     "sources": "PRES, TIME, DIVE_NUMBER", "comment": "-1 = ascending, 1 = descending"}
    return ds


def add_voto_stuff(outname):
    out_path = Path(outname)
    coder = xr.coders.CFDatetimeCoder(time_unit="s")
    ds = xr.open_dataset(outname, decode_times=coder)
    attrs = ds.attrs
    # OG1 VOTO specific
    timeseries_dir = out_path.parent
    rawncdir = timeseries_dir.parent / 'rawnc'
    nav_nc = list(rawncdir.glob("*rawgli.parquet"))[0]
    ds = set_profile_numbers(ds)
    basin = get_seas_merged_nav_nc(nav_nc)
    attrs["basin"] = basin
    # More custom metadata
    df = pl.read_parquet(nav_nc)
    total_dives = df.select("fnum").unique().shape[0]
    attrs["total_dives"] = total_dives
    filename = Path(outname).name.split('.')[0]
    dataset_type = "nrt" if 'nrt' in str(outname) else "delayed"
    glider_serial = ds.attrs['platform_serial_number']
    deployment_id = ds.attrs['deployment_id']
    dataset_id = (
        f"{dataset_type}_{glider_serial}_M{deployment_id}"
    )
    attrs["dataset_id"] = dataset_id
    attrs["data_url"] = f"https://erddap.observations.voiceoftheocean.org/erddap/tabledap/{attrs['dataset_id']}"
    attrs["variables"] = list(ds.variables)
    attrs["glider_serial"] = glider_serial
    ds.attrs = attrs
    outname_voto = f"{dataset_id}.nc"
    outpath_voto = out_path.parent / outname_voto
    drop_vars = {'profile_index', 'profile_direction'}.intersection(set(ds.data_vars))
    ds = ds.drop_vars(drop_vars)
    ds = encode_times(ds)
    ds.to_netcdf(outpath_voto)


def proc_one():
    logf = "/data/log/pyglider_og1.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    glider="SEA045"
    mission = 37
    kind='nrt'
    if kind =='raw':
        nc_out = proc_pyglider_og1(f"/data/data_raw/complete_mission/{glider}/M{mission}/",
                                   f"/data/data_l0_pyglider/OG_complete_mission/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'raw', reprocess=True)
    else:

        nc_out = proc_pyglider_og1(f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv",
                                   f"/data/data_l0_pyglider/OG_nrt/{glider}/M{mission}/",
                                   #f"/data/deployment_yaml/mission_yaml/OG_{glider}_M{str(mission)}.yml",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'sub', reprocess=True)
    if not nc_out:
        return
    add_voto_stuff(nc_out)

def proc_all_nrt():
    logf = "/data/log/pyglider_og1.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    all_yamls = list(Path("/data/deployment_yaml/og1").glob("*.yaml"))
    mission_yamls = [yml for yml in all_yamls if "pyglider_mod" not in str(yml)]
    mission_yamls.sort()

    for yml_file in mission_yamls:
        fn = yml_file.name
        print(fn)
        glider, mission = fn.split(".")[0].split('_M')
        nc_out = proc_pyglider_og1(f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv",
                                   f"/data/data_l0_pyglider/OG_nrt/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'sub')
        if not nc_out:
            continue
        add_voto_stuff(nc_out)
        print(nc_out)


def proc_all_delayed():
    logf = "/data/log/pyglider_og1.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    all_yamls = list(Path("/data/deployment_yaml/og1").glob("*.yaml"))
    mission_yamls = [yml for yml in all_yamls if "pyglider_mod" not in str(yml)]
    mission_yamls.sort()

    for yml_file in mission_yamls:
        fn = yml_file.name
        print(fn)
        glider, mission = fn.split(".")[0].split('_M')
        nc_out = proc_pyglider_og1(f"/data/data_raw/complete_mission/{glider}/M{mission}",
                                   f"/data/data_l0_pyglider/OG_delayed/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'raw')
        if not nc_out:
            continue
        add_voto_stuff(nc_out)
        print(nc_out)

if __name__ == "__main__":
    #proc_one()
    #add_voto_stuff("/data/data_l0_pyglider/OG_nrt/SEA069/M48/timeseries/mission_timeseries_VOTO.nc")
    #proc_all_nrt()
    proc_all_delayed()
