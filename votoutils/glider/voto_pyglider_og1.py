import numpy as np
import polars as pl
import xarray as xr
import logging
from pathlib import Path

from votoutils.glider.process_pyglider_og1 import proc_pyglider_og1
from votoutils.utilities.geocode import get_seas_merged_nav_nc


def set_profile_numbers(ds):
    ds["DIVE_NUM"] = np.around(ds["DIVE_NUM"]).astype(int)
    df = ds.to_pandas()
    df["profile_index"] = 1
    deepest_points = []
    dive_nums = np.unique(df.dive_num)
    for num in dive_nums:
        df_dive = df[df.dive_num == num]
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
    ds["profile_index"] = df.dive_num.copy()
    ds["profile_direction"] = df.dive_num.copy()
    ds["profile_index"].values = df.profile_index
    ds["profile_direction"].values = df.profile_direction
    ds["profile_index"].attrs = dict(long_name="profile index", units="1", sources="PRES, time, dive_num")
    ds["profile_direction"].attrs = {"long_name": "profile direction", "units": "1",
                                     "sources": "PRES, time, dive_num", "comment": "-1 = ascending, 1 = descending"}
    ds["profile_num"] = ds["profile_index"].copy()
    ds["profile_num"].attrs["long_name"] = "profile number"
    return ds


def add_voto_stuff(outname):
    out_path = Path(outname)
    ds = xr.open_dataset(outname)
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
    dataset_type = "nrt" if filename[-1] == 'R' else "delayed"
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
    outname_voto = str(outname).replace('.nc', '_VOTO.nc')
    ds.to_netcdf(outname_voto)


if __name__ == "__main__":
    logf = "/data/log/pyglider_og1.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    glider = "SEA045"
    mission = 79
    nc_out = proc_pyglider_og1(f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv",
                      f"/data/data_l0_pyglider/OG_nrt/{glider}/M{mission}/",
                      f"/data/deployment_yaml/mission_yaml/OG_{glider}_M{str(mission)}.yml", 'sub')
    add_voto_stuff(nc_out)
