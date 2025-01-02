import os
import sys
import pathlib
import shutil
import yaml
import numpy as np
import polars as pl
import xarray as xr
from votoutils.glider.pre_process import clean_infiles
from votoutils.utilities.geocode import get_seas_merged_nav_nc
from votoutils.glider.post_process_dataset import post_process
from votoutils.utilities.utilities import encode_times, set_best_dtype
from votoutils.fixers.file_operations import clean_nrt_bad_files
from votoutils.qc.flag_qartod import flagger

script_dir = pathlib.Path(__file__).parent.parent.parent.absolute()
parent_dir = script_dir.parents[0]
qc_dir = parent_dir / "voto_glider_qc"
sys.path.append(str(qc_dir))
pyglider_dir = parent_dir / "pyglider"
sys.path.append(str(pyglider_dir))
# noinspection PyUnresolvedReferences
import pyglider.seaexplorer as seaexplorer  # noqa:E402

# noinspection PyUnresolvedReferences
import pyglider.ncprocess as ncprocess  # noqa: E402

os.chdir(pyglider_dir)


def safe_delete(directories):
    for directory in directories:
        if pathlib.Path.exists(pathlib.Path.absolute(script_dir / directory)):
            shutil.rmtree(directory)


def set_profile_numbers(ds):
    ds["dive_num"] = np.around(ds["dive_num"]).astype(int)
    df = ds.to_pandas()
    df["profile_index"] = 1
    deepest_points = []
    dive_nums = np.unique(df.dive_num)
    for num in dive_nums:
        df_dive = df[df.dive_num == num]
        if np.isnan(df_dive.pressure).all():
            deep_inflect = df_dive.index[int(len(df_dive) / 2)]
        else:
            deep_inflect = df_dive[
                df_dive.pressure == df_dive.pressure.max()
            ].index.values[0]
        deepest_points.append(deep_inflect)

    previous_deep_inflect = deepest_points[0]
    df.loc[df.index[0] : previous_deep_inflect, "profile_index"] = 1
    num = 0
    for i, deep_inflect in enumerate(deepest_points[1:]):
        num = i + 1
        df_deep_to_deep = df.loc[previous_deep_inflect:deep_inflect]
        if np.isnan(df_deep_to_deep.pressure).all():
            shallow_inflect = df_deep_to_deep.index[int(len(df_deep_to_deep) / 2)]
        else:
            shallow_inflect = df_deep_to_deep[
                df_deep_to_deep.pressure == df_deep_to_deep.pressure.min()
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
    ds["profile_index"].attrs = {
        "long_name": "profile index",
        "units": "1",
        "sources": "pressure, time, dive_num",
    }
    ds["profile_direction"].attrs = {
        "long_name": "profile direction",
        "units": "1",
        "sources": "pressure, time, dive_num",
        "comment": "-1 = ascending, 1 = descending",
    }
    ds["profile_num"] = ds["profile_index"].copy()
    ds["profile_num"].attrs["long_name"] = "profile number"
    return ds


def proc_pyglider_l0(glider, mission, kind, input_dir, output_dir):
    if kind not in ["raw", "sub"]:
        raise ValueError("kind must be raw or sub")
    if kind == "sub":
        clean_nrt_bad_files(input_dir)
    rawdir = str(pathlib.Path(input_dir)) + "/"
    output_path = pathlib.Path(output_dir)
    if not output_path.exists():
        output_path.mkdir(parents=True)
    rawncdir = output_dir + "rawnc/"
    l0tsdir = output_dir + "timeseries/"
    profiledir = output_dir + "profiles/"
    griddir = output_dir + "gridfiles/"
    original_deploymentyaml = (
        f"/data/deployment_yaml/mission_yaml/SEA{str(glider)}_M{str(mission)}.yml"
    )
    deploymentyaml = f"/data/tmp/deployment_yml/SEA{str(glider)}_M{str(mission)}.yml"

    safe_delete([rawncdir, l0tsdir, profiledir, griddir])
    clean_infiles(input_dir)
    seaexplorer.raw_to_rawnc(rawdir, rawncdir, original_deploymentyaml)
    # merge individual netcdf files into single netcdf files *.gli*.nc and *.pld1*.nc
    seaexplorer.merge_parquet(rawncdir, rawncdir, original_deploymentyaml, kind=kind)
    # geolocate and add helcom basin info to yaml
    with open(original_deploymentyaml) as fin:
        deployment = yaml.safe_load(fin)
    nav_nc = list(pathlib.Path(rawncdir).glob("*rawgli.parquet"))[0]
    basin = get_seas_merged_nav_nc(nav_nc)
    deployment["metadata"]["basin"] = basin
    # More custom metadata
    df = pl.read_parquet(nav_nc)
    total_dives = df.select("fnum").unique().shape[0]
    deployment["metadata"]["total_dives"] = total_dives
    glider_num_pad = str(deployment["metadata"]["glider_serial"]).zfill(3)
    dataset_type = "nrt" if kind == "sub" else "delayed"
    dataset_id = (
        f"{dataset_type}_SEA{glider_num_pad}_M{deployment['metadata']['deployment_id']}"
    )
    deployment["metadata"]["dataset_id"] = dataset_id
    variables = list(deployment["netcdf_variables"].keys())
    if "keep_variables" in variables:
        variables.remove("keep_variables")
    if "timebase" in variables:
        variables.remove("timebase")
    deployment["metadata"]["variables"] = variables
    with open("/data/deployment_yaml/deployment_profile_variables.yml", "r") as fin:
        profile_variables = yaml.safe_load(fin)
    deployment["profile_variables"] = profile_variables
    with open(deploymentyaml, "w") as fin:
        yaml.dump(deployment, fin)
    # Make level-0 timeseries netcdf file from the raw files
    outname = seaexplorer.raw_to_L0timeseries(
        rawncdir,
        l0tsdir,
        deploymentyaml,
        kind=kind,
    )
    int_vars = [
        "angular_cmd",
        "ballast_cmd",
        "linear_cmd",
        "nav_state",
        "security_level",
        "dive_num",
        "desired_heading",
    ]
    ds = xr.open_dataset(outname)
    ds = flagger(ds)
    ds_variables = list(ds)
    for var in ds_variables:
        if var in int_vars or var[-2:] == "qc":
            ds[var] = np.around(ds[var]).astype(int)
        elif var[-3:] == "raw":
            ds[var] = np.around(ds[var])
    ds = post_process(ds)
    ds = set_best_dtype(ds)
    ds = set_profile_numbers(ds)
    ds = encode_times(ds)
    ds.to_netcdf(outname)

    ncprocess.make_L0_gridfiles(outname, griddir, deploymentyaml)
