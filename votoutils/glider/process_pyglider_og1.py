import os
import sys
import pathlib
import shutil
import yaml
import numpy as np
import polars as pl
import xarray as xr
import datetime
import pandas as pd

from votoutils.glider import grid_glider_data
from votoutils.glider.pre_process import clean_infiles
from votoutils.utilities.geocode import get_seas_merged_nav_nc
#from votoutils.glider.post_process_dataset import post_process
from votoutils.utilities.utilities import encode_times, set_best_dtype
from votoutils.fixers.file_operations import clean_nrt_bad_files
#from votoutils.qc.flag_qartod import flagger

script_dir = pathlib.Path(__file__).parent.parent.parent.absolute()
parent_dir = script_dir.parents[0]
qc_dir = parent_dir / "voto_glider_qc"
sys.path.append(str(qc_dir))
pyglider_dir = parent_dir / "pyglider"
sys.path.append(str(pyglider_dir))
from pyglider import seaexplorer
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

og1_pyglider_var_names = {'CNDC': 'conductivity',
                          'TEMP': 'temperature',
                          'PRES': 'pressure',
                          'LATITUDE': 'latitude',
                          'LONGITUDE': 'longitude',
                          'TIME': 'time',
                          'DEPTH': 'depth'}
def proc_pyglider_l0(platform_serial, mission, kind, input_dir, output_dir):
    og_date_format = "%Y%m%dT%H%M"
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
        f"/data/deployment_yaml/mission_yaml/OG_{platform_serial}_M{str(mission)}.yml"
    )
    deploymentyaml = f"/data/tmp/deployment_yml/{platform_serial}_M{str(mission)}.yml"

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
    dataset_type = "nrt" if kind == "sub" else "delayed"
    dataset_id = (
        f"{dataset_type}_{platform_serial}_M{deployment['metadata']['deployment_id']}"
    )
    deployment["metadata"]["dataset_id"] = dataset_id
    variables = list(deployment["netcdf_variables"].keys())
    if "keep_variables" in variables:
        variables.remove("keep_variables")
    if "timebase" in variables:
        variables.remove("timebase")
    deployment["metadata"]["variables"] = variables
    deployment["metadata"]["glider_serial"] = deployment["metadata"]["platform_serial_number"]
    with open("/data/deployment_yaml/deployment_profile_variables.yml", "r") as fin:
        profile_variables = yaml.safe_load(fin)
    deployment["profile_variables"] = profile_variables
    # temporary convert for pyglider compatibility
    new_var_dict = {}
    for var_name in deployment['netcdf_variables']:
        if var_name in og1_pyglider_var_names.keys():
            new_var_dict[og1_pyglider_var_names[var_name]] =  deployment['netcdf_variables'][var_name]
        else:
            new_var_dict[var_name] = deployment['netcdf_variables'][var_name]
    deployment['netcdf_variables'] = new_var_dict
    with open(deploymentyaml, "w") as fin:
        yaml.dump(deployment, fin)
    # Make level-0 timeseries netcdf file from the raw files
    outname = seaexplorer.raw_to_L0timeseries(
        rawncdir,
        l0tsdir,
        deploymentyaml,
        kind=kind,
    )

    ds = xr.open_dataset(outname)
    #ds = flagger(ds)

    ds = set_profile_numbers(ds)
    #ds = post_process(ds)
    ds = set_best_dtype(ds)
    ds = encode_times(ds)

    # OG1 rename variables back to OG1 names
    for og1_var, pyglider_var in  og1_pyglider_var_names.items():
        if pyglider_var in ds.variables:
            ds[og1_var] = ds[pyglider_var].copy()
            ds = ds.drop_vars([pyglider_var])
    deployment['netcdf_variables'] = new_var_dict

    # OG1 dimensions and coordinates
    
    for var_name in ds.data_vars:
        if ds[var_name].dims[0] == 'time':
            ds[var_name] = (
                "N_MEASUREMENTS",
                ds[var_name].values,
                ds[var_name].attrs,
            )

    ds = ds.set_coords(['TIME', 'LONGITUDE', 'LATITUDE', 'DEPTH'])
    
    
    # OG1 GPS variables and phase
    
    for vname in ["LATITUDE", "LONGITUDE", "TIME"]:
        ds[f"{vname}_GPS"] = ds[vname].copy()
        null_val = np.nan
        if 'TIME' in vname:
            null_val = np.datetime64("NaT")
        ds[f"{vname}_GPS"].values[ds["NAV_STATE"].values != 119] = null_val
        ds[f"{vname}_GPS"].attrs["long_name"] = f"{vname.lower()} of each GPS location"
    ds["LATITUDE_GPS"].attrs["vocabulary"] = (
        "https://vocab.nerc.ac.uk/collection/OG1/current/LAT_GPS/"
    )
    ds["LONGITUDE_GPS"].attrs["vocabulary"] = (
        "https://vocab.nerc.ac.uk/collection/OG1/current/LON_GPS/"
    )
    seaex_phase = ds["NAV_STATE"].values
    standard_phase = np.zeros(len(seaex_phase)).astype(int)
    standard_phase[seaex_phase == 115] = 3
    standard_phase[seaex_phase == 116] = 3
    standard_phase[seaex_phase == 119] = 3
    standard_phase[seaex_phase == 110] = 5
    standard_phase[seaex_phase == 118] = 5
    standard_phase[seaex_phase == 100] = 2
    standard_phase[seaex_phase == 117] = 1
    standard_phase[seaex_phase == 123] = 4
    standard_phase[seaex_phase == 124] = 4
    ds["PHASE"] = xr.DataArray(
        standard_phase,
        coords=ds["LATITUDE"].coords,
        attrs={
            "long_name": "behavior of the glider at sea",
            "phase_vocabulary": "https://github.com/OceanGlidersCommunity/OG-format-user-manual/blob/main/vocabularyCollection/phase.md",
        },
    )
    
    # OG1 dimensionless variables
    ds["TRAJECTORY"] = xr.DataArray(
        ds.attrs["id"],
        attrs={"cf_role": "trajectory_id", "long_name": "trajectory name"},
    )
    ds["WMO_IDENTIFIER"] = xr.DataArray(
        ds.attrs["wmo_id"],
        attrs={"long_name": "wmo id"},
    )
    ds["PLATFORM_MODEL"] = xr.DataArray(
        ds.attrs["platform_model"],
        attrs={
            "long_name": "model of the glider",
            "platform_model_vocabulary": ds.attrs["platform_model_vocabulary"],
        },
    )
    ds["PLATFORM_SERIAL_NUMBER"] = xr.DataArray(
        f"sea{ds.attrs['platform_serial_number'].zfill(3)}",
        attrs={"long_name": "glider serial number"},
    )
    ds["DEPLOYMENT_TIME"] = np.nanmin(ds.TIME.values)
    ds["DEPLOYMENT_TIME"].attrs = {
        "long_name": "date of deployment",
        "standard_name": "time",
        #"units": "seconds since 1970-01-01T00:00:00Z",
       # "calendar": "gregorian",
    }
    ds["DEPLOYMENT_LATITUDE"] = ds.LATITUDE.values[0]
    ds["DEPLOYMENT_LATITUDE"].attrs = {"long_name": "latitude of deployment"}
    ds["DEPLOYMENT_LONGITUDE"] = ds.LONGITUDE.values[0]
    ds["DEPLOYMENT_LONGITUDE"].attrs = {"long_name": "longitude of deployment"}

    # OG1 add sensors
    for sensor_name, sensors_dict in deployment['glider_devices'].items():
        ds[sensor_name] = xr.DataArray(attrs=sensors_dict)
        if sensor_name in ds.attrs:
            ds.attrs.pop(sensor_name)
            
    # OG1 add attributes
    attrs = ds.attrs
    start_datetime = pd.to_datetime(ds.TIME.values, unit="s").min()
    ts = start_datetime.strftime(og_date_format)
    dt_created = datetime.datetime.now().strftime(og_date_format)
    # OG1 reformat existing date strings
    for date_attr in["start_date", "date_created", "time_coverage_start", "time_coverage_end"]:
        if date_attr in attrs.keys():
            dt_in = attrs[date_attr][:19]
            if dt_in[10] != 'T':
                continue
            dt_out = datetime.datetime.strptime(dt_in, "%Y-%m-%dT%H:%M:%S")
            attrs[date_attr] = dt_out.strftime(og_date_format)
    if "delayed" in attrs["dataset_id"]:
        postscript = "delayed"
    else:
        postscript = "R"
    attrs["start_date"] = ts
    attrs["id"] = f"sea{str(attrs['glider_serial']).zfill(3)}_{ts}_{postscript}"
    attrs["date_created"] = dt_created
    attrs["data_url"] = f"https://erddap.observations.voiceoftheocean.org/erddap/tabledap/{attrs['dataset_id']}"
    ds.attrs = attrs

    ds.to_netcdf(outname)
    #if kind=='raw':
    #    from votoutils.ad2cp.ad2cp_proc import adcp_data_present, proc_gliderad2cp
    #    if adcp_data_present(platform_serial, mission):
    #        proc_gliderad2cp(platform_serial, mission)
    grid_glider_data.make_gridfile_gliderad2cp(platform_serial, mission, kind)

if __name__ == '__main__':
    glider = "SEA045"
    mission = 79
    proc_pyglider_l0(glider, mission, 'sub', f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv", f"/data/data_l0_pyglider/OG_nrt/{glider}/M{mission}/")