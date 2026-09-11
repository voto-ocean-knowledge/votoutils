import yaml
import numpy as np
import polars as pl
import xarray as xr
import datetime
import logging
from pathlib import Path
from pyglider import seaexplorer
from packaging.version import parse as parse_version
from votoutils.utilities.utilities import encode_times_og1
from votoutils.glider.pre_process import clean_infiles
from votoutils.glider.process_pyglider import safe_delete
_log = logging.getLogger(__name__)


def convert_seaexplorer_phase(ds):
    if "NAV_RESOURCE" not in ds.variables:
        return ds
    seaex_phase = ds["NAV_RESOURCE"].values
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
            "long_name": "Identifier of glider phase",
            "vocabulary": "http://vocab.nerc.ac.uk/collection/OG1/current/PHASE/",
            "phase_vocabulary": "https://github.com/OceanGlidersCommunity/OG-format-user-manual/blob/main/vocabularyCollection/phase.md",
        },
    )
    return ds

def drop_derived_variables(ds):
    drop_vars = {'density', 'potential_density', 'potential_temperature', 'salinity', 'distance_over_ground'}.intersection(set(ds.data_vars))
    ds = ds.drop_vars(drop_vars)
    return ds

def add_extra_delayed_vars(deployment):
    if 'seanav' not in deployment['metadata'].keys():
        return deployment
    nav_string = deployment['metadata']['seanav'].split('-')[0].replace(' ', '')
    nav_version = parse_version(nav_string)
    if nav_version < parse_version('3.9.1'):
        return deployment
    extras = {
        'ACC_X': {'source': 'AccX',
                  'long_name': 'Acceleration measured along the X-axis',
                  'units': 'ms-2', },
        'ACC_Y': {'source': 'AccY',
                  'long_name': 'Acceleration measured along the Y-axis',
                  'units': 'ms-2',},
        'ACC_Z': {'source': 'AccZ',
                  'long_name': 'Acceleration measured along the Z-axis',
                  'units': 'ms-2',},
        'MAG_X': {'source': 'MagX',
                  'long_name': 'Magnetic field component along the X-axis',
                  'units': 'ms-2', },
        'MAG_Y': {'source': 'MagY',
                  'long_name': 'Magnetic field component along the Y-axis',
                  'units': 'ms-2', },
        'MAG_Z': {'source': 'MagZ',
                  'long_name': 'Magnetic field component along the Z-axis',
                  'units': 'ms-2', },
         'DISTORTION': {'source': 'Distortion',
                        'long_name': 'Measurement of magnetic distortion',
                        'units': 'None',}
    }
    for key, val in extras.items():
        extras[key]['coordinates'] = 'TIME, LONGITUDE, LATITUDE, DEPTH'
        extras[key]['observation_type'] = 'measured'
        extras[key]['sensor'] = 'SeaExplorer'
    deployment['netcdf_variables'] =  deployment['netcdf_variables'] | extras
    return deployment

def proc_pyglider_og1(input_dir, output_dir, yaml_file, kind, reprocess=False):
    og_date_format = "%Y%m%dT%H%M"
    og1_pyglider_var_names = {'CNDC': 'conductivity',
                              'TEMP': 'temperature',
                              'PRES': 'pressure',
                              'LATITUDE': 'latitude',
                              'LONGITUDE': 'longitude',
                              'TIME': 'time',
                              'DEPTH': 'depth'
                              }

    if kind not in ["raw", "sub"]:
        raise ValueError("kind must be raw or sub")
    rawdir = str(Path(input_dir)) + "/"
    if not Path(input_dir).exists():
        print(f"No input found in {input_dir}")
        return None
    output_path = Path(output_dir)
    if not output_path.exists():
        output_path.mkdir(parents=True)
    rawncdir = output_dir + "rawnc/"
    l0tsdir = output_dir + "timeseries/"
    original_deploymentyaml = yaml_file
    if not reprocess and Path(l0tsdir).exists():
        _log.info(f"Will not reprocess {input_dir}")
        return
    if reprocess:
        safe_delete([l0tsdir, rawncdir])
    safe_delete([l0tsdir])
    clean_infiles(input_dir)

    # temporarily convert some variable names for pyglider compatibility
    with open(original_deploymentyaml) as fin:
        deployment = yaml.safe_load(fin)
    deployment_original = deployment.copy()

    new_var_dict = {}
    for var_name in deployment['netcdf_variables']:
        if var_name in og1_pyglider_var_names.keys():
            new_var_dict[og1_pyglider_var_names[var_name]] =  deployment['netcdf_variables'][var_name]
        else:
            new_var_dict[var_name] = deployment['netcdf_variables'][var_name]
    deployment['netcdf_variables'] = new_var_dict
    if kind == 'raw':
        deployment = add_extra_delayed_vars(deployment)

    deploymentyaml = str(Path(original_deploymentyaml).parent / Path(original_deploymentyaml).name.replace('.', '_pyglider_mod.'))

    with open(deploymentyaml, "w") as fin:
        yaml.dump(deployment, fin)

    seaexplorer.raw_to_rawnc(rawdir, rawncdir, deploymentyaml)
    # merge individual netcdf files into single netcdf files *.gli*.nc and *.pld1*.nc
    seaexplorer.merge_parquet(rawncdir, rawncdir, deploymentyaml, kind=kind)

    # OG1 add some metadata attributes for OG1-pyglider compatability
    postscript = 'R'
    if kind == 'raw':
        postscript = "delayed"

    nav_file = list(Path(rawncdir).glob('*rawgli*'))[0]
    nav_df = pl.read_parquet(nav_file)
    start_datetime = nav_df['time'].nan_min()
    ts = start_datetime.strftime(og_date_format)
    glider_serial = deployment["metadata"]["platform_serial_number"]
    deployment['metadata']["glider_serial"] = glider_serial
    deployment['metadata']["deployment_name"] = f"{glider_serial.lower()}_{ts}_{postscript}"

    with open(deploymentyaml, "w") as fin:
        yaml.dump(deployment, fin)


    # Make level-0 timeseries netcdf file from the raw files
    outname = seaexplorer.raw_to_L0timeseries(
        rawncdir,
        l0tsdir,
        deploymentyaml,
        kind=kind,
    )

    # Open output netCDF for some OG1 specific post-processing
    ds = xr.open_dataset(outname)

    # OG1 rename variables back to OG1 names
    ds = drop_derived_variables(ds)
    for og1_var, pyglider_var in  og1_pyglider_var_names.items():
        if pyglider_var in ds.variables:
            ds[og1_var] = ds[pyglider_var].copy()
            ds = ds.drop_vars([pyglider_var])

    # OG1 dimensions and coordinates
    
    for var_name in ds.data_vars:
        if ds[var_name].dims[0] == 'time':
            ds[var_name] = (
                "N_MEASUREMENTS",
                ds[var_name].values,
                ds[var_name].attrs,
            )

    ds = ds.set_coords(['TIME', 'LONGITUDE', 'LATITUDE', 'DEPTH'])
    ds.TIME.values = ds.TIME.values.astype('datetime64[s]') # ERDDAP cannot take nanoseconds
    ds.TIME.encoding['calendar'] = deployment_original['netcdf_variables']['TIME']['calendar']

    # OG1 GPS variables and phase
    
    for vname in ["LATITUDE", "LONGITUDE", "TIME"]:
        ds[f"{vname}_GPS"] = ds[vname].copy()
        null_val = np.nan
        if 'TIME' in vname:
            null_val = np.datetime64("NaT")
        if 'DEAD_RECKONING' in list(ds):
            non_surface_mask = ds['DEAD_RECKONING'].values != 0
        else:
            non_surface_mask = ~np.logical_or(ds["NAV_RESOURCE"].values == 116, ds["NAV_RESOURCE"].values == 119)
        ds[f"{vname}_GPS"].values[non_surface_mask] = null_val
        ds[f"{vname}_GPS"].attrs["long_name"] = f"{vname.lower()} of each GPS location"
    ds["LATITUDE_GPS"].attrs["vocabulary"] = (
        "http://vocab.nerc.ac.uk/collection/OG1/current/LAT_GPS/"
    )
    ds["LONGITUDE_GPS"].attrs["vocabulary"] = (
        "http://vocab.nerc.ac.uk/collection/OG1/current/LON_GPS/"
    )
    ds = convert_seaexplorer_phase(ds)

    ds.attrs['id'] = ds.attrs['deployment_name']
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
    ds["DEPLOYMENT_TIME"] = xr.DataArray(np.nanmin(ds.TIME.values), attrs = {
        "long_name": "date of deployment",
        "standard_name": "time",})
    ds.DEPLOYMENT_TIME.encoding['calendar'] = deployment_original['netcdf_variables']['TIME']['calendar']

    ds["DEPLOYMENT_LATITUDE"] = xr.DataArray(ds.LATITUDE.values[~np.isnan(ds.LATITUDE)][0],
                                              attrs = {"long_name": "latitude of deployment"})
    ds["DEPLOYMENT_LONGITUDE"] = xr.DataArray(ds.LONGITUDE.values[~np.isnan(ds.LONGITUDE)][0],
                                              attrs = {"long_name": "longitude of deployment"})

    # OG1 add sensors
    for sensor_name, sensors_dict in deployment['glider_devices'].items():
        ds[sensor_name] = xr.DataArray(attrs=sensors_dict)
        if sensor_name in ds.attrs:
            ds.attrs.pop(sensor_name)
            
    # OG1 add attributes
    dt_created = datetime.datetime.now().strftime(og_date_format)
    # OG1 reformat existing date strings
    for date_attr in["start_date", "date_created", "time_coverage_start", "time_coverage_end"]:
        if date_attr in ds.attrs.keys():
            dt_in = ds.attrs[date_attr][:19]
            if dt_in[10] != 'T':
                continue
            dt_out = datetime.datetime.strptime(dt_in, "%Y-%m-%dT%H:%M:%S")
            ds.attrs[date_attr] = dt_out.strftime(og_date_format)
    ds.attrs["start_date"] = ts
    ds.attrs["date_created"] = dt_created
    ds.attrs['Conventions'] = deployment_original['metadata']['Conventions']
    ds.to_netcdf(outname)
    return outname


if __name__ == "__main__":
    logf = "/data/log/pyglider_og1.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    glider = "SHW003"
    mission = 14
    kind='raw'
    if kind == 'raw':
        nc_out = proc_pyglider_og1(f"/data/data_raw/complete_mission/{glider}/M{mission}",
                                   f"/data/data_l0_pyglider/OG_complete_mission/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'raw',
                                   reprocess=True)
    else:
        nc_out = proc_pyglider_og1(f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv",
                                   f"/data/data_l0_pyglider/OG_nrt/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'sub',
                                   reprocess=True)
