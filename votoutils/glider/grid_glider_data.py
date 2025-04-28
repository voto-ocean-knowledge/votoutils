import logging
import xarray as xr
import numpy as np
import yaml
import pandas as pd
import scipy.stats as stats
from pathlib import Path
from votoutils.ad2cp.ad2cp_proc import adcp_data_present, proc_gliderad2cp
from gliderad2cp.tools import grid2d
_log = logging.getLogger(__name__)

def _get_deployment(deploymentyaml):
    """
    Take the list of files in *deploymentyaml* and parse them
    for deployment information, with subsequent files overwriting
    previous files.
    """
    if isinstance(deploymentyaml, str):
        deploymentyaml = [deploymentyaml,]
    deployment = {}
    for nn, d in enumerate(deploymentyaml):
        with open(d) as fin:
            deployment_ = yaml.safe_load(fin)
            for k in deployment_:
                deployment[k] = deployment_[k]

    return deployment


def gappy_fill_vertical(data):
    """
    Fill vertical gaps from the first to last bin with data in them.
    Applied column-wise.

    data = gappy_fill_vertical(data)
    """
    m, n = np.shape(data)
    for j in range(n):
        ind = np.where(~np.isnan(data[:, j]))[0]
        if (0 < len(ind) < (ind[-1] - ind[0])
                and len(ind) > (ind[-1] - ind[0]) * 0.05):
            int = np.arange(ind[0], ind[-1])
            data[:, j][ind[0]:ind[-1]] = np.interp(int, ind, data[ind, j])
    return data


def make_gridfile_gliderad2cp(platform_serial, mission, kind):
    """
    Turn a timeseries netCDF file into a vertically gridded netCDF. Adds ad2cp data if present

    Parameters
    ----------
    glider: glider number
    mission: mission number

    Returns
    -------
    outname : str
        Name of gridded netCDF file. The gridded netCDF file has coordinates of
        'depth' and 'profile', so each variable is gridded in depth bins and by
        profile number.  Each profile has a time, latitude, and longitude.
    """


    if kind=='sub':
        infix = 'nrt'
    else:
        infix = 'complete_mission'
    inname = f"/data/data_l0_pyglider/{infix}/{platform_serial}/M{str(mission)}/timeseries/mission_timeseries.nc"
    outdir = Path(f"/data/data_l0_pyglider/{infix}/{platform_serial}/M{str(mission)}/gridfiles/")
    outname = outdir / 'gridded.nc'
    if not outdir.exists():
        outdir.mkdir(parents=True)


    ds = xr.open_dataset(inname, decode_times=True)
    yi = 2
    xi = 1
    xi = np.arange(np.nanmin(ds.profile_num.values), np.nanmax(ds.profile_num.values) + xi, xi)
    yi = np.arange(0, np.nanmax(np.nanmax(ds.depth)) + yi, yi)
    # Create structure
    dsout = xr.Dataset(coords={"depth": yi, "profile": xi})

    dsout["depth"].attrs = {"units": 'm', 'description': 'Central measurement depth in meters.'}
    dsout["profile"].attrs = {"units": '', 'description': 'Central profile number of measurement.'}
    if adcp_data_present(platform_serial, mission):
        adcp_file = Path(f"/data/data_l0_pyglider/complete_mission/{platform_serial}/M{mission}/gliderad2cp/{platform_serial}_M{mission}_adcp_proc.nc")
        if not adcp_file.exists():
            proc_gliderad2cp(platform_serial, mission)
        dsout = xr.open_dataset(adcp_file)
        dsout = dsout.rename_dims({'profile_index': 'profile'})
        dsout['profile'] = dsout['profile_index'].copy()
        dsout = dsout.drop_vars('profile_index')
        xi = dsout.profile.values
        yi = dsout.depth.values
        dsout['time2'] = dsout['time']
        dsout = dsout.drop_vars('time')
        dsout = dsout.rename({'time2': 'time'})

    for var_name in ds.variables:
        if var_name in dsout.variables or var_name in dsout.dims:
            continue
        if "average_method" in ds[var_name].attrs:
            average_method = ds[var_name].attrs["average_method"]
            ds[var_name].attrs["processing"] = (
                f"Using average method {average_method} for "
                f"variable {var_name} following deployment yaml.")
            if average_method == "geometric mean":
                average_method = stats.gmean
                ds[var_name].attrs["processing"] += (" Using geometric mean implementation "
                                              "scipy.stats.gmean")
        else:
            average_method = "median"
        good = np.where(~np.isnan(ds[var_name]) & (ds['profile_index'] % 1 == 0))[0]
        dsout[var_name] = (('depth', 'profile'),
                       grid2d(ds.profile_num.values[good], ds.depth.values[good],
                         ds[var_name].values[good], xi=xi, yi=yi, fn=average_method)[0],
                           ds[var_name].attrs
                      )

    if len(dsout.time.dims) == 2:
        dsout['time'] = ('profile', pd.to_datetime(dsout.time.median(dim='depth').values), dsout.time.attrs)
        dsout = dsout.assign_coords(time=("time", dsout.time.values))

    dsout.attrs = ds.attrs
    dsout.attrs.pop('cdm_data_type')
    # fix to be ISO parsable:
    if len(dsout.attrs['deployment_start']) > 18:
        dsout.attrs['deployment_start'] = dsout.attrs['deployment_start'][:19]
        dsout.attrs['deployment_end'] = dsout.attrs['deployment_end'][:19]
        dsout.attrs['time_coverage_start'] = dsout.attrs['time_coverage_start'][:19]
        dsout.attrs['time_coverage_end'] = dsout.attrs['time_coverage_end'][:19]
    # fix standard_name so they don't overlap!
    try:
        dsout['waypoint_latitude'].attrs.pop('standard_name')
        dsout['waypoint_longitude'].attrs.pop('standard_name')
        dsout['profile_time_start'].attrs.pop('standard_name')
        dsout['profile_time_end'].attrs.pop('standard_name')
    except:
        pass
    # set some attributes for cf guidance
    # see H.6.2. Profiles along a single trajectory
    # https://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/build/aphs06.html
    dsout.attrs['featureType'] = 'trajectoryProfile'
    dsout['profile'].attrs['cf_role'] = 'profile_id'
    dsout['mission_number'] = np.int32(1)
    dsout['mission_number'].attrs['cf_role'] = 'trajectory_id'
    for var_name in dsout:
        if var_name in ['profile', 'depth',  'mission_number']:
            dsout[var_name].attrs['coverage_content_type'] = 'coordinate'
        else:
            dsout[var_name].attrs['coverage_content_type'] = 'physicalMeasurement'

     # Cut gridded data down to actual extent
    _log.info(f'Check depth extent. Originally {dsout.depth.min()} - {dsout.depth.max()} m')
    vmin = 0
    vmax = 0
    for variable in dsout.variables:
        if len(dsout[variable].dims) != 2:
            continue
        if 'bias' in variable:
            continue
        var_sum = np.sum(~np.isnan(dsout[variable].data), axis=1)
        valid_depths = dsout[variable].depth.data[var_sum != 0.0]
        if len(valid_depths) > 0:
            _log.info(f"{variable}: {valid_depths.min()} - {valid_depths.max()} m")
            vmax = max((vmax, valid_depths.max()))
        else:
            _log.info(f"{variable}: has no valid depths")
    _log.info(f'cutting down to {vmin} - {vmax} m depth')
    dsout = dsout.sel(depth=slice(vmin, vmax+2))

    _log.info('Writing %s', outname)
    dsout.to_netcdf(
        outname,
    )
    _log.info('Done gridding')

    return outname
if __name__ == '__main__':
    glider_num = "SEA055"
    mission = 85
    make_gridfile_gliderad2cp(glider_num, mission, 'raw')
