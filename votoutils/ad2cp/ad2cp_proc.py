from gliderad2cp import process_currents, process_shear, process_bias, tools
import xarray as xr
import numpy as np
from pathlib import Path
import pandas as pd
import subprocess
from votoutils.upload.sync_functions import sync_script_dir
from votoutils.utilities.utilities import missions_no_proc
import logging
_log = logging.getLogger(__name__)

options = tools.get_options(xaxis=1, yaxis=None, shear_bias_regression_depth_slice=(10, 1000))


def adcp_data_present(platform_serial, mission):
    adcp_raw_dir = Path(f"/data/data_raw/complete_mission/{platform_serial}/M{mission}/ADCP")
    adcp_file = adcp_raw_dir / f"{platform_serial}_M{mission}.ad2cp.00000.nc"
    return adcp_file.exists()


def getGeoMagStrength(ADCP):
    lat = np.nanmedian(ADCP.latitude.values)
    lon = np.nanmedian(ADCP.longitude.values)
    date = pd.to_datetime(np.nanmedian(ADCP.time.values.astype(float)))
    year = date.year
    month = date.month
    day = date.day
    url = str('https://geomag.bgs.ac.uk/web_service/GMModels/igrf/14/?'+
          'latitude='+str(lat)+'&longitude='+str(lon)+
          '&date='+str(year)+'-'+str(month)+'-'+str(day)+
          '&resultFormat=xml')
    import urllib
    import xml.etree.ElementTree as ET
    with urllib.request.urlopen(url) as resp:
        xml_bytes = resp.read()
    root = ET.fromstring(xml_bytes)
    total_intensity = float(root.find('field-value/total-intensity').text) * 1e-9 * 10000 * 1000 # To tesla, then to gauss then to millgauss
    declination = float(root.find('field-value/declination').text)
    return total_intensity, declination


def remove_territorial_waters_adcp(infile_path, gliderfile_path, outfile_path, pressure_margin=30):
    """
    Removing AD2CP data from near the seafloor in territorial waters before uploading it to ERDDAP
    :param infile_path:
    :return:
    """
    ADCP = xr.open_dataset(infile_path, group='Data/Average')
    ADCP = ADCP.drop_vars(["MatlabTimeStamp"])
    config = xr.open_dataset(infile_path, group='Config')
    glider_data = xr.open_dataset(gliderfile_path)
    df_glider = pd.DataFrame(
        {'altimeter': glider_data.altimeter, 'dive_num': glider_data.dive_num.astype(int), 'pressure': glider_data.pressure},
        index=glider_data.time)
    df_adcp = pd.DataFrame({'adcp_pressure': ADCP.Pressure}, index=ADCP.time)
    df_adcp_alt = pd.merge_asof(df_adcp, df_glider, left_index=True, right_index=True)
    df_adcp_alt.loc[np.isnan(df_adcp_alt.dive_num.values), 'dive_num'] = 0
    df_adcp_alt['dive_num'] = df_adcp_alt['dive_num'].astype(int)
    df_glider_by_dive = df_glider.groupby('dive_num').max().rename(
        {'altimeter': 'max_altimeter', 'pressure': 'max_pressure'}, axis=1)
    df_adcp_alt = df_adcp_alt.sort_values("dive_num")
    df_adcp_bool = pd.merge_asof(df_adcp_alt, df_glider_by_dive, left_on='dive_num', right_index=True)
    df_adcp_bool['territorial_waters'] = True
    df_adcp_bool.loc[~np.isnan(df_adcp_bool.max_altimeter), 'territorial_waters'] = False
    df_adcp_bool['near_seabed'] = True
    df_adcp_bool.loc[df_adcp_bool.pressure < (df_adcp_bool.max_pressure - pressure_margin), 'near_seabed'] = False
    df_adcp_bool['territorial_near_seabed'] = np.logical_and(df_adcp_bool['territorial_waters'],
                                                             df_adcp_bool['near_seabed'])
    if sum(df_adcp_bool['territorial_near_seabed']):
        percent_remove = sum(df_adcp_bool['territorial_near_seabed']) / len(df_adcp_bool) * 100
        for var_name in ADCP.variables:
            if 'Beam' not in var_name:
                continue
            data = ADCP[var_name].values
            if data.shape[0] != len(df_adcp_bool):
                continue
            _log.info(
                f"Dives found within Swedish territorial seas. Will remove {int(percent_remove)} % of data from {var_name}")

            data[df_adcp_bool['territorial_near_seabed'], :] = np.nan
            ADCP[var_name].attrs[
                'comment'] = (f'Post-conversion, processing was performed to remove collected from within the deepest'
                              f' {pressure_margin} dbar of the glider dive when the glider was within territorial waters.'
                              f' This processing removed {round(percent_remove, 1)} % of data points. Contact callum.rollo@voiceoftheocean.org for more information')
            ADCP[var_name].values = data
    else:
        _log.info("no data in territorial water. Copying as-is")
    ADCP.to_netcdf(outfile_path, "w", group="Data/Average", format="NETCDF4")
    config.to_netcdf(outfile_path, "a", group="Config", format="NETCDF4")


def proc_gliderad2cp(platform_serial, mission, reprocess=False):
    adcp_raw_dir = Path(f"/data/data_raw/complete_mission/{platform_serial}/M{mission}/ADCP")
    adcp_fn = f"{platform_serial}_M{mission}.ad2cp.00000.nc"
    adcp_file = adcp_raw_dir / adcp_fn
    data_dir = Path(f"/data/data_l0_pyglider/complete_mission/{platform_serial}/M{mission}")
    data_file = data_dir / "timeseries" / "mission_timeseries.nc"
    outdir_filtered = data_dir / "ad2cp_filtered"
    if not outdir_filtered.exists():
        outdir_filtered.mkdir()
    outfile_filtered = outdir_filtered / adcp_fn
    if reprocess or not outfile_filtered.exists():
        remove_territorial_waters_adcp(adcp_file, data_file, outfile_filtered)
        subprocess.check_call(
            [
                "/usr/bin/bash",
                str(sync_script_dir / "upload_adcp_erddap.sh"),
                str(platform_serial),
                str(mission),
                str(outfile_filtered),
            ],
        )
        print(f"sent {adcp_fn} filtered adcp file to ERDDAP")

    out_dir = data_dir / "gliderad2cp"
    if not out_dir.exists():
        out_dir.mkdir(parents=True)
    outfile = out_dir / f"{platform_serial}_M{mission}_adcp_proc.nc"
    if outfile.exists() and not reprocess:
        print(f"outfile {outfile} already exists. Exiting")
        return
    print(f"will process {adcp_file}")
    ds_adcp = process_shear.process(str(adcp_file), data_file, options)

    data = xr.open_dataset(data_file)
    # Correct magnetic declination
    df_glider = data['heading'].to_pandas()
    df_adcp = ds_adcp['Heading'].to_pandas()
    intentisy, declination = getGeoMagStrength(data)
    df_comp = pd.merge_asof(df_glider, df_adcp, left_index=True, right_index=True,
                            tolerance=pd.Timedelta("1s")).dropna().rename(
        {'heading': 'heading_glider', 'Heading': 'heading_ad2cp'}, axis=1)
    df_comp['heading_ad2cp_corr'] = df_comp.heading_ad2cp + declination

    df_comp['difference'] = df_comp.heading_glider - df_comp.heading_ad2cp
    df_comp['difference_corr'] = df_comp.heading_glider - df_comp.heading_ad2cp_corr

    for var_name in ['difference', 'difference_corr']:
        df_comp[var_name][df_comp[var_name] > 180] = df_comp[var_name][df_comp[var_name] > 180] - 360
        df_comp[var_name][df_comp[var_name] < -180] = df_comp[var_name][df_comp[var_name] < -180] + 360
    if abs(np.nanmedian(df_comp.difference_corr)) < 2 and abs(np.nanmedian(df_comp.difference)) > 4:
        _log.warning(
            f"will correct adcp data for heading error from {abs(np.nanmedian(df_comp.difference))} to {abs(np.nanmedian(df_comp.difference_corr))} using declination {declination}")
        ds_adcp.Heading.values += declination
    else:
        _log.warning(
            f"Heading error {abs(np.nanmedian(df_comp.difference))} too small to correct using declination {declination}")

    # Calculate DAC
    data = data.to_pandas()
    data['time'] = data.index
    lon_lat_time = ~np.isnan(data.index) * ~np.isnan(data.longitude) * ~np.isnan(data.latitude)
    data = data[lon_lat_time]
    gps_predive = []
    gps_postdive = []

    dives = np.round(np.unique(data.dive_num))

    _idx = np.arange(len(data.dead_reckoning.values))
    dr  = np.sign(np.gradient(data.dead_reckoning.values))

    for dn in dives:
        _gd = data.dive_num.values == dn
        if all(np.unique(dr[_gd]) == 0):
            continue

        _post = -dr.copy()
        _post[_post != 1] = np.nan
        _post[~_gd] = np.nan

        _pre = dr.copy()
        _pre[_pre != 1] = np.nan
        _pre[~_gd] = np.nan

        if any(np.isfinite(_post)):
            # The last -1 value is when deadreckoning is set to 0, ie. GPS fix. This is post-dive.
            last  = int(np.nanmax(_idx * _post))
            gps_postdive.append(np.array([data.time[last], data.longitude[last], data.latitude[last]]))

        if any(np.isfinite(_pre)):
            # The first +1 value is when deadreckoning is set to 1, the index before that is the last GPS fix. This is pre-dive.
            first = int(np.nanmin(_idx * _pre))-1 # Note the -1 here.
            gps_predive.append(np.array([data.time[first], data.longitude[first], data.latitude[first]]))

    gps_predive = np.vstack(gps_predive)
    gps_postdive = np.vstack(gps_postdive)

    currents, DAC = process_currents.process(
        ds_adcp, gps_predive, gps_postdive, options
    )
    currents = process_bias.process(currents, options)
    currents.to_netcdf(outfile)


def proc_all_ad2cp():
    in_paths = list(Path("/data/data_raw/complete_mission").glob("S*/M*"))
    print(in_paths)
    for inpath in in_paths:
        parts = inpath.parts
        platform_serial = parts[-2]
        mission = int(parts[-1][1:])
        if [platform_serial, mission] in missions_no_proc:
            print(f"{platform_serial} M{mission} in mission_no_proc. Skipping")
            continue
        if adcp_data_present(platform_serial, mission):
            proc_gliderad2cp(platform_serial, mission)

if __name__ == '__main__':
     proc_all_ad2cp()
    #proc_gliderad2cp("SEA045", 100)
