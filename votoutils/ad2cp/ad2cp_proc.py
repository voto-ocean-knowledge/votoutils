from gliderad2cp import process_currents, process_shear, process_bias, tools
import xarray as xr
import numpy as np
from pathlib import Path
import subprocess
from votoutils.utilities.utilities import missions_no_proc

options = tools.get_options(xaxis=1, yaxis=None, shear_bias_regression_depth_slice=(10,1000))


def adcp_data_present(platform_serial, mission):
    adcp_raw_dir = Path(f"/data/data_raw/complete_mission/{platform_serial}/M{mission}/ADCP")
    adcp_file = adcp_raw_dir / f"{platform_serial}_M{mission}.ad2cp.00000.nc"
    return adcp_file.exists()


def proc_gliderad2cp(platform_serial, mission, reprocess=False):
    adcp_raw_dir = Path(f"/data/data_raw/complete_mission/{platform_serial}/M{mission}/ADCP")
    if not adcp_raw_dir.exists():
        adcp_raw_dir.mkdir(parents=True)
    adcp_file = adcp_raw_dir / f"{platform_serial}_M{mission}.ad2cp.00000.nc"
    if not adcp_file.exists():
        subprocess.check_call(
            [
                "/usr/bin/rsync",
                f'usrerddap@136.243.54.252:/data/ad2cp/{platform_serial}_M{mission}.ad2cp.00000.nc',
                str(adcp_file),
            ],
        )
    data_dir = Path(f"/data/data_l0_pyglider/complete_mission/{platform_serial}/M{mission}")
    data_file = data_dir / "timeseries" / "mission_timeseries.nc"
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
    dead = data.dead_reckoning
    # Keep only data points with valid time, lon and lat
    lon_lat_time = ~np.isnan(data.time) * ~np.isnan(data.longitude) * ~np.isnan(data.latitude)
    dead = dead[lon_lat_time]
    sample_step = ((dead.time.max() - dead.time.min()) / len(dead)) / np.timedelta64(1, 's')
    target_step = 10  # target one sample every 10 seconds for the calculation of DAC. Coarsening speeds this up *a lot*
    subsample = max(1, int(target_step / sample_step))
    print(f"subsampling dead reckoning data by a factor of {subsample} to match target sampling rate of {target_step} seconds during DAC caclulation")
    dead = dead[::subsample]

    # dead reckoning 0 when lon/lat are from GPS fix. 1 when interpolated. Use the gradient of this to find
    # where the glider starts & ends surface GPS fixes
    dead_reckoning_post_change = dead[1:][dead.diff(dim='time') != 0]
    post_dive = dead_reckoning_post_change[dead_reckoning_post_change == 0]

    dead_reckoning_pre_change = dead[:-1][dead.diff(dim='time', label='lower') != 0]
    pre_dive = dead_reckoning_pre_change[dead_reckoning_pre_change == 0]

    # Cut out any predives after the last postdives and postdives before the first predive
    pre_dive = pre_dive[pre_dive.time < post_dive.time.max()]
    post_dive = post_dive[post_dive.time > pre_dive.time.min()]

    gps_predive = np.array([[time, lat, lon] for time, lat, lon in
                   zip(pre_dive.time.values, pre_dive.latitude.values, pre_dive.longitude.values)])
    gps_postdive = np.array([[time, lat, lon] for time, lat, lon in
                    zip(post_dive.time.values, post_dive.latitude.values, post_dive.longitude.values)])
    dive_time_hours = (post_dive.time.values - pre_dive.time.values) / np.timedelta64(1, 'h')
    assert (dive_time_hours > 0).all
    assert 24 > np.mean(dive_time_hours) > 0.5
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
    #proc_gliderad2cp("SEA044", 98)