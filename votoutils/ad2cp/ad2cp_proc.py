from gliderad2cp import process_currents, process_shear, process_bias, tools
import xarray as xr
import numpy as np
from pathlib import Path

options = tools.get_options(
    xaxis=1,
    yaxis=3,
    QC_correlation_threshold=80,
    QC_amplitude_threshold=80,
    QC_velocity_threshold=1.5,
    velocity_dependent_shear_bias_correction=False,
    shear_bias_regression_depth_slice=(10, 1000),
)


def adcp_data_present(glider, mission):
    raw_adcp_dir = Path(f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/ADCP")
    adcp_nc = raw_adcp_dir / f"sea{glider}_m{mission}_ad2cp.nc"
    return adcp_nc.exists()


def proc_gliderad2cp(glider, mission):
    data_file = str(glider)
    adcp_file = str(mission)
    data_dir = adcp_file
    ds_adcp = process_shear.process(adcp_file, data_file, options)

    data = xr.open_dataset(data_file)
    gps_predive = []
    gps_postdive = []

    dives = np.round(np.unique(data.dive_num))

    _idx = np.arange(len(data.dead_reckoning.values))
    dr = np.sign(np.gradient(data.dead_reckoning.values))

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
            last = int(np.nanmax(_idx * _post))
            gps_postdive.append(
                np.array(
                    [
                        data.time[last].values,
                        data.longitude[last].values,
                        data.latitude[last].values,
                    ]
                ),
            )

        if any(np.isfinite(_pre)):
            # The first +1 value is when deadreckoning is set to 1, the index before that is the last GPS fix. This is pre-dive.
            first = int(np.nanmin(_idx * _pre)) - 1  # Note the -1 here.
            gps_predive.append(
                np.array(
                    [
                        data.time[first].values,
                        data.longitude[first].values,
                        data.latitude[first].values,
                    ]
                ),
            )

    gps_predive = np.vstack(gps_predive)
    gps_postdive = np.vstack(gps_postdive)
    currents, DAC = process_currents.process(
        ds_adcp, gps_predive, gps_postdive, options
    )
    currents = process_bias.process(currents, options)
    currents.to_netcdf(data_dir / f"SEA0{glider}_M{mission}_adcp_proc.nc")
