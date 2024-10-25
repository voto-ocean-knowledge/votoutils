import xarray as xr
import numpy as np
from pathlib import Path
import argparse
import shutil
import logging
import subprocess
from votoutils.utilities.utilities import encode_times, set_best_dtype
from votoutils.utilities import geocode
from votoutils.upload.sync_functions import sync_script_dir

_log = logging.getLogger(__name__)


def metadata_extr(attrs, glider_attrs):
    ds_id = f"adcp_{glider_attrs['dataset_id'][8:]}"
    title = f"{glider_attrs['title']}_adcp"
    extra_attrs = {
        "dataset_id": ds_id,
        "id": ds_id,
        "title": title,
        "processing_level": "L0 uncorrected ADCP data",
        "source": "relative velocity data from a glider mounted ADCP",
    }
    for key, val in extra_attrs.items():
        attrs[key] = val
    transfer_attrs = [
        "AD2CP",
        "acknowledgement",
        "basin",
        "comment",
        "contributor_name",
        "contributor_role",
        "creator_email",
        "creator_name",
        "creator_url",
        "date_created",
        "date_issued",
        "date_modified",
        "deployment_end",
        "deployment_id",
        "deployment_name",
        "deployment_start",
        "geospatial_lat_max",
        "geospatial_lat_min",
        "geospatial_lat_units",
        "geospatial_lon_max",
        "geospatial_lon_min",
        "geospatial_lon_units",
        "glider_instrument_name",
        "glider_model",
        "glider_name",
        "glider_serial",
        "institution",
        "license",
        "project",
        "project_url",
        "publisher_email",
        "publisher_name",
        "publisher_url",
        "sea_name",
        "summary",
        "time_coverage_end",
        "time_coverage_start",
        "wmo_id",
        "disclaimer",
        "platform",
    ]
    for key, val in glider_attrs.items():
        if key in transfer_attrs:
            attrs[key] = val
    return attrs


def adcp_data_present(glider, mission):
    raw_adcp_dir = Path(f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/ADCP")
    adcp_nc = raw_adcp_dir / f"sea{glider}_m{mission}_ad2cp.nc"
    return adcp_nc.exists()


def get_glider_timeseries(nc):
    ds = xr.open_dataset(nc)
    ts = ds[["time", "ad2cp_time", "longitude", "latitude", "dive_num"]]
    return ts


def recombine_glider_timeseries(nc, adcp_for_ts, outfile):
    ts = xr.open_dataset(nc)
    for var_name in list(ts):
        if var_name == "ad2cp_time":
            ts["adcp_time"] = ts["ad2cp_time"]
        if "ad2cp" in var_name:
            ts = ts.drop_vars(var_name)
    ts = xr.merge((ts, adcp_for_ts))
    ts = set_best_dtype(ts)
    ts = encode_times(ts)
    ts.to_netcdf(outfile)
    ts.close()


def write_3d_adcp(adcp, beam_attrs, output_path, good_dives):
    # rearrange the AD2CP data into 3D DataArrays. Using the native dtype of the data (32-bit float)
    dimensions = {"time": adcp.time, "cell": adcp.range, "beam": (1, 2, 3, 4)}
    for kind in ["Velocity", "Correlation", "Amplitude"]:
        vel = np.empty(
            (*adcp[f"{kind}Beam1"].values.shape, 4),
            dtype=type(adcp[f"{kind}Beam1"].values[0, 0]),
        )
        vel[:] = np.nan
        for beam in (1, 2, 3, 4):
            old_var = adcp[f"{kind}Beam{beam}"]
            vel[:, :, beam - 1] = old_var.values
            adcp = adcp.drop_vars(f"{kind}Beam{beam}")
        da_3d = xr.DataArray(data=vel, dims=dimensions)
        da_3d.attrs = old_var.attrs
        if "comment" not in da_3d.attrs.keys():
            da_3d.attrs["comment"] = ""
        adcp[f"adcp_{kind.lower()}"] = da_3d

    adcp.assign_coords(beam=("beam", np.array((1, 2, 3, 4))))
    adcp.beam.attrs = beam_attrs
    adcp = adcp[["adcp_correlation", "adcp_amplitude", "adcp_velocity"]]
    adcp = geocode.filter_adcp_data(adcp, good_dives)
    adcp = set_best_dtype(adcp)
    adcp = encode_times(adcp)
    if not output_path.exists():
        output_path.mkdir(parents=True)
    adcp.to_netcdf(output_path / "adcp.nc")
    adcp.close()


def drop_unique_vals(adcp):
    # drop unneeded adcp vars:
    unwanted = ["TimeStamp", "MatlabTimeStamp", "Battery"]
    # All adcp variables with a constant value converted to attributes
    for var_name in list(adcp):
        if var_name in unwanted:
            adcp = adcp.drop_vars(var_name)
            continue
        var = adcp[var_name].values
        uniques = np.unique(var)
        if len(uniques) == 1:
            adcp.attrs[var_name] = uniques[0]
            adcp = adcp.drop_vars(var_name)
    return adcp


def proc_ad2cp_mission(glider, mission):
    # open datasets
    _log.info(f"Start ADCP for SEA{glider} M{mission}")
    raw_adcp_dir = Path(f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/ADCP")
    adcp_nc = raw_adcp_dir / f"sea{glider}_m{mission}_ad2cp.nc"
    proc_dir = Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/")
    nc = proc_dir / "timeseries/mission_timeseries.nc"
    ts = get_glider_timeseries(nc)
    if "ad2cp_time" not in list(ts):
        _log.warning("timeseries has already been processed with adcp data")
        return
    adcp = xr.open_dataset(adcp_nc)
    adcp.attrs = metadata_extr(adcp.attrs, ts.attrs)
    adcp = drop_unique_vals(adcp)

    # test that beams stay the same throughout the dataset
    unique_beams = np.unique(adcp.Physicalbeam.values, axis=0)
    beam_attrs = adcp.Physicalbeam.attrs
    if unique_beams.shape[0] == 1:
        adcp.attrs["physical_beam"] = list(unique_beams[0])
        adcp = adcp.drop_vars("Physicalbeam")
    else:
        raise ValueError("physical beams are not consistent throughout mission! Abort")

    # align timestamps of adcp data to those of the glider
    ts_adcp_time = ts.ad2cp_time[~np.isnan(ts.ad2cp_time.values)]
    ts_time = ts.time[~np.isnan(ts.ad2cp_time.values)]
    adcp = adcp.reindex(time=ts_adcp_time, method="nearest")
    adcp = adcp.assign_coords(time=ts_time)
    # TODO tests here that the alignment of timestamps hasn't screwed things up

    # Standardise the dimension names of the variables that are on a range
    for var_name in list(adcp):
        var = adcp[var_name]
        if "Range" in str(var.dims):
            var_fixed = var.rename({var.dims[1]: "range"})
            adcp = adcp.drop_vars(var_name)
            adcp[var_name] = var_fixed
    adcp = adcp.reset_index(
        ["Amplitude Range", "Correlation Range", "Velocity Range"],
        drop=True,
    )
    # Keep only 3D variables in the ad2cp dataset (time, cell, beam). All others transferred to the glider dataset
    for_ts = []
    for var_name in list(adcp):
        if "Beam" not in var_name:
            for_ts.append(var_name)
    adcp_for_ts = adcp[for_ts]
    adcp = adcp.drop_vars(for_ts)
    for var_name in list(adcp_for_ts):
        adcp_for_ts[f"adcp_{var_name}"] = adcp_for_ts[var_name]
        adcp_for_ts = adcp_for_ts.drop_vars(var_name)
    outfile = proc_dir / "timeseries/mission_timeseries_with_adcp.nc"
    recombine_glider_timeseries(nc, adcp_for_ts, outfile)
    output_path = proc_dir / "ADCP"
    ts_min = ts[["dive_num"]]
    adcp_min = adcp_for_ts[["adcp_Status"]]
    ts_out = xr.merge((adcp_min, ts_min), join="left")
    df_geocode = geocode.geocode_by_dives(ts_out)
    good_dives = geocode.identify_territorial_dives(ts_out, df_geocode)
    write_3d_adcp(adcp, beam_attrs, output_path, good_dives)

    shutil.move(str(proc_dir / "timeseries/mission_timeseries_with_adcp.nc"), nc)
    _log.info(f"processed ADCP for SEA{glider} M{mission}")
    subprocess.check_call(
        [
            "/usr/bin/bash",
            sync_script_dir / "send_to_erddap_adcp.sh",
            str(glider),
            str(mission),
        ],
    )
    _log.info(f"sent SEA{glider} M{mission} to ERDDAP")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine ADCP data with glider data")
    parser.add_argument("glider", type=int, help="glider number, e.g. 70")
    parser.add_argument("mission", type=int, help="Mission number, e.g. 23")
    args = parser.parse_args()
    logf = f"/data/log/complete_mission/adcp_SEA{str(args.glider)}_M{str(args.mission)}.log"
    logging.basicConfig(
        filename=logf,
        filemode="w",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    proc_ad2cp_mission(args.glider, args.mission)
