import re
import numpy as np
import pandas as pd
import logging
import subprocess
import datetime
from votoutils.upload.sync_functions import sync_script_dir

_log = logging.getLogger(__name__)


def natural_sort(unsorted_list):
    convert = lambda text: int(text) if text.isdigit() else text.lower()  # noqa: E731
    alphanum_key = lambda key: [convert(c) for c in re.split("([0-9]+)", key)]  # noqa: E731
    return sorted(unsorted_list, key=alphanum_key)


def match_input_files(gli_infiles, pld_infiles):
    gli_nums = []
    for fname in gli_infiles:
        parts = fname.split(".")
        try:
            gli_nums.append(int(parts[-1]))
        except ValueError:
            try:
                gli_nums.append(int(parts[-2]))
            except ValueError:
                raise ValueError(
                    "Unexpected gli file filename found in input. Aborting",
                )
    pld_nums = []
    for fname in pld_infiles:
        parts = fname.split(".")
        try:
            pld_nums.append(int(parts[-1]))
        except ValueError:
            try:
                pld_nums.append(int(parts[-2]))
            except ValueError:
                raise ValueError(
                    "Unexpected pld file filename found in input. Aborting",
                )
    good_cycles = set(pld_nums) & set(gli_nums)
    good_gli_files = []
    good_pld_files = []
    for i, num in enumerate(gli_nums):
        if num in good_cycles:
            good_gli_files.append(gli_infiles[i])
    for i, num in enumerate(pld_nums):
        if num in good_cycles:
            good_pld_files.append(pld_infiles[i])
    return good_gli_files, good_pld_files


def encode_times(ds):
    if "units" in ds.time.attrs.keys():
        ds.time.attrs.pop("units")
    if "calendar" in ds.time.attrs.keys():
        ds.time.attrs.pop("calendar")
    ds["time"].encoding["units"] = "seconds since 1970-01-01T00:00:00Z"
    for var_name in list(ds):
        if "time" in var_name.lower() and not var_name == "time":
            for drop_attr in ["units", "calendar", "dtype"]:
                if drop_attr in ds[var_name].attrs.keys():
                    ds[var_name].attrs.pop(drop_attr)
            ds[var_name].encoding["units"] = "seconds since 1970-01-01T00:00:00Z"
    return ds


def encode_times_og1(ds):
    for var_name in ds.variables:
        if "axis" in ds[var_name].attrs.keys():
            ds[var_name].attrs.pop("axis")
        if "time" in var_name.lower():
            for drop_attr in ["units", "calendar", "dtype"]:
                if drop_attr in ds[var_name].attrs.keys():
                    ds[var_name].attrs.pop(drop_attr)
                if drop_attr in ds[var_name].encoding.keys():
                    ds[var_name].encoding.pop(drop_attr)
            if var_name.lower() == "time":
                ds[var_name].attrs["units"] = "seconds since 1970-01-01T00:00:00Z"
                ds[var_name].attrs["calendar"] = "gregorian"
    return ds


def find_best_dtype(var_name, da):
    input_dtype = da.dtype.type
    if "latitude" in var_name.lower() or "longitude" in var_name.lower():
        return np.double
    if var_name[-2:].lower() == "qc":
        return np.int8
    if "time" in var_name.lower():
        return input_dtype
    if var_name[-3:] == "raw" or "int" in str(input_dtype):
        if np.nanmax(da.values) < 2**16 / 2:
            return np.int16
        elif np.nanmax(da.values) < 2**32 / 2:
            return np.int32
    if input_dtype == np.float64:
        return np.float32
    return input_dtype


def set_fill_value(new_dtype):
    fill_val = 2 ** (int(re.findall("\d+", str(new_dtype))[0]) - 1) - 1
    return fill_val


def set_best_dtype(ds):
    bytes_in = ds.nbytes
    for var_name in list(ds):
        da = ds[var_name]
        input_dtype = da.dtype.type
        new_dtype = find_best_dtype(var_name, da)
        for att in ["valid_min", "valid_max"]:
            if att in da.attrs.keys():
                da.attrs[att] = np.array(da.attrs[att]).astype(new_dtype)
        if new_dtype == input_dtype:
            continue
        _log.debug(f"{var_name} input dtype {input_dtype} change to {new_dtype}")
        da_new = da.astype(new_dtype)
        ds = ds.drop_vars(var_name)
        if "int" in str(new_dtype):
            fill_val = set_fill_value(new_dtype)
            da_new[np.isnan(da)] = fill_val
            da_new.encoding["_FillValue"] = fill_val
        ds[var_name] = da_new
    bytes_out = ds.nbytes
    _log.info(
        f"Space saved by dtype downgrade: {int(100 * (bytes_in - bytes_out) / bytes_in)} %",
    )
    return ds


def sensor_sampling_period(glider, mission):
    # Get sampling period of CTD in seconds for a given glider mission
    fn = f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/sea{str(glider).zfill(3)}.{mission}.pld1.raw.10.gz"
    df = pd.read_csv(fn, sep=";", dayfirst=True, parse_dates=["PLD_REALTIMECLOCK"])
    if "LEGATO_TEMPERATURE" in list(df):
        df_ctd = df.dropna(subset=["LEGATO_TEMPERATURE"])
    else:
        df_ctd = df.dropna(subset=["GPCTD_TEMPERATURE"])
    ctd_seconds = df_ctd["PLD_REALTIMECLOCK"].diff().median().microseconds / 1e6

    if "AROD_FT_DO" in list(df):
        df_oxy = df.dropna(subset=["AROD_FT_DO"])
    else:
        df_oxy = df.dropna(subset=["LEGATO_CODA_DO"])
    oxy_seconds = df_oxy["PLD_REALTIMECLOCK"].diff().median().microseconds / 1e6
    sample_dict = {
        "glider": glider,
        "mission": mission,
        "ctd_period": ctd_seconds,
        "oxy_period": oxy_seconds,
    }
    return sample_dict


def mailer(subject, message, recipient="callum.rollo@voiceoftheocean.org"):
    if "callum" in str(sync_script_dir):
        _log.error(f"Mock mail {subject}: {message} to {recipient}")
        return
    _log.warning(f"email: {subject}, {message}, {recipient}")
    subject = subject.replace(" ", "-")
    send_script = sync_script_dir / "mailer.sh"
    subprocess.check_call(["/usr/bin/bash", send_script, message, subject, recipient])


def add_standard_global_attrs(ds):
    date_created = datetime.datetime.now().isoformat().split(".")[0]
    attrs = {
        "acknowledgement": "This study used data collected and made freely available by Voice of the Ocean Foundation ("
        "https://voiceoftheocean.org)",
        "conventions": "CF-1.11",
        "institution_country": "SWE",
        "creator_email": "callum.rollo@voiceoftheocean.org",
        "creator_name": "Callum Rollo",
        "creator_type": "Person",
        "creator_url": "https://observations.voiceoftheocean.org",
        "date_created": date_created,
        "date_issued": date_created,
        "geospatial_lat_max": np.nanmax(ds.LATITUDE),
        "geospatial_lat_min": np.nanmin(ds.LATITUDE),
        "geospatial_lat_units": "degrees_north",
        "geospatial_lon_max": np.nanmax(ds.LONGITUDE),
        "geospatial_lon_min": np.nanmin(ds.LONGITUDE),
        "geospatial_lon_units": "degrees_east",
        "contributor_email": "callum.rollo@voiceoftheocean.org, louise.biddle@voiceoftheocean.org, , , , , , ",
        "contributor_role_vocabulary": "https://vocab.nerc.ac.uk/collection/W08/",
        "contributor_role": "Data scientist, PI, Operator, Operator, Operator, Operator, Operator, Operator,",
        "contributing_institutions": "Voice of the Ocean Foundation",
        "contributing_institutions_role": "Operator",
        "contributing_institutions_role_vocabulary": "https://vocab.nerc.ac.uk/collection/W08/current/",
        "agency": "Voice of the Ocean",
        "agency_role": "contact point",
        "agency_role_vocabulary": "https://vocab.nerc.ac.uk/collection/C86/current/",
        "infoUrl": "https://observations.voiceoftheocean.org",
        "inspire": "ISO 19115",
        "institution": "Voice of the Ocean Foundation",
        "institution_edmo_code": "5579",
        "keywords": "CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, "
        "Conductivity, Density, Salinity",
        "keywords_vocabulary": "GCMD Science Keywords",
        "licence": "Creative Commons Attribution 4.0 (https://creativecommons.org/licenses/by/4.0/) This study used data collected and made freely available by Voice of the Ocean Foundation (https://voiceoftheocean.org) accessed from https://erddap.observations.voiceoftheocean.org/erddap/index.html",
        "disclaimer": "Data, products and services from VOTO are provided 'as is' without any warranty as to fitness for "
        "a particular purpose.",
        "references": "Voice of the Ocean Foundation",
        "source": "Voice of the Ocean Foundation",
        "sourceUrl": "https://observations.voiceoftheocean.org",
        "standard_name_vocabulary": "CF Standard Name Table v70",
        "time_coverage_end": str(np.nanmax(ds.time)).split(".")[0],
        "time_coverage_start": str(np.nanmin(ds.time)).split(".")[0],
        "variables": list(ds),
    }
    for key, val in attrs.items():
        if key in ds.attrs.keys():
            continue
        ds.attrs[key] = val
    return ds
