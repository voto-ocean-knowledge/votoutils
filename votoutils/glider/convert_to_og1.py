import datetime
import pandas as pd
import numpy as np
import xarray as xr
from votoutils.utilities.utilities import encode_times_og1, set_best_dtype
from votoutils.utilities import vocabularies
import logging

_log = logging.getLogger(__name__)

variables_sensors = {
    "CNDC": "CTD",
    "DOXY": "dissolved gas sensors",
    "PRES": "CTD",
    "PSAL": "CTD",
    "TEMP": "CTD",
    "DEPTH": "CTD",
    "BBP700": "fluorometers",
    "CHLA": "fluorometers",
    "PRES_ADCP": "ADVs and turbulence probes",
}


def add_sensors(ds, dsa):
    attrs = ds.attrs
    sensors = []
    for key, var in attrs.items():
        if not isinstance(var, str):
            continue
        if "{" not in var:
            continue
        if isinstance(eval(var), dict):
            sensors.append(key)

    sensor_name_type = {}
    for instr in sensors:
        if instr in ["altimeter"]:
            continue
        attr_dict = eval(attrs[instr])
        if attr_dict["make_model"] not in vocabularies.sensor_vocabs.keys():
            _log.error(f"sensor {attr_dict['make_model']} not found")
            continue
        var_dict = vocabularies.sensor_vocabs[attr_dict["make_model"]]
        if "serial" in attr_dict.keys():
            var_dict["serial_number"] = str(attr_dict["serial"])
            var_dict["long_name"] += f":{str(attr_dict['serial'])}"
        for var_name in ["calibration_date", "calibration_parameters"]:
            if var_name in attr_dict.keys():
                var_dict[var_name] = str(attr_dict[var_name])
        da = xr.DataArray(attrs=var_dict)
        sensor_var_name = f"sensor_{var_dict['sensor_type']}_{var_dict['serial_number']}".upper().replace(
            " ",
            "_",
        )
        dsa[sensor_var_name] = da
        sensor_name_type[var_dict["sensor_type"]] = sensor_var_name

    for key, var in attrs.copy().items():
        if not isinstance(var, str):
            continue
        if "{" not in var:
            continue
        if isinstance(eval(var), dict):
            attrs.pop(key)
    ds.attrs = attrs
    for key, sensor_type in variables_sensors.items():
        if key in dsa.variables:
            instr_key = sensor_name_type[sensor_type]
            dsa[key].attrs["sensor"] = instr_key

    return ds, dsa


def convert_to_og1(ds, num_vals=None):
    """
    Based on example by Jen Seva https://github.com/OceanGlidersCommunity/OG-format-user-manual/pull/136/files
    Using variable names from OG1 vocab https://vocab.nerc.ac.uk/collection/OG1/current/
    Using units from collection P07 http://vocab.nerc.ac.uk/collection/P07/current/
    e.g. mass_concentration_of_chlorophyll_a_in_sea_water from https://vocab.nerc.ac.uk/collection/P07/current/CF14N7/
    :param ds: dataset to convert
    :param num_vals: optional argument to subset input dataset to first num_values values default=None for no subset
    :return: converted dataset
    """
    dsa = xr.Dataset()
    for var_name in list(ds) + list(ds.coords):
        if "_QC" in var_name:
            continue
        dsa[var_name] = (
            "N_MEASUREMENTS",
            ds[var_name].values[:num_vals],
            ds[var_name].attrs,
        )
        qc_name = f"{var_name}_QC"
        if qc_name in list(ds):
            dsa[qc_name] = (
                "N_MEASUREMENTS",
                ds[qc_name].values[:num_vals].astype("int8"),
                ds[qc_name].attrs,
            )
            dsa[qc_name].attrs["long_name"] = (
                f'{dsa[var_name].attrs["long_name"]} Quality Flag'
            )
            dsa[qc_name].attrs["standard_name"] = "status_flag"
            dsa[qc_name].attrs["flag_values"] = np.array((1, 2, 3, 4, 9)).astype("int8")
            dsa[qc_name].attrs["flag_meanings"] = "GOOD UNKNOWN SUSPECT FAIL MISSING"
            dsa[var_name].attrs["ancillary_variables"] = qc_name
    if "time" in str(dsa.TIME.dtype):
        var_name = "TIME"
        dsa[var_name].values = dsa[var_name].values.astype(float)
        if np.nanmean(dsa[var_name].values) > 1e12:
            dsa[var_name].values = dsa[var_name].values / 1e9
    dsa = dsa.set_coords(("TIME", "LATITUDE", "LONGITUDE", "DEPTH"))
    for vname in ["LATITUDE", "LONGITUDE", "TIME"]:
        dsa[f"{vname}_GPS"] = dsa[vname].copy()
        dsa[f"{vname}_GPS"].values[dsa["NAV_STATE"].values != 119] = np.nan
        dsa[f"{vname}_GPS"].attrs["long_name"] = f"{vname.lower()} of each GPS location"
    dsa["LATITUDE_GPS"].attrs["vocabulary"] = (
        "https://vocab.nerc.ac.uk/collection/OG1/current/LAT_GPS/"
    )
    dsa["LONGITUDE_GPS"].attrs["vocabulary"] = (
        "https://vocab.nerc.ac.uk/collection/OG1/current/LON_GPS/"
    )
    seaex_phase = dsa["NAV_STATE"].values
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
    dsa["PHASE"] = xr.DataArray(
        standard_phase,
        coords=dsa["LATITUDE"].coords,
        attrs={
            "long_name": "behavior of the glider at sea",
            "phase_vocabulary": "https://github.com/OceanGlidersCommunity/OG-format-user-manual/blob/main/vocabularyCollection/phase.md",
        },
    )
    ds, dsa = add_sensors(ds, dsa)
    attrs = {}  # ds.attrs
    for key in ["glider_serial", "dataset_id", "contributor_name", "comment"]:
        attrs[key] = ds.attrs[key]
    for drop_attr in ["Metadata_Conventions"]:
        if drop_attr in attrs.keys():
            attrs.pop(drop_attr)
    start_datetime = pd.to_datetime(dsa.TIME.values, unit="s").min()
    ts = start_datetime.strftime("%Y%m%dT%H%M")
    dt_created = datetime.datetime.now().strftime("%Y%m%dT%H%M")
    if "delayed" in ds.attrs["dataset_id"]:
        postscript = "delayed"
    else:
        postscript = "R"
    attrs["id"] = f"sea{str(attrs['glider_serial']).zfill(3)}_{ts}_{postscript}"
    attrs["title"] = "OceanGliders example file for SeaExplorer data"
    attrs["platform"] = "sub-surface gliders"
    attrs["platform_vocabulary"] = "https://vocab.nerc.ac.uk/collection/L06/current/27/"
    attrs["contributor_email"] = (
        "callum.rollo@voiceoftheocean.org, louise.biddle@voiceoftheocean.org, , , , , , "
    )
    attrs["contributor_role_vocabulary"] = "https://vocab.nerc.ac.uk/collection/W08/"
    attrs["contributor_role"] = (
        "Data scientist, PI, Operator, Operator, Operator, Operator, Operator, Operator,"
    )
    attrs["contributing_institutions"] = "Voice of the Ocean Foundation"
    attrs["contributing_institutions_role"] = "Operator"
    attrs["contributing_institutions_role_vocabulary"] = (
        "https://vocab.nerc.ac.uk/collection/W08/current/"
    )
    attrs["date_created"] = dt_created
    attrs["agency"] = "Voice of the Ocean"
    attrs["agency_role"] = "contact point"
    attrs["agency_role_vocabulary"] = "https://vocab.nerc.ac.uk/collection/C86/current/"
    attrs["data_url"] = (
        f"https://erddap.observations.voiceoftheocean.org/erddap/tabledap/{attrs['dataset_id']}"
    )
    attrs["rtqc_method"] = "IOOS QC QARTOD https://github.com/ioos/ioos_qc"
    attrs["rtqc_method_doi"] = "None"
    attrs["featureType"] = "trajectory"
    attrs["Conventions"] = "CF-1.10, OG-1.0"
    if num_vals:
        attrs["comment"] = (
            f"Dataset for demonstration purposes only. Original dataset truncated to {num_vals} values for the sake of simplicity"
        )
    attrs["start_date"] = ts
    dsa.attrs = attrs
    dsa["TRAJECTORY"] = xr.DataArray(
        ds.attrs["id"],
        attrs={"cf_role": "trajectory_id", "long_name": "trajectory name"},
    )
    dsa["WMO_IDENTIFIER"] = xr.DataArray(
        ds.attrs["wmo_id"],
        attrs={"long_name": "wmo id"},
    )
    dsa["PLATFORM_MODEL"] = xr.DataArray(
        ds.attrs["glider_model"],
        attrs={
            "long_name": "model of the glider",
            "platform_model_vocabulary": "None",
        },
    )
    dsa["PLATFORM_SERIAL_NUMBER"] = xr.DataArray(
        f"sea{ds.attrs['glider_serial'].zfill(3)}",
        attrs={"long_name": "glider serial number"},
    )
    dsa["DEPLOYMENT_TIME"] = np.nanmin(dsa.TIME.values)
    dsa["DEPLOYMENT_TIME"].attrs = {
        "long_name": "date of deployment",
        "standard_name": "time",
        "units": "seconds since 1970-01-01T00:00:00Z",
        "calendar": "gregorian",
    }
    dsa["DEPLOYMENT_LATITUDE"] = dsa.LATITUDE.values[0]
    dsa["DEPLOYMENT_LATITUDE"].attrs = {"long_name": "latitude of deployment"}
    dsa["DEPLOYMENT_LONGITUDE"] = dsa.LONGITUDE.values[0]
    dsa["DEPLOYMENT_LONGITUDE"].attrs = {"long_name": "longitude of deployment"}
    dsa = encode_times_og1(dsa)
    dsa = set_best_dtype(dsa)
    return dsa


vars_as_is = [
    "altimeter",
    "nav_resource",
    "angular_cmd",
    "angular_pos",
    "ballast_cmd",
    "ballast_pos",
    "dead_reckoning",
    "declination",
    "desired_heading",
    "dive_num",
    "internal_pressure",
    "internal_temperature",
    "linear_cmd",
    "linear_pos",
    "security_level",
    "voltage",
    "distance_over_ground",
    "ad2cp_beam1_cell_number1",
    "ad2cp_beam2_cell_number1",
    "ad2cp_beam3_cell_number1",
    "ad2cp_beam4_cell_number1",
    "vertical_distance_to_seafloor",
    "profile_direction",
    "nav_state",
]


# + ['backscatter_raw', 'oxygen_phase', 'phycocyanin', 'phycocyanin_raw', 'down_irradiance_532', 'turbidity_raw', 'internal_temperature_PAR', 'methane_concentration', 'methane_raw_concentration', 'mets_raw_temperature', 'mets_temperature', 'nitrate_concentration', 'nitrate_molar_concentration', 'suna_internal_humidity', 'suna_internal_temperature'] # DELETE


def standardise_og10(ds):
    dsa = xr.Dataset()
    dsa.attrs = ds.attrs
    for var_name in list(ds) + list(ds.coords):
        if "qc" in var_name:
            continue
        if var_name in vocabularies.standard_names.keys():
            name = vocabularies.standard_names[var_name]
            dsa[name] = ("time", ds[var_name].values, vocabularies.vocab_attrs[name])
            for key, val in ds[var_name].attrs.items():
                if key not in dsa[name].attrs.keys():
                    dsa[name].attrs[key] = val
            qc_name = f"{var_name}_qc"
            if qc_name in list(ds):
                dsa[f"{name}_QC"] = ("time", ds[qc_name].values, ds[qc_name].attrs)
                dsa[name].attrs["ancillary_variables"] = f"{name}_QC"
        else:
            if var_name in vars_as_is:
                dsa[var_name.upper()] = (
                    "time",
                    ds[var_name].values,
                    ds[var_name].attrs,
                )
                _log.error(f"variable {var_name} not translated. Will be added as-is")
            else:
                _log.error(f"variable {var_name} not to be included. Dropping")

    dsa = set_best_dtype(dsa)
    return dsa


if __name__ == "__main__":
    dsn = xr.open_dataset(
        "/data/data_l0_pyglider/nrt/SEA76/M19/timeseries/mission_timeseries.nc",
    )
    dsn = standardise_og10(dsn)
    dsn = convert_to_og1(dsn)
    dsn.to_netcdf("new.nc")
