import numpy as np
import yaml
import ioos_qc
from ioos_qc.config import Config
from ioos_qc.qartod import aggregate
from ioos_qc.streams import XarrayStream
from ioos_qc.results import collect_results, CollectedResult
import datetime
import logging

_log = logging.getLogger(__name__)

cond_temp_vars = ["potential_density", "density", "potential_temperature"]


def get_configs():
    configs = {
        "pressure": {
            "pressure": {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 2000],
                        "fail_span": [-2, 2000],
                    },
                    "spike_test": {"suspect_threshold": 2.0, "fail_threshold": 6.0},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "depth": {
            "depth": {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 2000],
                        "fail_span": [-2, 2000],
                    },
                    "spike_test": {"suspect_threshold": 2.0, "fail_threshold": 6.0},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "temperature": {
            "temperature": {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 30],
                        "fail_span": [-2.5, 40],
                    },
                    "spike_test": {"suspect_threshold": 2.0, "fail_threshold": 6.0},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "salinity": {
            "conductivity": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [5, 42], "fail_span": [2, 45]},
                },
            },
            "salinity": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [5, 38], "fail_span": [2, 41]},
                    "spike_test": {"suspect_threshold": 0.3, "fail_threshold": 0.9},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "conductivity": {
            "conductivity": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [5, 42], "fail_span": [2, 45]},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "oxygen_concentration": {
            "oxygen_concentration": {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 350],
                        "fail_span": [0, 500],
                    },
                    "spike_test": {"suspect_threshold": 10, "fail_threshold": 50},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "chlorophyll": {
            "chlorophyll": {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 15],
                        "fail_span": [-1, 20],
                    },
                    "spike_test": {"suspect_threshold": 1, "fail_threshold": 5},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
        },
        "longitude": {
            "latitude": {
                "qartod": {
                    "spike_test": {
                        "suspect_threshold": 0.0001,
                        "fail_threshold": 0.001,
                    },
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
            "longitude": {
                "qartod": {
                    "spike_test": {
                        "suspect_threshold": 0.0001,
                        "fail_threshold": 0.001,
                    },
                },
            },
        },
        "latitude": {
            "latitude": {
                "qartod": {
                    "spike_test": {
                        "suspect_threshold": 0.0001,
                        "fail_threshold": 0.001,
                    },
                    "location_test": {"bbox": [10, 50, 25, 70]},
                },
            },
            "longitude": {
                "qartod": {
                    "spike_test": {
                        "suspect_threshold": 0.0001,
                        "fail_threshold": 0.001,
                    },
                },
            },
        },
    }
    return configs


def derive_configs(configs):
    for var in cond_temp_vars:
        configs[var] = {**configs["temperature"], **configs["salinity"]}
    return configs


def apply_ioos_flags(ds, config):
    if not set(config.keys()).issubset(set(list(ds) + list(ds.coords))):
        _log.warning(f"{ds.keys} not found in dataset. Skipping")
        return None, None
    c = Config(config)
    qc = XarrayStream(ds, lon="longitude", lat="latitude")
    runner = list(qc.run(c))
    results = collect_results(runner, how="list")
    agg = CollectedResult(
        stream_id="",
        package="qartod",
        test="qc_rollup",
        function=aggregate,
        results=aggregate(results),
        tinp=qc.time(),
        data=ds,
    )
    flag_vals = agg.results
    call = c.calls
    proc_record = str(call)
    return flag_vals, proc_record


def flag_ioos(ds):
    configs = get_configs()
    # If the glider has a GPCTD, use this for the salinity config
    if ds["conductivity"].attrs["units"] == "S m-1":
        configs["salinity"]["conductivity"]["qartod"]["gross_range_test"] = {
            "suspect_span": [0.6, 4.2],
            "fail_span": [0.3, 4.5],
        }
    configs = derive_configs(configs)
    for config_name, config in configs.items():
        if config_name not in list(ds.variables) + list(ds.coords):
            _log.warning(f"{config_name} not found in dataset")
            continue
        # extract ioos flags for these variables
        flags, comment = apply_ioos_flags(ds, config)
        flagged_prop = 100 * sum(np.logical_and(flags > 1, flags < 9)) / len(flags)
        _log.info(f"Flagged {flagged_prop.round(3)} % of {config_name} as bad")
        # Apply flags and add comment
        ioos_comment = (
            f"Quality control flags from IOOS QC QARTOD https://github.com/ioos/ioos_qc Version: "
            f"{ioos_qc.__version__}. Using config: {comment}."
        )
        if (
            "temperature" in config.keys()
            or "salinity" in config.keys()
            or "conductivity" in config.keys()
        ):
            ioos_comment = (
                f"{ioos_comment}  Threshold values from EuroGOOS DATA-MEQ Working Group (2010)"
                f" Recommendations for in-situ data Near Real Time Quality Control [Version 1.2]. EuroGOOS"
                f", 23pp. DOI http://dx.doi.org/10.25607/OBP-214."
            )

        flag = ds[config_name].copy()
        flag.values = flags
        parent_attrs = flag.attrs
        flag.attrs = {
            "ioos_qc_module": "qartod",
            "quality_control_conventions": "IOOS QARTOD standard flags",
            "quality_control_set": 1,
            "valid_min": 1,
            "valid_max": 9,
            "flag_values": [1, 2, 3, 4, 9],
            "flag_meanings": "GOOD, UNKNOWN, SUSPECT, FAIL, MISSING",
            "long_name": f"quality control flags for {parent_attrs['long_name']}",
            "standard_name": f"{parent_attrs['standard_name']}_flag",
            "comment": ioos_comment,
        }
        ds[f"{config_name}_qc"] = flag
    return ds


def flag_oxygen(ds):
    oxy_meta_str = ds.oxygen
    import ast

    oxy_meta = ast.literal_eval(oxy_meta_str)
    cal_date = datetime.date.fromisoformat(oxy_meta["calibration_date"])
    if "coda" in oxy_meta["make_model"] and cal_date < datetime.date(2022, 12, 30):
        # These early batches of codas were improperly calibrated
        _log.info("bad legato")
        pre_flags = ds["oxygen_concentration_qc"].values
        sus_flags = np.ones(len(pre_flags), dtype=int) * 3
        ds["oxygen_concentration_qc"].values = np.maximum(pre_flags, sus_flags)
        original_comment = ds["oxygen_concentration_qc"].attrs["comment"]
        bad_oxy_comment = (
            "Oxygen optode improperly calibrated during this deployment. All flags set to minimum value"
            " of 3 (SUSPECT). Data may be recoverable. "
        )
        comment = f"{bad_oxy_comment} {original_comment}"
        ds["oxygen_concentration_qc"].attrs["comment"] = comment
        ds["oxygen_concentration_qc"].attrs["quality_control_set"] = 1
    return ds


def flag_pilot(ds):
    attrs = ds.attrs
    glider = attrs["glider_serial"]
    mission = attrs["deployment_id"]
    mission_yaml = f"/data/deployment_yaml/mission_yaml/SEA{glider}_M{mission}.yml"
    with open(mission_yaml) as fin:
        deployment = yaml.safe_load(fin)
    if "qc" not in deployment.keys():
        return ds
    # If temperature or conductivity flagged, add qc entries for vars derived from conductivity/temperature
    if "temperature" in deployment["qc"]:
        for ct_var in cond_temp_vars:
            deployment["qc"][ct_var] = deployment["qc"]["temperature"]
    elif "conductivity" in deployment["qc"]:
        for ct_var in cond_temp_vars:
            deployment["qc"][ct_var] = deployment["qc"]["conductivity"]
    for variable in deployment["qc"]:
        if f"{variable}_qc" not in list(ds):
            _log.warning(
                f"{variable} in yaml qc section, but has no qc from IOOS. Applying minimum qc",
            )
            flag = ds[variable].copy()
            flag.values[:] = 2
            parent_attrs = flag.attrs
            flag.attrs = {
                "quality_control_conventions": "IOOS QARTOD standard flags",
                "quality_control_set": 1,
                "valid_min": 1,
                "valid_max": 9,
                "flag_values": [1, 2, 3, 4, 9],
                "flag_meanings": "GOOD, UNKNOWN, SUSPECT, FAIL, MISSING",
                "long_name": f"quality control flags for {parent_attrs['long_name']}",
                "standard_name": f"{parent_attrs['standard_name']}_flag",
                "comment": "no automated QC applied",
            }
            ds[f"{variable}_qc"] = flag
        pilot_qc = deployment["qc"][variable]
        var_qc = ds[f"{variable}_qc"]
        time_str = ""
        if "start" in pilot_qc.keys():
            start_str = pilot_qc["start"]
            start = np.datetime64(start_str)
            time_str = f"start: {start_str}"
        else:
            start = np.nanmin(ds.time)
        if "end" in pilot_qc.keys():
            end_str = pilot_qc["end"]
            end = np.datetime64(end_str)
            time_str = f"{time_str}, end: {end_str}"
        else:
            end = np.nanmax(ds.time)
        var_qc_timesub = var_qc.values[np.logical_and(ds.time >= start, ds.time <= end)]
        var_qc_timesub[var_qc_timesub < pilot_qc["value"]] = pilot_qc["value"]
        var_qc.values[np.logical_and(ds.time >= start, ds.time <= end)] = var_qc_timesub
        original_comment = var_qc.attrs["comment"]
        pilot_comment = pilot_qc["comment"]
        comment = f"Pilot QC: {pilot_comment} {time_str}. Minimum QC value set to {pilot_qc['value']}. IOOS_QC: {original_comment}"
        var_qc.attrs["comment"] = comment
        _log.info(f"applied pilot QC to {variable}, min value {pilot_qc['value']}")
    return ds


def flagger(ds):
    ds = flag_ioos(ds)
    ds = flag_oxygen(ds)
    ds = flag_pilot(ds)
    ds.attrs["processing_level"] = (
        f"L1. Quality control flags from IOOS QC QARTOD https://github.com/ioos/ioos_qc "
        f"Version: {ioos_qc.__version__} "
    )
    ds.attrs["disclaimer"] = (
        "Data, products and services from VOTO are provided 'as is' without any warranty as"
        " to fitness for a particular purpose."
    )
    return ds


def apply_flags(ds, max_flag_accepted=2, var_max_flags={}):
    variable_list = list(ds)
    for var_name in variable_list:
        if var_name[-2:] == "qc":
            flag = ds[var_name]
            var = var_name[:-3]
            data = ds[var].values
            if var in var_max_flags.keys():
                data[flag > var_max_flags[var]] = np.nan
            else:
                data[flag > max_flag_accepted] = np.nan
    return ds
