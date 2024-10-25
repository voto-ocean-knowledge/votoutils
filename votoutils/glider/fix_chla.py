from copy import copy
import numpy as np
import xarray as xr


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
            ds[var_name].encoding = ds["time"].encoding
    return ds


def fix_dark_counts(ds, min_depth=50, max_depth=70):
    ds["chlorophyll_uncorrected"] = ds["chlorophyll"].copy()
    ds["chlorophyll_uncorrected"].attrs["comment"] = (
        "Original, uncorrected chlorohyll-a values"
    )
    raw = copy(ds.chlorophyll_raw.values)
    # raw[ds.chlorophyll_qc > 4] = np.nan
    depth_min = np.nanpercentile(ds.depth.values, min_depth)
    raw[ds.depth < depth_min] = np.nan
    depth_max = np.nanpercentile(ds.depth.values, max_depth)
    raw[ds.depth > depth_max] = np.nan
    deep_dark_count = int(np.nanpercentile(raw, 5))
    optics = eval(ds.attrs["optics"])
    ds["chlorophyll"].attrs["comment"] = (
        f"Chlorophyll values recalculated using corrected dark count. old DC: {optics['calibration_parameters']['Chl_DarkCounts']}, new DC:{deep_dark_count}"
    )
    optics["calibration_parameters"]["Chl_DarkCounts"] = deep_dark_count
    ds.attrs["optics"] = str(optics)
    new_dc = np.full(len(ds.time), deep_dark_count)
    new_chl = (ds.chlorophyll_raw - new_dc) * optics["calibration_parameters"]["Chl_SF"]
    ds["chlorophyll"].values = new_chl
    ds = encode_times(ds)
    return ds


if __name__ == "__main__":
    ds = xr.open_dataset(
        "/data/data_l0_pyglider/nrt/SEA76/M21/timeseries/mission_timeseries.nc",
    )
    dsa = fix_dark_counts(ds)
    ds.close()
    dsa.to_netcdf("mission_timeseries.nc")
