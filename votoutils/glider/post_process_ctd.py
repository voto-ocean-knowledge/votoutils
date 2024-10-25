import gsw
import numpy as np
from scipy.interpolate import interp1d
import logging
import pandas as pd

_log = logging.getLogger(__name__)


def interp(x, y, xi):
    _gg = np.isfinite(x + y)
    return interp1d(x[_gg], y[_gg], bounds_error=False, fill_value=np.nan)(xi)


def salinity_pressure_correction(ds):
    """correct salinity from pressure lag"""
    _log.info("performing RBR salinity pressure correction")
    X2 = 1.8e-06
    X3 = -9.472e-10
    X4 = 2.112e-13
    Cmeas = ds["conductivity"].values
    Pmeas = ds["pressure"].values
    ds["conductivity"].values = Cmeas / (1 + X2 * Pmeas + X3 * Pmeas**2 + X4 * Pmeas**3)
    ds["conductivity"].attrs["comment"] = (
        "Corrected for pressure lag in post-processing. "
    )
    ds["salinity"].values = gsw.SP_from_C(
        ds["conductivity"].values,
        ds["temperature"].values,
        Pmeas,
    )
    ds["salinity"].attrs["comment"] = "Corrected for pressure lag in post-processing. "
    return ds


def pandas_fill(arr):
    return pd.DataFrame(arr).bfill().values[:, 0]


def correct_rbr_lag(ds):
    """
    Thermal lag from Thermal Inertia of Conductivity Cells: Observations with a Sea-Bird Cell
    Rolf G. Lueck and James J. Picklo https://doi.org/10.1175/1520-0426(1990)007<0756:TIOCCO>2.0.CO;2
    :return:
    """
    raw_seconds = ds["time"].values - np.nanmin(ds["time"].values)
    if "float" not in str(ds.time.dtype):
        raw_seconds = raw_seconds / np.timedelta64(1, "s")
    vert_spd = np.gradient(
        -gsw.z_from_p(ds["pressure"].values, ds["latitude"].values),
        raw_seconds,
    )

    spd = np.abs(vert_spd / np.sin(np.deg2rad(ds["pitch"].values)))

    spd[spd < 0.01] = 0.01
    spd[spd > 1] = 1
    spd[~np.isfinite(spd)] = 0.01

    spd = spd * 100

    raw_temp = ds["temperature"].values

    Fs = np.median(1 / np.gradient(raw_seconds))
    if not 0.01 < Fs < 100:
        _log.warning(
            f"Bad calculated sampling frequency {str(Fs)} Hz. Abort correction",
        )
        return ds
    _log.info(
        "Performing thermal mass correction... Assuming a sampling frequency of "
        + str(Fs)
        + " Hz.",
    )
    fn = Fs / 2

    corr_temp = raw_temp.copy()

    # Correct temperature probe's thermal lag to get real temperature
    alpha = 0.05 * spd ** (-0.83)
    tau = 375
    bias_temp = np.full_like(corr_temp, 0)
    a = 4 * fn * alpha * tau / (1 + 4 * fn * tau)  # Lueck and Picklo (1990)
    b = 1 - 2 * a / alpha  # Lueck and Picklo (1990)
    for sample in np.arange(1, len(bias_temp)):
        bias_temp[sample] = -b[sample] * bias_temp[sample - 1] + a[sample] * (
            corr_temp[sample] - corr_temp[sample - 1]
        )
    corr_temp = interp(raw_seconds, raw_temp, raw_seconds + 0.9)
    corr_temp = pandas_fill(corr_temp)

    # Estimate effective temperature of the conductivity measurement (long thermal lag)
    alpha = 0.18 * spd ** (-1.10)
    tau = 179
    bias_long = np.full_like(corr_temp, 0)
    a = 4 * fn * alpha * tau / (1 + 4 * fn * tau)  # Lueck and Picklo (1990)
    b = 1 - 2 * a / alpha  # Lueck and Picklo (1990)
    for sample in np.arange(1, len(bias_long)):
        bias_long[sample] = -b[sample] * bias_long[sample - 1] + a[sample] * (
            corr_temp[sample] - corr_temp[sample - 1]
        )

    # Estimate effective temperature of the conductivity measurement (short thermal lag)
    alpha = 0.23 * spd ** (-0.82)
    tau = 27.15 * spd ** (-0.58)
    bias_short = np.full_like(corr_temp, 0)
    a = 4 * fn * alpha * tau / (1 + 4 * fn * tau)  # Lueck and Picklo (1990)
    b = 1 - 2 * a / alpha  # Lueck and Picklo (1990)
    for sample in np.arange(1, len(bias_short)):
        bias_short[sample] = -b[sample] * bias_short[sample - 1] + a[sample] * (
            corr_temp[sample] - corr_temp[sample - 1]
        )

    corr_sal = gsw.SP_from_C(
        ds["conductivity"].values,
        corr_temp - bias_long - bias_short,
        ds["pressure"].values,
    )
    corr_temp[np.isnan(ds["temperature"].values)] = np.nan
    corr_sal[np.isnan(ds["salinity"].values)] = np.nan

    ds["temperature"].values = corr_temp
    ds["salinity"].values = corr_sal

    sa = gsw.SA_from_SP(ds["salinity"], ds["pressure"], ds["longitude"], ds["latitude"])
    ct = gsw.CT_from_t(sa, ds["temperature"], ds["pressure"])
    ds["potential_density"].values = 1000 + gsw.density.sigma0(sa, ct)
    ds["density"] = gsw.density.rho(ds.salinity, ds.temperature, ds.pressure)
    rbr_str = (
        "Corrected following Thermal lag from Thermal Inertia of Conductivity Cells: Observations with a "
        "Sea-Bird Cell Rolf G. Lueck and James J. Picklo"
        " https://doi.org/10.1175/1520-0426(1990)007<0756:TIOCCO>2.0.CO;2 as implemented by "
        "Dever M., Owens B., Richards C., Wijffels S., Wong A., Shkvorets I., Halverson M., and Jonhson G."
        " (accepted). Static and dynamic performance of the RBRargo3 CTD."
        " Journal of Atmospheric and Oceanic Technology."
    )
    ds["temperature"].attrs["comment"] += rbr_str
    ds["salinity"].attrs["comment"] += rbr_str
    return ds


if __name__ == "__main__":
    logf = "/data/log/new.log"
    logging.basicConfig(
        filename=logf,
        filemode="w",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    import xarray as xr

    ds = xr.open_dataset(
        "/home/callum/Downloads/new/M11/timeseries/mission_timeseries.nc",
    )
    ds = salinity_pressure_correction(ds)
    ds = correct_rbr_lag(ds)
    ds.to_netcdf(
        "/home/callum/Downloads/new/M11/timeseries/mission_timeseries_corrected.nc",
    )
