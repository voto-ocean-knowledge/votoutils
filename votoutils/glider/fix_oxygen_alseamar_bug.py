import logging
import gsw
import numpy as np
import yaml

_log = logging.getLogger(__name__)

# DO calculations
def dissolved_oxygen_from_raw(arod_ft_do_an, arod_ft_temperature, arod_ft_led, pressure, coef):
    N = arod_ft_do_an / 10000
    DO = (((1 + coef["d0"] * arod_ft_temperature) / (coef["d1"] + coef["d2"] * N + coef["d3"] * arod_ft_led + coef["d4"] * arod_ft_led * N)) ** coef[
        "e0"] - 1) * (1 / (coef["c0"] + coef["c1"] * arod_ft_temperature + coef["c2"] * arod_ft_temperature ** 2))
    return DO


def oxygen_concentration_correction(ds, ncvar):
    """
    Correct oxygen signal for salinity signal

    Parameters
    ----------
    ds : `xarray.Dataset`
        Should have *oxygen_concentration*, *potential_temperature*, *salinity*,
        on a *time* coordinate.
    ncvar : dict
        dictionary with netcdf variable definitions in it.  Should have
        *oxygen_concentration* as a key, which itself should specify
        a *reference_salinity* and have *correct_oxygen* set to ``"True"``.

    Returns
    -------
    ds : `xarray.Dataset`
        With *oxygen_concentration* corrected for the salinity effect.
    """

    oxy_yaml = ncvar['oxygen_concentration']
    if 'reference_salinity' not in oxy_yaml.keys():
        _log.warning('No reference_salinity found in oxygen deployment yaml. '
                     'Assuming reference salinity of 0 psu')
        ref_sal = 0
    else:
        ref_sal = float(oxy_yaml['reference_salinity'])
    _log.info(f'Correcting oxygen using reference salinity {ref_sal} PSU')
    if len(ds.sizes) > 1:
        ds_oxy = ds.oxygen_concentration.copy()
        ds_temp = ds.potential_temperature
        ds_sal = ds.salinity
        o2_sol = gsw.O2sol_SP_pt(ds_sal, ds_temp)
        o2_sat = ds_oxy / gsw.O2sol_SP_pt(ds_sal * 0 + ref_sal, ds_temp)
        ds['oxygen_concentration'].values = o2_sat * o2_sol
        ds['oxygen_concentration'].values[np.isnan(ds_oxy)] = np.nan

    else:

        ds_oxy = ds.oxygen_concentration[~np.isnan(ds.oxygen_concentration)]
        # Match the nearest temperature and salinity values from their timestamps
        ds_temp = ds.potential_temperature[~np.isnan(ds.potential_temperature)].reindex(
            time=ds_oxy.time, method="nearest")
        ds_sal = ds.salinity[~np.isnan(ds.salinity)].reindex(
            time=ds_oxy.time, method="nearest")
        o2_sol = gsw.O2sol_SP_pt(ds_sal, ds_temp)
        o2_sat = ds_oxy / gsw.O2sol_SP_pt(ds_sal*0 + ref_sal, ds_temp)
        ds['oxygen_concentration'].values[~np.isnan(ds.oxygen_concentration)] = (
            o2_sat * o2_sol)
    ds['oxygen_concentration'].attrs['oxygen_concentration_QC:RTQC_methodology'] = (
        f'oxygen concentration corrected for salinity using gsw.O2sol_SP_pt with'
        f'salinity and potential temperature from dataset. Original oxygen '
        f'concentration assumed to have been calculated using salinity = '
        f'{ref_sal} PSU')
    return ds


def recalc_oxygen(ds):
    """
    This function recalculates dissolved oxygen concentration to correct a bug in ALSEAMAR firmware versions.
    beginning in version 2.24.2-r and ending in 2.25.1-r. Versions 2.24.1-r and lower versions 2.25.2-r and greater are not affected.
    More information in this report:
    https://observations.voiceoftheocean.org/static/img/reports/Quality_Issue_4_pld_firmware_oxygen.pdf

    :param ds: dataset containing dissolved oxygen data
    :return: ds: datasets with corrected dissolved oxygen concentrations
    """
    if "oxygen_concentration" not in ds.variables:
        _log.info("oxygen_concentration no present in ds. No correction applied")
        return ds

    if "recalc_oxygen" in ds['oxygen_concentration'].attrs['comment']:
        _log.info("oxygen concentration has already been fixed for this dataset. No correction applied")
        return ds

    _log.warning("Correcting dissolved_oxygen for alseamar firmware bug VOTO QC Issue #4")
    ds['oxygen_concentration_uncorrected'] = ds['oxygen_concentration'].copy()
    ds['oxygen_concentration'].attrs['comment'] += ("Corrected oxygen concentration to fix miscalculation by alseamar"
                                                    " firmware. Using function voto_utils.glider.fix_oxygen_alseamar.recalc_oxygen."
                                                    " Issues described in "
                                                    "https://observations.voiceoftheocean.org/static/img/reports/Quality_Issue_4_pld_firmware_oxygen.pdf")

    arod_temperature = ds.temperature_oxygen.values
    arod_led = ds.oxygen_led_counts.values
    arod_do_an = ds.oxygen_ad_counts.values
    pres = ds.pressure.values
    attrs = ds.attrs

    coefficients = eval(attrs['oxygen'])['calibration_parameters']

    o2_correct = dissolved_oxygen_from_raw(arod_do_an, arod_temperature, arod_led, pres, coefficients)
    ds['oxygen_concentration'].values = o2_correct
    platform_serial = ds.attrs['platform_serial']
    mission = ds.attrs['deployment_id']
    original_deploymentyaml = (
        f"/data/deployment_yaml/mission_yaml/{platform_serial}_M{str(mission)}.yml"
    )
    with open(original_deploymentyaml) as fin:
        deployment = yaml.safe_load(fin)
    nc_yaml = deployment['netcdf_variables']
    ds = oxygen_concentration_correction(ds, nc_yaml)
    return ds