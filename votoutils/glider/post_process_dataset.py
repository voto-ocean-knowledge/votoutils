import numpy as np
import re
import gsw
import xarray as xr
import yaml
from packaging.version import parse as parse_version
from votoutils.glider.post_process_optics import betasw_ZHH2009
from votoutils.qc.flag_qartod import flagger
from votoutils.utilities.geocode import filter_territorial_data, nan_bad_locations, flag_bad_locations, locs_to_seas
from votoutils.glider.post_process_ctd import (
    salinity_pressure_correction,
    correct_rbr_lag,
)
from votoutils.glider.fix_oxygen_alseamar_bug import recalc_oxygen
import logging

_log = logging.getLogger(__name__)

jammed_missions = [("SEA044", 106),
                  ("SEA067", 73),
                  ("SEA063", 88),
                   ("SEA067", 86),
                   ("SHW002", 26),
                   ]

jammmed_cycles = {'SHW003_M13': [2365, 2367, 2368]}

def remove_gps_jammed_cycles(ds):
    dataset_id = ds.attrs['platform_serial_number'] + '_M' + str(ds.attrs['deployment_id'])
    if dataset_id not in jammmed_cycles.keys():
        return ds
    bad_cycles = jammmed_cycles[dataset_id]
    good_points = np.empty(len(ds.N_MEASUREMENTS), dtype=bool)
    good_points[:] = True
    for cycle in bad_cycles:
        good_points[ds['DIVE_NUMBER']==cycle] = False
    ds = ds.isel(N_MEASUREMENTS=good_points)
    return ds

def calculate_bbp(ds, beam_angle=117):
    # https://oceanobservatories.org/wp-content/uploads/2015/10/1341-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf
    _log.info("processing backscatter")
    temperature = ds["TEMP"].values
    salinity = ds["PSAL"].values
    beta_total = ds["backscatter_scaled"].values
    backscatter_str = ds["backscatter_scaled"].attrs["standard_name"]
    wavelength = int(re.findall(r"\d+", backscatter_str)[0])
    beta_sw, __, __ = betasw_ZHH2009(temperature, salinity, wavelength, beam_angle)
    beta_p = beta_total - beta_sw
    if beam_angle == 117:
        chi_p = 1.08  # For 117* angle (Sullivan & Twardowski, 2009)
    elif beam_angle == 140:
        chi_p = 1.17  # For 140* angle (Sullivan & Twardowski, 2009)
    else:
        _log.error("Incompatible beam_angle. Allowed values are 117 or 140")
        return
    bbp_val = 2 * np.pi * chi_p * beta_p  # in m-1
    bbp = ds["backscatter_scaled"].copy()
    bbp.values = bbp_val
    bbp.attrs = {
        "units": "m^{-1}",
        "observation_type": "calculated",
        "long_name": f"{wavelength} nm b_bp: scattering of particles integrated over the backwards hemisphere",
        "processing": "Particulate backscatter b_bp calculated following methods in the Ocean Observatories Initiative document "
        "DATA PRODUCT SPECIFICATION FOR OPTICAL BACKSCATTER (RED WAVELENGTHS) Version 1-05 "
        "Document Control Number 1341-00540 2014-05-28. Downloaded from "
        "https://oceanobservatories.org/wp-content/uploads/2015/10/1341-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf",
    }
    ds["particulate_backscatter"] = bbp

    return ds


def vertical_distance_from_altimeter(altimeter, pitch_glider, roll):
    pitch_altimeter = pitch_glider + 20
    vertical_distance = (
        np.cos(np.deg2rad(pitch_altimeter)) * np.cos(np.deg2rad(roll)) * altimeter
    )
    return vertical_distance


def process_altimeter(ds):
    """
    From the seaexploer manual: the angle of the altimeter is 20 degrees, such that it is vertical when the glider
    is pitched at 20 degrees during the dive.
    :param ds:
    :return: ds with additional bathymetry variable
    """
    if "altimeter" not in list(ds):
        _log.warning("No altimeter data found")
        return ds
    altim_raw = ds["altimeter"].values
    altim = altim_raw.copy()
    altim[altim_raw <= 0] = np.nan
    bathy_from_altimeter = vertical_distance_from_altimeter(
        altim,
        ds["PITCH"].values,
        ds["ROLL"].values,
    )
    vertical_distance_to_seafloor = ds["altimeter"].copy()
    vertical_distance_to_seafloor.values = bathy_from_altimeter
    attrs = vertical_distance_to_seafloor.attrs
    attrs["long_name"] = "vertical distance from glider to seafloor"
    attrs["comment"] = (
        "Distance to the seafloor is calculated from the glider altimeter (see altimeter variable),"
        " which is oriented at 20 degrees from the vertical such that it is vertical when the glider "
        "is pitched downwards at 20 degrees."
    )
    vertical_distance_to_seafloor.attrs = attrs
    ds["vertical_distance_to_seafloor"] = vertical_distance_to_seafloor
    return ds



def nan_bad_depths(ds):
    ds["DEPTH"][ds["DEPTH"] > int(ds["DEPTH"].attrs["valid_max"])] = np.nan
    ds["PRES"][ds["PRES"] > int(ds["PRES"].attrs["valid_max"])] = np.nan
    return ds


def remove_jammed_locations(ds):
    attrs = ds.attrs
    if (attrs["platform_serial_number"], int(attrs["deployment_id"])) not in jammed_missions:
        return ds
    for coord in ['LATITUDE', 'LONGITUDE']:
        ds[coord].values[ds.LONGITUDE.values > 18] = np.nan
        ds[coord].attrs['comment'] += 'This mission suffered from extensive GPS jamming. Affected lon and lat have been replace with nan'
    return ds


def correct_locations(ds):
    ds = flag_bad_locations(ds)
    ds = nan_bad_locations(ds)
    qc_good = np.logical_and(ds.LONGITUDE_QC == 1, ds.LATITUDE_QC == 1)
    lon = ds.LONGITUDE[qc_good].values
    lat = ds.LATITUDE[qc_good].values
    ds.attrs["basin"] = locs_to_seas(lon[::10], lat[::10])
    if len(lon>0) and len(lat)>0:
        ds.attrs["geospatial_lon_min"] = np.nanmin(lon)
        ds.attrs["geospatial_lon_max"] = np.nanmax(lon)
        ds.attrs["geospatial_lat_min"] = np.nanmin(lat)
        ds.attrs["geospatial_lat_max"] = np.nanmax(lat)
    return ds


def hydrostatic_depth(ds):
    """Hydrostatic depth calculation based on temperature, salinity and pressure measurements.
    The standard depth computation of pyglider assumes a standard ocean salinity of ~34 PSU,
    which is not correct for the Baltic Sea. This error leads to a bias in our depth dimension,
    placing the glider and its data shallower than it actually is. This function returns a
    more accurate depth calculation. However, the calculation depends on working and calibrated
    temperature and salinity outputs, which means that this function cannot replace the original
    (simpler) pyglider depth calculation for the cases of broken/clogged CTDs.

    Parameters:
        ds (xarray.Dataset): Dataset style input, including variables for pressure (dbar),
        salinity (in-situ), temperature (in-situ).

    Returns:
        ds with additional depth_hydrostatic variable"""

    def surface_layer_pot_density(ds):
        """ compute surface properties to approximate hydrostatic start depth for profiles with
        start_depth>0

        Returns:
           mean salinity and temperature between 1-2 dbar depth (to make sure it is within MLD)
        """
        ds_surface = ds.where((ds.PRES>1) & (ds.PRES<3))
        ds_surface = ds_surface.mean()
        return float(ds_surface.potential_density)

    def compute_depth_hydrostatic(dfprofile):
        downcast=True if dfprofile["PROFILE_DIRECTION"].iloc[0]==1. else False
        dfprofile = dfprofile[::-1] if not downcast else dfprofile # flip upcasts for hydrostatic calculation
        start_depth = dfprofile["PRES"].iloc[0]*10**4/(g*surface_pot_density)
        if len(dfprofile["PRES"]) <= 1:
            dfprofile["delta_z"] = np.full([len(dfprofile["PRES"])], np.nan)
            dfprofile["depth_hydrostatic"] = np.full([len(dfprofile["PRES"])], np.nan)
            return dfprofile

        dfprofile["delta_z"] = -np.gradient(dfprofile["PRES"]*10**4)/(g*dfprofile["potential_density"])
        dfprofile["depth_hydrostatic"] = -np.nancumsum(dfprofile["delta_z"])+start_depth
        dfprofile = dfprofile[::-1] if not downcast else dfprofile # flip back upcasts after calculation
        return dfprofile

    g = 9.82 # best approximate value for Baltic Sea latitude
    surface_pot_density = surface_layer_pot_density(ds)
    df = ds.to_pandas() # much better performance of pandas compared to xarray
    df = df.groupby("PROFILE_NUMBER").apply(compute_depth_hydrostatic)
    ds["depth_hydrostatic"] = df.droplevel('PROFILE_NUMBER')["depth_hydrostatic"]
    return ds


def fix_specific_mission(ds):
    platform_mission = (ds.attrs['platform_serial_number'], int(ds.attrs['deployment_id']))
    ctd_var = ds['TEMP'].attrs['sensor']
    model = ds[ctd_var].attrs['sensor_model']
    if 'legato4' in model.lower():
        # The legato4 pressure sensor is not corrected for air pressure in early versions of pld software
        pld_string = ds.attrs['seapld']
        pld_version = parse_version(pld_string.split('-')[0].replace(' ', ''))
        legato4_fix_version = parse_version('2.26.1')

        if pld_version < legato4_fix_version:
            ds['PRES'].values -= 10.1325
            ds['DEPTH'].values -= 10
            ds['PRES'].attrs['comment'] += 'manually corrected for air pressure offset of 10.1325 dbar'
            ds['DEPTH'].attrs['comment'] += 'manually corrected for air pressure offset'

    match platform_mission:

        case ('SHW002', 26):
            # On this mission the nav memory card failed after 3 weeks. Need to make artificial
            # dead reckoning data to fix this, as this is needed for the ADCP processing
            dead = ds['DEAD_RECKONING'].values
            dead[ds['TIME'] > np.datetime64('2026-05-10T16:00:00')] = 1
            dead[np.logical_and(ds['TIME'] > np.datetime64('2026-05-10T16:00:00'), ds['DEPTH']< 2.5)] = 0
            ds['DEAD_RECKONING'].values = dead
            ds['DEAD_RECKONING'].attrs['comment'] = 'Dead reckoning manually created after 2026-05-10T16:00:00 when glider memory card failed to enable ADCP processing'
        case ('SEA069', 15):
            attrs = ds.attrs
            if attrs["platform_serial_number"] == "SEA069" and int(attrs["deployment_id"]) == 15:
                _log.info("correcting phycocyanin values for SEA69 M15")
                ds["PHYCOCYANIN"].values = ds["PHYCOCYANIN"].values * 0.1
                ds.PHYCOCYANIN.attrs["comment"] += (
                    " Values multiplied by 0.1 in post-processing to correct for bad scale "
                    "factor during deployment"
                )
    return ds


def add_derived_variables(ds):
    psal = gsw.SP_from_C(
        ds["CNDC"].values,
        ds["TEMP"].values,
        ds["PRES"].values
    )
    with open("/data/ocean-gliders-format-vocabularies/yaml/validated_yaml/og1_variables.yaml") as fin:
        og1_vars = yaml.safe_load(fin)
    ds['PSAL'] = xr.DataArray(psal, dims=ds['TEMP'].dims, attrs=og1_vars['PSAL'])
    ds['PSAL'].attrs['sensor'] = ds['TEMP'].attrs['sensor']
    density = gsw.density.rho(ds.PSAL, ds.TEMP, ds.PRES)
    ds['DENSITY'] = xr.DataArray(density, dims=ds['TEMP'].dims, attrs=og1_vars['DENSITY'])
    return ds


def add_uncorr_variables(ds):
    ds['TEMP_UNCORR'] = ds['TEMP'].copy()
    ds['TEMP_UNCORR'].attrs['comment'] = 'uncorrected temperature'
    ds['CNDC_UNCORR'] = ds['CNDC'].copy()
    ds['CNDC_UNCORR'].attrs['comment'] = 'uncorrected conductivity'
    return ds

def variables_optimise_dtype(ds):
    int_vars = [
        "angular_cmd",
        "ballast_cmd",
        "linear_cmd",
        "nav_state",
        "security_level",
        "dive_num",
        "desired_heading",
    ]
    ds_variables = list(ds)
    for var in ds_variables:
        if var in int_vars or var[-2:] == "QC":
            ds[var] = np.around(ds[var]).astype(int)
        elif var[-3:] == "raw":
            ds[var] = np.around(ds[var])
    return ds

def post_process(ds):
    _log.info("start post process")
    ds = add_derived_variables(ds)
    ds = add_uncorr_variables(ds)
    ds = flagger(ds)
    ds = fix_specific_mission(ds)
    ds = salinity_pressure_correction(ds)
    ds = correct_rbr_lag(ds)
    ds = recalc_oxygen(ds)
    ds = remove_jammed_locations(ds)
    ds = process_altimeter(ds)
    ds = filter_territorial_data(ds)
    if "backscatter_scaled" in list(ds):
        ds = calculate_bbp(ds)
    ds = nan_bad_depths(ds)
    ds = correct_locations(ds)
    ds = hydrostatic_depth(ds)
    ds= variables_optimise_dtype(ds)
    ds = ds.sortby("TIME")
    _log.info("complete post process")
    return ds


if __name__ == '__main__':
    import xarray as xr
    ds = xr.open_dataset("/data/data_l0_pyglider/OG_nrt/SEA063/M24/timeseries/sea063_20210920T1145_R.nc")
    post_process(ds)