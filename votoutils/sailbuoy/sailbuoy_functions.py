import pandas as pd

from votoutils.utilities import utilities, vocabularies


def get_attrs(ds, platform_serial_number, postscript="delayed"):
    ds = utilities.add_standard_global_attrs(ds)
    attrs = {
        "platform": "autonomous surface water vehicle",
        "platform_vocabulary": "https://vocab.nerc.ac.uk/collection/L06/current/3B/",
        "platform_serial": platform_serial_number,
        "area": "Baltic Sea",
        "cdm_data_type": "TrajectoryProfile",
        "keywords": "CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, "
        "Conductivity, Density, Salinity",
        "keywords_vocabulary": "GCMD Science Keywords",
        "title": "Sailbuoy data from the Baltic",
        "QC_indicator": "L1",
    }
    ts = pd.to_datetime(ds.attrs["time_coverage_start"]).strftime("%Y%m%dT%H%M")
    attrs["id"] = f"{attrs['platform_serial']}_{ts}_{postscript}"
    for key, val in attrs.items():
        if key in ds.attrs.keys():
            continue
        ds.attrs[key] = val
    return ds


clean_names_nrt = {
     'Lat': 'LATITUDE',
     'Long': 'LONGITUDE',
     'RBRL_T': 'TEMP',
     'RBRL_C': 'CNDC',
     'RBRL_Sal': 'PSAL',
}

clean_names = {
    "lat": "LATITUDE",
    "lon": "LONGITUDE",
    "time": "TIME",
    "pitch": "PITCH",
    "roll": "ROLL",
    "heading_magnetic": "HEADING",
    "pressure_legato": "PRES",
    "Conductivity": "CNDC",
    "oxygen_concentration": "DOXY",
    "chlorophyll": "CHLA",
    "Temperature": "TEMP",
    "wind_direction_true": "WIND_DIRECTION",
    "windspeed_true": "WIND_SPEED",
    "air_temperature": "TEMP_AIR",
    "air_pressure": "PRESSURE_AIR",
    "humidity_%": "HUMIDITY",
    "significant_wave_height": "significant_wave_height",
    "significant_wave_period": "significant_wave_period",
    "mean_wave_period": "mean_wave_period",
    "maximum_wave_height": "maximum_wave_height",
    "percentage_error_lines": "percentage_error_lines",
    "vert_m": "vertical_displacement",
    "north_m": "northward_displacement",
    "west_m": "westward_displacement",
}
sensors = {
    "sensor_ctd": {
        "sensor": "RBR legato CTD",
        "serial_number": 207496,
        "calibration_date": "2021-06-22",
    },
    "sensor_meteorology": {
        "sensor": "Gill Instruments GMX560",
        "serial_number": 24080013,
    },
    "sensor_wave": {
        "sensor": "Datawell MOSE-G1000",
        "serial_number": "unknown",
    },
}


def add_sensors(ds):
    for sensor_id, serial_dict in sensors.items():
        sensor_dict = vocabularies.sensor_vocabs[serial_dict["sensor"]]
        for key, item in serial_dict.items():
            if key == "sensor":
                continue
            sensor_dict[key] = item
        ds.attrs[sensor_id] = str(sensor_dict)
    return ds
