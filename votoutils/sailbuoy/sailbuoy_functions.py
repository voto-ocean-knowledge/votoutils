import datetime
import gsw
import numpy as np
import pandas as pd
import pynmea2
import xarray as xr
import logging
from votoutils.utilities import utilities, vocabularies
_log = logging.getLogger(__name__)


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

def add_sensors(ds, sensors):
    for sensor_id, serial_dict in sensors.items():
        make_model = serial_dict['make_model']
        if make_model in ['Sailbuoy datalogger', 'Sailbuoy autopilot']:
            continue
        if make_model not in vocabularies.sailbuoy_sensors_vocabs.keys():
            _log.warning(f"could not find sensor {make_model} in vocabs dict. Skipping")
            continue
        sensor_dict = vocabularies.sailbuoy_sensors_vocabs[make_model]
        for key, item in serial_dict.items():
            if key == "make_model":
                continue
            sensor_dict[key] = item
        ds.attrs[sensor_id] = str(sensor_dict)
    return ds


def parse_nbosi(input_dir, output_dir):
    dt = datetime.datetime(1970, 1, 1)
    temperature = []
    conductivity = []
    sample_time = []
    with open(input_dir / "NBOSI.TXT") as infile:
        for line in infile.readlines():
            if "Sensorlog opened" in line:
                dt = datetime.datetime.strptime(line[17:36], "%d.%m.%Y %H:%M:%S")
            try:
                temp, cond, sample_num = line.replace('\n', '').split(' ')
                temperature.append(float(temp))
                conductivity.append(float(cond))
                sample_time.append(dt + datetime.timedelta(0,int(sample_num)/10))
            except:
                continue
    df = pd.DataFrame({'datetime': sample_time, 'CNDC': conductivity, 'TEMP': temperature})
    df = df.set_index("datetime").sort_index()
    out_dir = output_dir
    if not out_dir.exists():
        out_dir.mkdir()
    df.to_parquet(out_dir / "NBOSI.pqt")


def parse_aanderaa_ctd(input_dir, output_dir):
    dt = datetime.datetime(1970, 1, 1)
    temperature = []
    conductivity = []
    sample_time = []
    with open(input_dir / "AADICOND.TXT", encoding="latin") as infile:
        for line in infile.readlines():
            if "Sensorlog opened" in line:
                dt = datetime.datetime.strptime(line[17:36], "%d.%m.%Y %H:%M:%S")
            if 'MEASUREMENT' in line:
                try:
                    __, __, __, __, cond,__, temp = line.replace('\n', '').split('\t')
                    temperature.append(float(temp))
                    conductivity.append(float(cond))
                    sample_time.append(dt)
                except:
                    continue
    df = pd.DataFrame({'datetime': sample_time, 'CNDC': conductivity, 'TEMP': temperature})
    df = df.set_index("datetime").sort_index()
    out_dir = output_dir
    if not out_dir.exists():
        out_dir.mkdir()
    df.to_parquet(out_dir / "AADICOND.pqt")

def parse_aanderaa_adcp(input_dir, output_dir):
    dt = datetime.datetime(1970, 1, 1)
    dicts = []
    measurement_dict = {}
    with open(input_dir / "DCPS.TXT") as infile:
        for line in infile.readlines():

            if "Sensorlog opened" in line:
                dt = datetime.datetime.strptime(line[17:36], "%d.%m.%Y %H:%M:%S")

            if 'MEASUREMENT' in line:
                if measurement_dict:
                    dicts.append(measurement_dict)
                parts = line.replace('\n','').split('\t')[3:]
                if len(parts) % 2 == 1:
                    parts = parts[:-1]
                keys = parts[::2]
                values = parts[1::2]
                measurement_dict = {key:val for key, val in zip(keys, values)}
                measurement_dict['datetime'] = dt
            if line[:4] == 'Cell':
                parts = line.split()
                cell_num = parts[1]
                u = parts[-3]
                v = parts[-1]
                measurement_dict[f"water_u_cell_{cell_num}"] = u
                measurement_dict[f"water_v_cell_{cell_num}"] = v
    if measurement_dict:
        dicts.append(measurement_dict)

    df = pd.DataFrame(dicts)
    df = df.set_index("datetime").sort_index()
    out_dir = output_dir
    if not out_dir.exists():
        out_dir.mkdir()
    df.to_parquet(out_dir / "DCPS.pqt")


def parse_airmar(input_dir, output_dir):
    dt = datetime.datetime(1970, 1, 1)
    messages = {}
    with open(input_dir / "AIRMAR.TXT", encoding="latin") as infile:
        for line in infile.readlines():
            if "Sensorlog opened" in line:
                dt = datetime.datetime.strptime(line[-20:-1], "%d.%m.%Y %H:%M:%S")
            try:
                msg = pynmea2.parse(line, check=True)
            except pynmea2.ParseError:
                continue
            if not msg:
                continue
            if msg.identifier() == "GPGGA,":
                timestamp = msg.data[0]
                if len(timestamp) > 6:
                    dt = datetime.datetime(
                        dt.year,
                        dt.month,
                        dt.day,
                        int(timestamp[:2]),
                        int(timestamp[2:4]),
                        int(timestamp[4:6]),
                    )
            talker = line.split(",")[0].replace('$', '').replace(' ', '')
            if not talker.isprintable():
                continue
            if talker == "WIXDR":
                talker += line.split(",")[1]
            elif talker == "WIMWV":
                talker += line.split(",")[2]
            elif talker =="YXXDR" and "RAT" in line:
                talker += "A_RATE"
            elif talker == "YXXDR":
                talker += f'_{line.split(",")[1]}'
            if talker not in messages.keys():
                messages[talker] = []
            messages[talker].append(f"{dt},{line}")
    nmea_dir = output_dir / "AIRMAR"
    if not nmea_dir.exists():
        nmea_dir.mkdir(parents=True)
    for talker, lines in messages.items():
        if not lines:
            continue
        with open(nmea_dir / f"{talker}.txt", mode="w") as outfile:
            outfile.writelines(lines)
    df_merged = pd.DataFrame()
    talker_fields_dicts = {
        "GPGGA": ["datetime", "talker", "timestamp", "lat_str", "northing_sign", "lon_str", "easting_sign", "d",
                  "n", "e", "f", "g", "m", "i", "j", "k", "l", ],
        "GPVTG": ["datetime", "talker", "track_degrees_true", "T", "track_degrees_magnetic", "M",
                  "track_speed_knots", "a", "b", "c", "d"],
        "TIROT": ["datetime", "talker", "rate_of_turn_degrees_per_minute", "a", "b"],
        "WIMDA": ["datetime", "talker", "a", "b", "air_pressure_bar", "c", "air_temperature_celcius", "d",
                  "water_temperature_c", "e", "relative_humidity_%", "absolute_humidity_%", "h", "i",
                  "wind_direction_true_degrees", "j", "wind_direction_magnetic_degrees", "k", "l", "m",
                  "wind_speed_m_s", "n", "o"],
        "WIMWVR": ["datetime", "talker", "wind_angle_relative", "b", "wind_speed", "wind_speed_units", "e"],
        "YXXDR_A": ["datetime", "talker", "a", "pitch_degrees", "b", "c", "d", "roll_degrees", "e", "f"],
    }
    for talker_file in nmea_dir.glob("*.txt"):
        talker = talker_file.name.split('.')[0]
        if talker not in talker_fields_dicts.keys():
            continue
        names = talker_fields_dicts[talker]
        df = pd.read_csv(talker_file, names=names, parse_dates=["datetime"])
        keep_names = [name for name in names if len(name) > 1 and name not in ["talker"]]
        df = df[keep_names]
        if talker == "GPGGA":
            lat_sign = np.ones(len(df.lat_str))
            lat_sign[df['northing_sign'] == 'S'] = -1
            df['LATITUDE'] = coord_to_decimal(df['lat_str'].astype(float)) * lat_sign
            lon_sign = np.ones(len(df.lon_str))
            lon_sign[df['easting_sign'] == 'W'] = -1
            df['LONGITUDE'] = coord_to_decimal(df['lon_str'].astype(float)) * lon_sign
            df = df.drop(['lon_str', 'lat_str', "northing_sign", "easting_sign"], axis=1)
        df = df.set_index("datetime").sort_index()
        if df_merged.empty:
            df_merged = df
            continue
        df_merged = pd.merge_asof(
            df_merged,
            df,
            left_index=True,
            right_index=True,
            tolerance=datetime.timedelta(0, 5)
        )
    df_merged.to_parquet(output_dir/ "AIRMAR.pqt")


def coord_to_decimal(coord_in):
    # convert from kongsberg DDMM.mm to decimal degrees DD.dd
    if np.isnan(coord_in):
        return np.nan
    coord_in = coord_in / 100
    deg = int(coord_in)
    minutes = coord_in - deg
    decimal_degrees = deg + minutes / 0.6
    return decimal_degrees


coord_to_decimal = np.vectorize(coord_to_decimal)


def parse_data(data_dir, output_dir):
    file_in = data_dir / "DATA.TXT"
    data_lines = []
    with open(file_in) as fin:
        for line in fin.readlines():
            if line[:4] == "Time":
                data_lines.append(line)
    dicts_list = []
    for line in data_lines:
        data_dict = {}
        line = line.replace(' ', '').replace('\n', '')
        items = line.split(',')
        for item in items:
            if '=' not in item:
                continue
            key, val = item.split('=')
            if val == "NULL":
                val = ""
            if val:
                data_dict[key] = val
        dicts_list.append(data_dict)
    df = pd.DataFrame(dicts_list)
    df["datetime"] = pd.to_datetime(df.Time, format="%d.%m.%Y%H:%M:%S")
    df = df.drop(['Time'], axis=1)
    df = df.set_index("datetime").sort_index()

    clean_names_data = {
        "Lat": "LATITUDE",
        "Long": "LONGITUDE",
        "RBRL_T": "TEMP",
        "RBRL_C": "CNDC",
        "RBRL_Sal": "PSAL",
        "CTTemp": "TEMP",
        "WindDirection": "WIND_DIRECTION",
        "WindSpeed": "WIND_SPEED",
        "WindGust": "WIND_GUST",
        "AirTemp": "TEMP_AIR",
        "AirPressure": "PRESSURE_AIR",
        "RelativeHumidity": "HUMIDITY",
    }
    df = df.rename(clean_names_data, axis=1)
    df= df[list(set(df.columns).intersection(set(clean_names_data.values())))]
    df.to_parquet(output_dir / "Data_logger.pqt")


def parse_nrt(auto_dir, output_dir):
    auto_csv = auto_dir / "DATA.TXT"
    df = pd.read_csv(auto_csv, skiprows=1)
    df = pd.read_csv(auto_csv, skiprows=1, names=np.arange(df.shape[1]))
    og_cols = df.columns.copy()
    for col_name in og_cols[:-1]:
        col = df[col_name]
        item = col[0]
        new_name = item.split(" = ")[0]
        df[new_name] = col.str[len(new_name) + 3 :]
    df = df.drop(og_cols, axis=1)
    df = df.replace("NULL ", "NaN")
    df = df.dropna()
    df["Time"] = pd.to_datetime(df.Time, dayfirst=True)
    for col_name in df.columns:
        if col_name in ["Time"]:
            continue
        df[col_name] = df[col_name].astype(float)
    for col_name in df.columns:
        if col_name in ["Time"]:
            continue
        try:
            if len(df[col_name].unique()) == len(df[col_name].astype(int).unique()):
                df[col_name] = df[col_name].astype(int)
        except:
            continue
    auto = df.set_index("Time")

    df = pd.read_csv(auto_csv, skiprows=1)
    df = pd.read_csv(auto_csv, skiprows=1, names=np.arange(df.shape[1]))
    og_cols = df.columns.copy()
    for col_name in og_cols[:-1]:
        col = df[col_name]
        item = col[0]
        new_name = item.split(" = ")[0]
        df[new_name] = col.str[len(new_name) + 3 :]
    df = df.drop(og_cols, axis=1)
    df = df.replace("NULL ", "NaN")
    df = df.dropna()
    df["Time"] = pd.to_datetime(df.Time, dayfirst=True)
    for col_name in df.columns:
        if col_name in ["Time"]:
            continue
        df[col_name] = df[col_name].astype(float)
    for col_name in df.columns:
        if col_name in ["Time"]:
            continue
        try:
            if len(df[col_name].unique()) == len(df[col_name].astype(int).unique()):
                df[col_name] = df[col_name].astype(int)
        except:
            continue
    data = df.set_index("Time")
    if {"Hs", "Ts", "T0", "Hmax", "Err"}.issubset(set(list(df))):
        mose_nrt = (
            data[["Hs", "Ts", "T0", "Hmax", "Err"]]
            .rename(
                {
                    "Hs": "significant_wave_height",
                    "Ts": "significant_wave_period",
                    "T0": "mean_wave_period",
                    "Hmax": "maximum_wave_height",
                    "Err": "percentage_error_lines",
                },
                axis=1,
            )
            .dropna(
                subset=["significant_wave_height"],
            )
        )
        mose_nrt.to_parquet(output_dir / "mose_nrt.pqt")
    df_nrt = data.join(auto, how="outer", lsuffix="_x", rsuffix="_y")
    vars_to_merge = [var_name[:-2] for var_name in list(df_nrt) if var_name[-2:] == '_x']
    for var_base in vars_to_merge:
        _log.info(f"merging {var_base}")
        df_nrt.loc[np.isnan(df_nrt[var_base + '_x'].astype(float)), var_base + '_x'] = df_nrt.loc[
            np.isnan(df_nrt[var_base + '_x'].astype(float)), var_base + '_y'].astype(float)
        df_nrt = df_nrt.rename({var_base + '_x': var_base}, axis=1)
        df_nrt = df_nrt.drop([var_base + '_y'], axis=1)
        df_nrt[var_base] = df_nrt[var_base].astype(float)

    clean_names_auto = {
        "Lat": "LATITUDE",
        "Long": "LONGITUDE",
    }
    df_nrt = df_nrt.rename(clean_names_auto, axis=1)
    df_nrt = df_nrt[list(set(df_nrt.columns).intersection(set(clean_names_auto.values())))]
    
    
    df_nrt.to_parquet(output_dir / "Auto_pilot.pqt")


def parse_legato(input_dir, intermediate_dir):
    infile_rsk = input_dir / "LEGATO_rsk.nc"
    infile_logger = input_dir / "LEGATO.TXT"
    if not infile_logger.exists() or not infile_rsk.exists():
        _log.error(f"Did not find expected LEGATO files in {input_dir}")
        return
    logger_datetime = None
    ctd_datetime = None
    datetime_diff = None
    with open(infile_logger, encoding="latin") as infile:
        for line in infile.readlines():
            if logger_datetime and ctd_datetime:
                datetime_diff = logger_datetime - ctd_datetime
                _log.info(f"datetime difference between logger and legato: {datetime_diff}")
                break
            if "Sensorlog opened" in line:
                line_parts = line.split(" opened ")
                time_string = line_parts[1][:19]
                logger_datetime = datetime.datetime.strptime(time_string, "%d.%m.%Y %H:%M:%S")
            if line[:2] == "20":
                line_parts = line.split(",")
                time_string = line_parts[0][:19]
                ctd_datetime = datetime.datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")
    if not datetime_diff:
        _log.error(f"Could not match legato datestamps for {input_dir}")
        return
    ds = xr.open_dataset(infile_rsk)
    if datetime_diff > datetime.timedelta(seconds=5):
        _log.info("Correcting legato timestamps")
        ds['time'] = ds['time'] + (np.datetime64(logger_datetime, 'ns') - np.datetime64(ctd_datetime, 'ns'))
    ds = ds.drop_dims('parameters')
    legato_dict = {
        'conductivity': 'CNDC',
        'temperature': 'TEMP',
        'salinity': 'PSAL',
        'pressure': 'PRES'
    }
    ds = ds.rename(legato_dict)
    ds = ds[list(legato_dict.values())]
    df = ds.to_pandas()
    df.to_parquet(intermediate_dir / "LEGATO.pqt")


def parse_gmx560(input_dir, output_dir):
    dt = datetime.datetime(1970, 1, 1)
    messages = {
        "GPGGA": [],
        "PGILT": [],
        "WIHDM": [],
        "WIMWVR": [],
        "WIMWVT": [],
        "WIXDRC": [],
        "WIXDRA": [],
    }
    with open(input_dir / "GMX560.TXT", encoding="latin") as infile:
        for line in infile.readlines():
            if "Sensorlog opened" in line:
                dt = datetime.datetime.strptime(line[-20:-1], "%d.%m.%Y %H:%M:%S")
            try:
                msg = pynmea2.parse(line, check=True)
            except pynmea2.ParseError:
                continue
            if not msg:
                continue
            if msg.identifier() == "GPGGA,":
                timestamp = msg.data[0]
                if len(timestamp) > 6:
                    dt = datetime.datetime(
                        dt.year,
                        dt.month,
                        dt.day,
                        int(timestamp[:2]),
                        int(timestamp[2:4]),
                        int(timestamp[4:6]),
                    )
            talker = line.split(",")[0][1:]
            if talker == "WIXDR":
                talker += line.split(",")[1]
            if talker == "WIMWV":
                talker += line.split(",")[2]
            messages[talker].append(f"{dt},{line}")
    gmx_dir = output_dir / "GMX560"
    if not gmx_dir.exists():
        gmx_dir.mkdir(parents=True)
    for talker, lines in messages.items():
        with open(gmx_dir / f"{talker}.txt", mode="w") as outfile:
            outfile.writelines(lines)

    df_wind_rel = pd.read_csv(
        gmx_dir / "WIMWVR.txt",
        names=[
            "datetime",
            "talker",
            "wind_direction_relative",
            "relative",
            "windspeed_relative",
            "unit",
            "acceptable_measurement",
        ],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_wind_rel = df_wind_rel[df_wind_rel["acceptable_measurement"].str[0] == "A"][
        ["wind_direction_relative", "windspeed_relative"]
    ]
    df_wind_true = pd.read_csv(
        gmx_dir / "WIMWVT.txt",
        names=[
            "datetime",
            "talker",
            "wind_direction_true",
            "relative",
            "windspeed_true",
            "unit",
            "acceptable_measurement",
        ],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_wind_true = df_wind_true[df_wind_true["acceptable_measurement"].str[0] == "A"][
        ["wind_direction_true", "windspeed_true"]
    ]
    df_heading = pd.read_csv(
        gmx_dir /"WIHDM.txt",
        names=["datetime", "talker", "heading_magnetic", " magnetic", "checksum"],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_heading = df_heading[["heading_magnetic"]]
    df_attitude = pd.read_csv(
        gmx_dir / "WIXDRA.txt",
        names=["datetime", "talker", "a", "pitch", "b", "c", "d", "roll", "e", "f"],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_attitude = df_attitude[["pitch", "roll"]]
    df_weather = pd.read_csv(
        gmx_dir / "WIXDRC.txt",
        names=[
            "datetime",
            "talker",
            "a",
            "air_temperature",
            "b",
            "c",
            "d",
            "air_pressure",
            "e",
            "f",
            "g",
            "humidity_%",
            "i",
            "j",
        ],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_weather = df_weather[["air_temperature", "air_pressure", "humidity_%"]]
    df_weather_gps = pd.read_csv(
        gmx_dir / "GPGGA.txt",
        names=[
            "datetime",
            "talker",
            "timestamp",
            "lat_str",
            "b",
            "lon_str",
            "d",
            "n",
            "e",
            "f",
            "g",
            "m",
            "i",
            "j",
            "k",
            "l",
        ],
        parse_dates=["datetime"],
    ).set_index(
        "datetime",
    )
    df_weather_gps = df_weather_gps[["timestamp", "lat_str", "lon_str"]]
    df_tilt = pd.read_csv(
        gmx_dir / "PGILT.txt",
        names=[
            "datetime",
            "talker",
            "a",
            "eastward_tilt",
            "b",
            "northward_tilt",
            "d",
            "vertical_orientation",
            "e",
        ],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_tilt = df_tilt[["eastward_tilt", "northward_tilt", "vertical_orientation"]]

    gmx_name_dict = {
        "wind_direction_true": "WIND_DIRECTION",
        "windspeed_true": "WIND_SPEED",
        "wind_direction_true_degrees": "WIND_DIRECTION",
        "wind_speed_m_s": "WIND_SPEED",
        "air_temperature_celcius": "TEMP_AIR",
        "air_temperature": "TEMP_AIR",
        "air_pressure": "PRESSURE_AIR",
        "air_pressure_bar": "PRESSURE_AIR",
        "humidity_%": "HUMIDITY",
        "relative_humidity_%": "HUMIDITY",
        "pitch": "PITCH",
        "roll": "ROLL",
        "heading_magnetic": "HEADING",
    }

    df_gmx = pd.DataFrame()
    for df_add in [
        df_wind_rel,
        df_wind_true,
        df_heading,
        df_attitude,
        df_weather,
        df_weather_gps,
        df_tilt,
    ]:
        df_add = df_add.sort_index().rename(gmx_name_dict, axis=1)
        df_add = df_add[list(set(df_add.columns).intersection(set(gmx_name_dict.values())))]
        if df_add.empty:
            continue
        if df_gmx.empty:
            df_gmx = df_add
            continue
        df_gmx = pd.merge_asof(
            df_gmx,
            df_add,
            left_index=True,
            right_index=True,
            direction="nearest",
            tolerance=pd.Timedelta("1s"),
        )
    df_gmx.to_parquet(output_dir /"MAXIMET.pqt")


def parse_mose(indir, intermediate_dir):
    mose_dir = intermediate_dir / "MOSE"
    if not mose_dir.exists():
        mose_dir.mkdir(parents=True)
    with open(indir / "MOSE.TXT", encoding="latin") as infile:
        with open(mose_dir / "mose_good.txt", "w") as outfile:
            with open(mose_dir / "mose_loc.txt", "w") as locfile:
                for line in infile.readlines():
                    try:
                        msg = pynmea2.parse(line, check=True)
                        if msg.data[1] == "MOT" and msg.data[3] != "80":
                            goodline = line.replace(" ", "")[:-4] + "\n"
                            outfile.write(goodline)
                        if msg.data[1] == "POS":
                            goodline = line.replace(" ", "")[:-4] + "\n"
                            locfile.write(goodline)
                    except pynmea2.ParseError:
                        continue
    mose = pd.read_csv(
        mose_dir / "mose_good.txt",
        names=[
            "manufactuer",
            "sentence_type",
            "frequency",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "vert_m",
            "north_m",
            "west_m",
            "flag",
        ],
        encoding="latin",
    )
    mose["year"] = 2000 + mose["year"]
    mose["datetime"] = pd.to_datetime(
        mose[["year", "month", "day", "hour", "minute", "second"]],
    )
    mose = mose[mose["flag"] == 0]  # remove bad flagged data (from mose manual)
    mose = (
        mose[["frequency", "datetime", "vert_m", "north_m", "west_m"]]
        .set_index("datetime")
        .sort_index()
    )
    mose_high_freq = mose[mose["frequency"] == "HF"][["vert_m", "north_m", "west_m"]]
    mose_loc = pd.read_csv(
        mose_dir / "mose_loc.txt",
        names=[
            "manufactuer",
            "sentence_type",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "lat_deg",
            "lat_min",
            "lat_dir",
            "lon_deg",
            "lon_min",
            "lon_dir",
            "height",
            "hdop",
            "vdop",
        ],
        encoding="latin",
    )
    mose_loc["year"] = 2000 + mose_loc["year"]
    mose_loc["datetime"] = pd.to_datetime(
        mose_loc[["year", "month", "day", "hour", "minute", "second"]],
    )
    mose_loc = mose_loc.set_index("datetime").sort_index()
    mose_loc["lon"] = mose_loc["lon_deg"] + mose_loc["lon_min"] / 60
    mose_loc["lat"] = mose_loc["lat_deg"] + mose_loc["lat_min"] / 60
    mose_loc = mose_loc[mose_loc["height"] > -100]
    mose_loc = mose_loc[["lat", "lon", "height", "hdop", "vdop"]]
    df_mose = mose_high_freq.join(mose_loc, how="outer")
    clean_names_mose = {
        "lat": "LATITUDE",
        "lon": "LONGITUDE",
        "height": "HEIGHT",
        "vert_m": "vertical_displacement",
        "north_m": "northward_displacement",
        "west_m": "westward_displacement",
    }
    df_mose = df_mose.rename(clean_names_mose, axis=1)
    df_mose = df_mose[list(set(df_mose.columns).intersection(set(clean_names_mose.values())))]
    df_mose.to_parquet(intermediate_dir / "MOSE.pqt")


def merge_sensors():
    df_legato = pd.read_parquet("intermediate_data/legato.pqt")
    df_mose = pd.read_parquet("intermediate_data/mose.pqt")
    df_gmx = pd.read_parquet("intermediate_data/gmx.pqt")
    df_mose_nrt = pd.read_parquet("intermediate_data/mose_nrt.parquet")
    df_delayed = df_legato.sort_index()
    df_delayed = pd.merge_asof(
        df_delayed,
        df_mose,
        left_index=True,
        right_index=True,
        direction="nearest",
        tolerance=pd.Timedelta("1s"),
    )
    df_delayed = pd.merge_asof(
        df_delayed,
        df_gmx,
        left_index=True,
        right_index=True,
        direction="nearest",
        tolerance=pd.Timedelta("1s"),
    )
    df_delayed = pd.merge_asof(
        df_delayed,
        df_mose_nrt,
        left_index=True,
        right_index=True,
        direction="nearest",
        tolerance=pd.Timedelta("1s"),
    )
    df_delayed = df_delayed[
        [
            "Conductivity",
            "Temperature",
            "pressure_legato",
            "vert_m",
            "north_m",
            "west_m",
            "lat",
            "lon",
            "wind_direction_relative",
            "windspeed_relative",
            "wind_direction_true",
            "windspeed_true",
            "heading_magnetic",
            "pitch",
            "roll",
            "air_temperature",
            "air_pressure",
            "humidity_%",
            "significant_wave_height",
            "significant_wave_period",
            "mean_wave_period",
            "maximum_wave_height",
            "percentage_error_lines",
        ]
    ]

    df_delayed.to_parquet("data_out/delayed.pqt")


def export_netcdf(output_dir, yml_dict):
    df = pd.read_parquet(output_dir / "delayed.pqt")
    ds = xr.Dataset()
    time_attr = {"name": "time"}
    ds["time"] = ("time", df.index, time_attr)
    platform_serial = yml_dict['metadata']['platform_serial']
    if platform_serial == "SB2017":
        _log.info("adding fake pressure for NBOSI")
        df['PRES'] = 0

    for col_name in list(df):
        try:
            values = df[col_name].astype(float)
        except:
            values = df[col_name]
        if col_name not in vocabularies.vocab_attrs.keys():
            _log.error(f"Did not find attributes for variable {col_name}")
            continue

        ds[col_name] = ("time", values, vocabularies.vocab_attrs[col_name])
    if {'PRES', 'TEMP', 'CNDC'}.issubset(list(ds)):
        ds["PSAL"] = ("time",
                      gsw.SP_from_C(ds.CNDC.values, ds.TEMP.values, ds.PRES.values),
                      vocabularies.vocab_attrs["PSAL"],
                      )
    ds = get_attrs(ds, platform_serial)
    ds = add_sensors(ds, yml_dict['devices'])
    ds.attrs["variables"] = list(ds.variables)
    ds["trajectory"] = xr.DataArray(1, attrs={"cf_role": "trajectory_id"})
    outfile = output_dir / f"{ds.attrs['id']}.nc"
    ds.to_netcdf(outfile)
    _log.info(f'Wrote nc out to {outfile}')
    for var_name in ds.variables:
        if var_name in ["trajectory", 'time']:
            continue
        good = len(ds[var_name][~np.isnan(ds[var_name])])
        _log.info(f"non nan values for {var_name}: {round(100 * good / len(ds[var_name]), 2)} %")

def merge_intermediate(intermediate_dir, output_dir):
    input_files = intermediate_dir.glob("*.pqt")
    df_merged = pd.DataFrame()
    for infile in input_files:
        df = pd.read_parquet(infile)
        if df_merged.empty:
            df_merged = df
            continue
        df_merged = df_merged.merge(
            df,
            left_index=True,
            right_index=True,
            how='outer'
        )
        vars_to_merge = [var_name[:-2] for var_name in list(df_merged) if var_name[-2:] == '_x']
        for var_base in vars_to_merge:
            _log.info(f"merging {var_base}")
            df_merged.loc[np.isnan(df_merged[var_base + '_x'].astype(float)), var_base + '_x'] = df_merged.loc[
                np.isnan(df_merged[var_base + '_x'].astype(float)), var_base + '_y'].astype(float)
            df_merged = df_merged.rename({var_base + '_x': var_base}, axis=1)
            df_merged = df_merged.drop([var_base + '_y'], axis=1)
            df_merged[var_base] = df_merged[var_base].astype(float)
    df_merged.to_parquet(output_dir / "delayed.pqt")

