import pandas as pd
import xarray as xr
import numpy as np
import pynmea2
import datetime
import gsw
from pathlib import Path

from votoutils.utilities import utilities
from votoutils.utilities import vocabularies


class Sailbuoy():
    def __init__(self, input_dir=".", base_dir="."):
        self.input_dir = Path(input_dir)
        self.base_dir = Path(base_dir)
        self.output_dir = self.base_dir / "output"
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)

    def parse_nrt(self):
        parse_nrt(self.input_dir / "AUTO", self.output_dir)

    def parse_legato(self):
        parse_legato(self.input_dir, self.output_dir)

    def parse_gmx560(self):
        parse_gmx560(self.input_dir / 'DATA', self.output_dir)


def parse_nrt(auto_dir, output_dir):
    auto_csv = auto_dir / "DATA.TXT"
    df = pd.read_csv(auto_csv, skiprows=1)
    df = pd.read_csv(auto_csv, skiprows=1, names=np.arange(df.shape[1]))
    og_cols = df.columns.copy()
    for col_name in og_cols[:-1]:
        col = df[col_name]
        item = col[0]
        new_name = item.split(" = ")[0]
        df[new_name] = col.str[len(new_name) + 3:]
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
        except TypeError:
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
        except TypeError:
            continue
    data = df.set_index("Time")
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
    mose_nrt.to_parquet(output_dir/ "mose_nrt.parquet")
    df_nrt = data.join(auto, how="outer", lsuffix="_data", rsuffix="_auto")
    df_nrt.to_csv(output_dir / "nrt.csv")


def parse_legato(input_dir, output_dir):
    df_legato = (
        pd.read_csv(list((input_dir / "LEGATO").glob("*data.txt"))[0], parse_dates=["Time"])
        .set_index(
            "Time",
        )
        .rename({"Pressure": "pressure_legato"}, axis=1)
    )
    df_legato.to_parquet(output_dir / "legato.pqt")


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
        with open(output_dir / "GMX560" f"{talker}.txt", mode="w") as outfile:
            outfile.writelines(lines)


def merge_gmx560():
    df_wind_rel = pd.read_csv(
        "intermediate_data/GMX560/WIMWVR.txt",
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
        "intermediate_data/GMX560/WIMWVT.txt",
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
        "intermediate_data/GMX560/WIHDM.txt",
        names=["datetime", "talker", "heading_magnetic", " magnetic", "checksum"],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_heading = df_heading[["heading_magnetic"]]
    df_attitude = pd.read_csv(
        "intermediate_data/GMX560/WIXDRA.txt",
        names=["datetime", "talker", "a", "pitch", "b", "c", "d", "roll", "e", "f"],
        parse_dates=["datetime"],
    ).set_index("datetime")
    df_attitude = df_attitude[["pitch", "roll"]]
    df_weather = pd.read_csv(
        "intermediate_data/GMX560/WIXDRC.txt",
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
        "intermediate_data/GMX560/GPGGA.txt",
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
        "intermediate_data/GMX560/PGILT.txt",
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

    df_gmx = df_wind_rel.sort_index()
    for df_add in [
        df_wind_true,
        df_heading,
        df_attitude,
        df_weather,
        df_weather_gps,
        df_tilt,
    ]:
        df_add = df_add.sort_index()
        df_gmx = pd.merge_asof(
            df_gmx,
            df_add,
            left_index=True,
            right_index=True,
            direction="nearest",
            tolerance=pd.Timedelta("1s"),
        )
    df_gmx.to_parquet("intermediate_data/gmx.pqt")


def parse_mose():
    with open("SB2120/DATA/MOSE.TXT", encoding="latin") as infile:
        with open("intermediate_data/mose_good.txt", "w") as outfile:
            with open("intermediate_data/mose_loc.txt", "w") as locfile:
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
                        # print('Parse error: {}'.format(e))
                        continue


def merge_mose():
    mose = pd.read_csv(
        "intermediate_data/mose_good.txt",
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
        "intermediate_data/mose_loc.txt",
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
    df_mose.to_parquet("intermediate_data/mose.pqt")


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


def get_attrs(ds):
    ds = utilities.add_standard_global_attrs(ds)
    attrs = {
        "platform": "autonomous surface water vehicle",
        "platform_vocabulary": "https://vocab.nerc.ac.uk/collection/L06/current/3B/",
        "platform_serial_number": "SB2120",
        "area": "Baltic Sea",
        "cdm_data_type": "TrajectoryProfile",
        "keywords": "CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, "
        "Conductivity, Density, Salinity",
        "keywords_vocabulary": "GCMD Science Keywords",
        "title": "Sailbuoy data from the Baltic",
        "QC_indicator": "L1",
    }
    ts = pd.to_datetime(ds.attrs["time_coverage_start"]).strftime("%Y%m%dT%H%M")
    postscript = "delayed"
    attrs["id"] = f"{attrs['platform_serial_number']}_{ts}_{postscript}"
    for key, val in attrs.items():
        if key in ds.attrs.keys():
            continue
        ds.attrs[key] = val
    return ds


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


def export_dataset():
    df = pd.read_parquet("data_out/delayed.pqt")
    ds = xr.Dataset()
    time_attr = {"name": "time"}
    ds["time"] = ("time", df.index, time_attr)

    for col_name in list(df):
        if col_name in clean_names.keys():
            name = clean_names[col_name]
            ds[name] = ("time", df[col_name], vocabularies.vocab_attrs[name])
        else:
            print(f"fail for {col_name}")
    ds['PSAL'] = xr.DataArray('N_MEASUREMENTS', gsw.SP_from_C(ds.CNDC, ds.TEMP, ds.PRES), vocabularies.vocab_attrs['PSAL'])
    # cut dataset down to active deployed period
    start = "2024-05-29T09:00:00"
    end = "2024-07-28T06:00:00"
    ds = ds.sel(time=slice(start, end))
    ds = get_attrs(ds)
    ds = add_sensors(ds)
    ds.attrs["variables"] = list(ds.variables)
    ds["trajectory"] = xr.DataArray(1, attrs={"cf_role": "trajectory_id"})
    ds.to_netcdf(f"data_out/{ds.attrs['id']}.nc")
    # ds = ds.sel(time=slice(start, "2024-05-30T09:00:00"))
    ds.to_netcdf(
        f"/home/callum/Documents/erddap/local_dev/erddap-gold-standard/datasets/{ds.attrs['id']}.nc",
    )


if __name__ == "__main__":
    sb = Sailbuoy("/home/callum/Documents/data-flow/sailbuoy/process/SB2120", "/home/callum/Downloads/tmpsb")
    sb.parse_legato()
    sb.parse_nrt()
    sb.parse_gmx560()
    export_dataset()
