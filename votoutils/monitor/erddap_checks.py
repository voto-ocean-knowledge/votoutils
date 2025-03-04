import httpx
import numpy as np
import datetime
from erddapy import ERDDAP
import geopandas as gp
import pandas as pd
from votoutils.utilities.utilities import mailer
import logging

_log = logging.getLogger(__name__)


def enough_datasets(df_datasets):
    _log.info("Check enough datasets")
    total = int(len(df_datasets))
    nrt = int(sum(df_datasets.index.str[:3] == "nrt"))
    complete = int(sum(df_datasets.index.str[:3] == "del"))

    try:
        total_datasets = pd.read_csv("total_datasets.csv")
    except FileNotFoundError:
        total_datasets = pd.DataFrame(
            {"date": [], "total": [], "nrt": [], "delayed": []},
        )
    new_row = pd.DataFrame(
        {
            "date": datetime.datetime.now(),
            "total": total,
            "nrt": nrt,
            "delayed": complete,
        },
        index=[len(total_datasets)],
    )
    if total < total_datasets.total.max():
        mailer("cherddap", "total datasets decreased")
    total_datasets = pd.concat((total_datasets, new_row))
    total_datasets.to_csv("total_datasets.csv", index=False)


def nrt_vs_complete(df_datasets):
    _log.info("Check nrt vs complete")
    mtype, glider, mission = [], [], []
    for gm in df_datasets.index:
        mission_type, g, m = gm.split("_")
        mtype.append(mission_type)
        glider.append(int(g[3:]))
        mission.append(int(m[1:]))
    df_datasets["glider"] = glider
    df_datasets["mission_type"] = mtype
    df_datasets["mission"] = mission

    df_nrt = df_datasets[df_datasets.mission_type == "nrt"]
    df_nrt.index = "SEA" + df_nrt.glider.astype(str) + "_M" + df_nrt.mission.astype(str)
    df_delayed = df_datasets[df_datasets.mission_type == "delayed"]
    df_delayed.index = (
        "SEA" + df_delayed.glider.astype(str) + "_M" + df_delayed.mission.astype(str)
    )

    expected_fails = ["SEA61_M63", "SEA67_M15", "SEA66_M45", "SEA57_M75", "SEA70_M29"]
    for this_dataset in df_nrt.index:
        if this_dataset in expected_fails:
            continue
        if this_dataset not in df_delayed.index:
            # print(f"{this_dataset} not found in delayed")
            time_since = (
                datetime.datetime.now(
                    tz=datetime.timezone(datetime.timedelta(minutes=0)),
                )
                - df_nrt.loc[this_dataset]["maxTime (UTC)"]
            )
            # print("last update: ", time_since)
            if time_since > datetime.timedelta(days=7):
                msg = f"unprocessed complete dataset: {this_dataset}. {time_since} since last nrt data"
                mailer("cherddap", msg)

    for this_dataset in df_delayed.index:
        if this_dataset not in df_nrt.index:
            mailer(
                "cherdap",
                f"{this_dataset} not found in nrt. Last update:{df_delayed.loc[this_dataset]['maxTime (UTC)']}",
            )
    return df_datasets


def bad_depths(df_datasets):
    _log.info("Check bad depths")
    if len(df_datasets[df_datasets["maxAltitude (m)"] < -2000]["maxAltitude (m)"]) > 0:
        mailer(
            "cherddap",
            f"bad altitude (glider depth): {list(df_datasets[df_datasets['maxAltitude (m)'] < -2000]['maxAltitude (m)'].index)}",
        )


def bad_dataset_id(df_datasets):
    _log.info("Check bad datasets ids")
    # Find missions where the dataset ID number doesn't match the name. This indicates the wrong dataset has been loaded
    for dataset_id, row in df_datasets.iterrows():
        name = row["title"]
        if "adcp" in dataset_id:
            # print("skip namecheck for adcp data")
            continue
        if " " in name and "King" not in name:
            num_title = int(name[3:6])
        else:
            num_title = int(name.split("-")[0][-2:])
        num_id = int(dataset_id.split("_")[1][-3:])
        if num_id != num_title:
            mailer("cherddap", f"bad dataset name:  {name}, {dataset_id}")


def profile_num_vs_dive_num(e, dataset_id):
    _log.info(f"Check profile num vs dive num {dataset_id}")
    e.variables = [
        "time",
        "pressure",
        "depth",
        "dive_num",
        "profile_direction",
        "profile_index",
        "profile_num",
    ]
    e.dataset_id = dataset_id
    ds = e.to_xarray(requests_kwargs={"timeout": 300})
    ds = ds.drop_dims("timeseries")
    ds_sort = ds.sortby("time")
    if not ds.time.equals(ds_sort.time):
        mailer("cherddap", "datasets not sorted by time")
    profiles = len(np.unique(ds.profile_num))
    dives = len(np.unique(ds.dive_num))
    if abs(profiles / 2 - dives) > 3:
        mailer(
            "cherddap",
            f"missmatch between {dataset_id} profile_num {profiles} and dive_num {dives} ({dives * 2})",
        )


def sensible_values(e, dataset_id):
    _log.info(f"Check sensible values {dataset_id}")
    e.dataset_id = dataset_id

    ds = e.to_xarray(requests_kwargs={"timeout": 300})
    ds = ds.drop_dims("timeseries")
    for var in list(ds.variables):
        mini, maxi = np.nanmin(ds[var].values), np.nanmax(ds[var].values)
        low_lim = -1e3
        high_lim = 1e4
        if isinstance(mini, np.datetime64):
            low_lim = np.datetime64("2020-01-31T09:02:47.457999872")
            high_lim = np.datetime64("2030-01-31T09:02:47.457999872")
        if var == "internal_pressure":
            high_lim = 1e5
        if var == "desired_heading":
            low_lim = -1e5
        if mini < low_lim:
            mailer("cherddap", f"{dataset_id} bad var {var} minimum {mini}")
        if maxi > high_lim:
            mailer("cherddap", f"{dataset_id} bad var {var} maximum {maxi}")


def unit_check(e, dataset_id):
    _log.info(f"Check units {dataset_id}")
    e.dataset_id = dataset_id

    ds = e.to_xarray(requests_kwargs={"timeout": 300})
    ds = ds.drop_dims("timeseries")
    for var in list(ds.variables):
        attrs = ds[var].attrs
        if var == "oxygen_concentration":
            if attrs["units"] != "mmol m-3":
                mailer("cherddap", f"bad oxy units {attrs['units']}")


def adcp_dataset_check(df):
    meta = pd.read_csv("https://erddap.observations.voiceoftheocean.org/erddap/tabledap/meta_users_table.csvp")
    adcp = pd.read_csv("https://erddap.observations.voiceoftheocean.org/erddap/tabledap/ad2cp.csvp")
    meta_with_adcp = meta[meta['available_variables'].str.contains("ad2cp_pressure")]
    delayed_missions = df.index[df.index.str[:11]=='delayed_SEA'].str[8:].values
    missions_with_adcp = meta_with_adcp['datasetID'].str[4:].values
    expected_adcp_missions = set(delayed_missions).intersection(missions_with_adcp)
    adcp_files = set(adcp.name.str.split('.', expand=True)[0].values)
    missing_adcp = expected_adcp_missions.difference(adcp_files)
    expected_missing_adcp = ['SEA066_M16']
    bad_missing_adcp = missing_adcp.difference(expected_missing_adcp)
    if len(bad_missing_adcp) > 0:
        mailer("cherddap", f"missing expected adcp files {bad_missing_adcp}")


def international_waters_check(e, dataset_id):
    _log.info(f"Check international waters {dataset_id}")
    e.dataset_id = dataset_id
    e.variables = [
        "longitude",
        "latitude",
        "dive_num",
        "vertical_distance_to_seafloor",
    ]
    df = e.to_pandas()
    df = df.rename(
        {
            "longitude (degrees_east)": "longitude",
            "latitude (degrees_north)": "latitude",
            "dive_num (None)": "dive_num",
            "vertical_distance_to_seafloor (m)": "vertical_distance_to_seafloor",
        },
        axis=1,
    )
    df_glider = (
        df[~np.isnan(df["vertical_distance_to_seafloor"])].groupby("dive_num").mean()
    )
    df_12nm = gp.read_file("/data/third_party/eez_12nm/eez_12nm_filled.geojson")
    # extend the Swedish territorial waters by a buffer lenght
    df_12nm_extend = df_12nm.copy()
    df_12nm_extend = df_12nm_extend.to_crs("epsg:3152")
    df_12nm_extend_in = df_12nm_extend.to_crs(epsg=4326)
    # Create minimal dataset and group it by dives
    df_glider = gp.GeoDataFrame(
        df_glider,
        geometry=gp.points_from_xy(df_glider.longitude, df_glider.latitude),
    )
    # Align crs and check which dives fall within Swedish 12 nm waters and helcom polygons
    df_glider = df_glider.set_crs(epsg=4326)
    df_12nm_extend = df_12nm_extend_in.to_crs(df_glider.crs)
    df_12nm_extend = gp.sjoin(df_12nm_extend, df_glider, predicate="contains")
    if df_12nm_extend.empty:
        return
    df_glider.index.rename("index", inplace=True)
    df_glider["dive_num"] = df_glider.index
    df_12nm_extend_id = df_12nm_extend[["index_right", "sovereign1"]]
    df_glider = pd.merge(
        df_glider,
        df_12nm_extend_id,
        left_on="dive_num",
        right_on="index_right",
        how="left",
    )
    df_glider.loc[df_glider.sovereign1 != "Sweden", "sovereign1"] = (
        "International waters"
    )
    if not (df_glider["sovereign1"].values == "International waters").all():
        mailer("cherddap", f"potential territorial waters data in {dataset_id}")


def datasets_to_emodnet(df_datasets):
    _log.info("Check datasets to emodnet")
    df_nrt = df_datasets[df_datasets.mission_type == "nrt"]
    df_delayed = df_datasets[df_datasets.mission_type == "delayed"]
    e_emodnet = ERDDAP(
        "https://ingestion-erddap.emodnet-physics.eu/erddap",
        protocol="tabledap",
    )
    # Fetch dataset list
    e_emodnet.response = "csv"
    e_emodnet.dataset_id = "allDatasets"
    try:
        df_datasets = e_emodnet.to_pandas(
            parse_dates=["minTime (UTC)", "maxTime (UTC)"],
        )
        # drop the allDatasets row and make the datasetID the index for easier reading
        df_datasets = df_datasets[df_datasets.datasetID.str.contains("VOTO")]
        df_datasets = df_datasets[df_datasets.datasetID.str.contains("SEA")]
        df_datasets["voto_datasetid"] = df_datasets.datasetID.str[5:]
        emodent_datasets = df_datasets.voto_datasetid.values
        check_names = list(df_nrt.index) + list(df_delayed.index)
        lost_datasets = set(check_names).difference(set(emodent_datasets))
        most_recent_emodnet = df_datasets["maxTime (UTC)"].max()
        most_recent_datasetid = df_datasets.iloc[df_datasets["maxTime (UTC)"].argmax()][
            "datasetID"
        ]
        most_recent_str = f"Most recent dataset on EMODnet is {most_recent_datasetid}, maxTime: {most_recent_emodnet}"
        if len(lost_datasets) > 0:
            names_text = "\n".join(lost_datasets)
            msg = f"Failed to find {len(lost_datasets)} datasets on  emodnet ERDDAP https://ingestion-erddap.emodnet-physics.eu/erddap.\n\n{most_recent_str}. \n\nMissing datasets \n {names_text}"
            mailer("cherddap", msg)
    except httpx.HTTPError:
        df_emodnet = pd.read_csv(
            e_emodnet.get_search_url(search_for="voto", response="csv"),
        )
        df_emodent_voto = df_emodnet[df_emodnet["Dataset ID"].str[:4] == "VOTO"]
        emodent_datasets = df_emodent_voto["Dataset ID"].str[5:].values
        check_names = list(df_nrt.index) + list(df_delayed.index)
        lost_datasets = list(set(check_names).difference(set(emodent_datasets)))
        if len(lost_datasets) > 0:
            names_text = "\n".join(lost_datasets)
            msg = f"failed to find {len(lost_datasets)} datasets on  emodnet ERDDAP: \n {names_text}"
            mailer("cherddap", msg)


def adcp_proc_check(e):
    adcp_datasets = pd.read_csv(
        e.get_search_url(search_for="delayed adcp", response="csv"),
    )["Dataset ID"]
    dataset_id = adcp_datasets[np.random.randint(0, len(adcp_datasets) - 1)]
    e.dataset_id = dataset_id
    e.variables = ["time", "adcp_time"]
    ds = e.to_xarray(requests_kwargs={"timeout": 300})
    ds = ds.drop_dims("timeseries")
    if len(np.unique(ds.adcp_time.values)) < 10000:
        msg = f"bad adcp times for {dataset_id}"
        mailer("cherddap", msg)


def good_times():
    _log.warning("TODO: check times of all datetime like columns are in expected range")
    _log.warning(
        "TODO: check that nrt and complete mission have similar time ranges. Check all or big mission as example",
    )


def manual_qc():
    _log.warning(
        "TODO: check that manually applied QC to vars like oxygen has been applied",
    )


def main():
    e = ERDDAP(
        server="https://erddap.observations.voiceoftheocean.org/erddap",
        protocol="tabledap",
    )
    # Fetch dataset list
    e.response = "csv"
    e.dataset_id = "allDatasets"
    df_datasets = e.to_pandas(parse_dates=["minTime (UTC)", "maxTime (UTC)"])
    # drop the allDatasets row and make the datasetID the index for easier reading
    df_datasets.set_index("datasetID", inplace=True)
    df_datasets.drop("allDatasets", inplace=True)
    df_datasets = df_datasets[df_datasets.index.str.contains("SEA")]
    adcp_dataset_check(df_datasets)
    enough_datasets(df_datasets)
    df_datasets = nrt_vs_complete(df_datasets)
    datasets_to_emodnet(df_datasets)
    bad_depths(df_datasets)
    bad_dataset_id(df_datasets)
    delayed = df_datasets.index[df_datasets.index.str[:3] == "del"]
    nrt = df_datasets.index[df_datasets.index.str[:3] == "nrt"]
    num_ds = len(delayed)
    num_nrt = len(nrt)
    unit_check(e, nrt[np.random.randint(0, num_nrt - 1)])
    profile_num_vs_dive_num(e, delayed[np.random.randint(0, num_ds - 1)])
    sensible_values(e, nrt[np.random.randint(0, num_ds - 1)])
    sensible_values(e, delayed[np.random.randint(0, num_ds - 1)])
    international_waters_check(e, "nrt_SEA067_M27")
    international_waters_check(e, nrt[np.random.randint(0, num_nrt - 1)])
    adcp_proc_check(e)
    good_times()
    manual_qc()


if __name__ == "__main__":
    logging.basicConfig(
        filename="/home/pipeline/log/cherrdap.log",
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _log.info("Start ERDDAP checks")
    main()
    _log.info("ERDDAP checks complete")
