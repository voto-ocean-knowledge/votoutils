import datetime
import pathlib
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from pyglider_single_mission import process
from votoutils.utilities.utilities import mailer

_log = logging.getLogger(__name__)

expected_adcp_fails = [(66, 16)]


def adcp_status(glider, mission):
    pld_files = list(
        Path(f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/").glob(
            "*pld1.raw.*gz",
        ),
    )
    if not pld_files:
        return False
    pld_file = pld_files[0]

    df = pd.read_csv(pld_file, sep=";")
    if (glider, mission) in expected_adcp_fails:
        _log.info("no adcp data expected, none recovered during mission")
        return True

    if "AD2CP_PRESSURE" not in list(df):
        _log.info("no adcp data expected")
        return True
    if Path(
        f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/ADCP/sea{glider}_m{mission}_ad2cp.nc",
    ).exists():
        _log.info("adcp data file found")
        return True
    else:
        mailer("no-adcp", f"did not find expected ADCP file for SEA{glider} M{mission}")
        return False


def main():
    logf = "/data/log/new_complete_mission.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _log.info("Check for new missions")
    if Path("/home/pipeline/reprocess.csv").exists():
        df_reprocess = pd.read_csv(
            "/home/pipeline/reprocess.csv",
            parse_dates=["proc_time"],
        )
        df_reprocess.sort_values("proc_time", inplace=True)
    else:
        df_reprocess = pd.DataFrame(
            {
                "glider": [55],
                "mission": [16],
                "proc_time": [datetime.datetime(1970, 1, 1)],
                "duration": [datetime.timedelta(minutes=10)],
            },
        )
        df_reprocess.to_csv("/home/pipeline/reprocess.csv", index=False)
    _log.info(f"start length {len(df_reprocess)}")
    glider_paths = list(pathlib.Path("/data/data_raw/complete_mission").glob("SEA*"))
    glider_paths_good = []
    for path in glider_paths:
        if "SEA57" in str(path):
            continue
        mission_paths = path.glob("M*")

        glider_paths_good.append(mission_paths)
    glider_paths_good = [item for sublist in glider_paths_good for item in sublist]
    if len(glider_paths_good) == len(df_reprocess):
        _log.info("No new missions to process")
        return
    for mission_path in glider_paths_good:
        glider = int(mission_path.parts[-2][3:])
        mission = int(mission_path.parts[-1][1:])
        a = [
            np.logical_and(
                df_reprocess.glider == glider,
                df_reprocess.mission == mission,
            ),
        ]
        if not sum(sum(a)):
            _log.warning(f"new mission {mission_path}")
            if not adcp_status(glider, mission):
                continue
            process(glider, mission)
            nc_file = list(
                (
                    pathlib.Path(
                        f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/timeseries",
                    )
                ).glob("*.nc"),
            )[0]
            nc_time = nc_file.lstat().st_mtime
            nc_time = datetime.datetime.fromtimestamp(nc_time)
            new_row = pd.DataFrame(
                {
                    "glider": glider,
                    "mission": mission,
                    "proc_time": nc_time,
                    "duration": datetime.timedelta(minutes=20),
                },
                index=[len(df_reprocess)],
            )
            df_reprocess = pd.concat((df_reprocess, new_row))
    _log.info(f"end length {len(df_reprocess)}")
    df_reprocess["gm"] = df_reprocess.glider * 10000 + df_reprocess.mission
    df_reprocess = df_reprocess.groupby("gm").first()
    df_reprocess.sort_values("proc_time", inplace=True)


if __name__ == "__main__":
    main()
    _log.info("Complete")
