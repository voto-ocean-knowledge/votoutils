import datetime
import pathlib
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from pyglider_single_mission import process
from votoutils.utilities.utilities import missions_no_proc

_log = logging.getLogger(__name__)


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
                "glider": ["SEA055"],
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
        mission_paths = path.glob("M*")

        glider_paths_good.append(mission_paths)
    glider_paths_good = [item for sublist in glider_paths_good for item in sublist]
    for mission_path in glider_paths_good:
        platform_serial = mission_path.parts[-2]
        mission = int(mission_path.parts[-1][1:])
        if [platform_serial, mission] in missions_no_proc:
            _log.debug(f"{platform_serial} M{mission} in mission_no_proc. Skipping")
            continue
        a = [
            np.logical_and(
                df_reprocess.glider == platform_serial,
                df_reprocess.mission == mission,
            ),
        ]
        if not sum(sum(a)):
            _log.warning(f"new mission {mission_path}")
            process(platform_serial, mission)
            nc_file = list(
                (
                    pathlib.Path(
                        f"/data/data_l0_pyglider/complete_mission/{platform_serial}/M{mission}/timeseries",
                    )
                ).glob("*.nc"),
            )[0]
            nc_time = nc_file.lstat().st_mtime
            nc_time = datetime.datetime.fromtimestamp(nc_time)
            new_row = pd.DataFrame(
                {
                    "glider": platform_serial,
                    "mission": mission,
                    "proc_time": nc_time,
                    "duration": datetime.timedelta(minutes=20),
                },
                index=[len(df_reprocess)],
            )
            df_reprocess = pd.concat((df_reprocess, new_row))
    _log.info(f"end length {len(df_reprocess)}")
    df_reprocess["gm"] = df_reprocess.glider + df_reprocess.mission.astype(str)
    df_reprocess = df_reprocess.groupby("gm").first()
    df_reprocess.sort_values("proc_time", inplace=True)


if __name__ == "__main__":
    main()
    _log.info("Complete")
