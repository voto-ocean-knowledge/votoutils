import datetime
import pathlib
import pandas as pd
import numpy as np
import logging
import types
from pathlib import Path
from pyglider_single_mission import process
_log = logging.getLogger(__name__)


def adcp_status(glider, mission):
    pld_file = list(Path(f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/").glob("*pld1.raw.*gz"))[0]
    df = pd.read_csv(pld_file, sep=";")
    if "AD2CP_PRESSURE" not in list(df):
        _log.info("no adcp data expected")
        return True
    if Path(f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/ADCP/sea{glider}_m{mission}_ad2cp.nc").exists():
        _log.info("adcp data file found")
        return True
    else:
        _log.warning("did not find expected ADCP file")
        return False


def main():
    logf = f'/data/log/new_complete_mission.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.info("Check for new missions")
    df_reprocess = pd.read_csv('/home/pipeline/reprocess.csv', parse_dates=["proc_time"])
    df_reprocess.sort_values("proc_time", inplace=True)
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
        a = [np.logical_and(df_reprocess.glider == glider, df_reprocess.mission == mission)]
        if not sum(sum(a)):
            _log.warning(f"new mission {mission_path}")
            if not adcp_status(glider, mission):
                continue
            args = types.SimpleNamespace()
            args.glider = glider
            args.mission = mission
            args.kind = "raw"
            process(args)
            nc_file = list((pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/timeseries")).glob('*.nc'))[0]
            nc_time = nc_file.lstat().st_mtime
            nc_time = datetime.datetime.fromtimestamp(nc_time)
            new_row = pd.DataFrame({"glider": glider, "mission": mission,
                                    "proc_time": nc_time, "duration": datetime.timedelta(minutes=20)},
                                   index=[len(df_reprocess)])
            df_reprocess = pd.concat((df_reprocess, new_row))
    _log.info(f"end length {len(df_reprocess)}")
    df_reprocess["gm"] = df_reprocess.glider * 10000 + df_reprocess.mission
    df_reprocess = df_reprocess.groupby("gm").first()
    df_reprocess.sort_values("proc_time", inplace=True)


if __name__ == '__main__':
    main()
    _log.info("Complete")
