import os
import sys
import pathlib
import argparse
import numpy as np
import logging
import glob
import pandas as pd
import datetime
import subprocess
import shutil
from votoutils.utilities.utilities import natural_sort, match_input_files
from votoutils.glider.process_pyglider import proc_pyglider_l0
from votoutils.upload.sync_functions import sync_script_dir

script_dir = pathlib.Path(__file__).parent.parent.absolute()
parent_dir = script_dir.parents[0]
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)


def remove_proc_files(glider, mission):
    rawnc_dir = pathlib.Path(
        f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/rawnc",
    )
    if rawnc_dir.exists():
        shutil.rmtree(rawnc_dir)
    return


def update_processing_time(glider, mission, start):
    df_reprocess = pd.read_csv(
        "/home/pipeline/reprocess.csv",
        parse_dates=["proc_time"],
    )
    a = [np.logical_and(df_reprocess.glider == glider, df_reprocess.mission == mission)]
    if df_reprocess.index[tuple(a)].any():
        ind = df_reprocess.index[tuple(a)].values[0]
        df_reprocess.at[ind, "proc_time"] = datetime.datetime.now()
        df_reprocess.at[ind, "duration"] = datetime.datetime.now() - start
    else:
        new_row = pd.DataFrame(
            {
                "glider": glider,
                "mission": mission,
                "proc_time": datetime.datetime.now(),
                "duration": datetime.datetime.now() - start,
            },
            index=[len(df_reprocess)],
        )
        df_reprocess = pd.concat((df_reprocess, new_row))
    df_reprocess.sort_values("proc_time", inplace=True)
    _log.info(f"updated processing time to {datetime.datetime.now()}")
    df_reprocess.to_csv("/home/pipeline/reprocess.csv", index=False)


def process(glider, mission):
    logf = f"/data/log/complete_mission/SEA{str(glider)}_M{str(mission)}.log"
    logging.basicConfig(
        filename=logf,
        filemode="w",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    start = datetime.datetime.now()
    input_dir = f"/data/data_raw/complete_mission/SEA{glider}/M{mission}/"
    if not input_dir:
        raise ValueError(f"Input dir {input_dir} not found")
    output_dir = f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/"

    in_files_gli = natural_sort(glob.glob(f"{input_dir}*gli*.gz"))
    in_files_pld = natural_sort(glob.glob(f"{input_dir}*pld1.raw*.gz"))
    in_files_gli, in_files_pld = match_input_files(in_files_gli, in_files_pld)

    if len(in_files_gli) == 0 or len(in_files_pld) == 0:
        raise ValueError(f"input dir {input_dir} does not contain gli and/or pld files")
    _log.info(f"Processing glider {glider} mission {mission}")
    proc_pyglider_l0(glider, mission, "raw", input_dir, output_dir)
    _log.info(f"Finished processing glider {glider} mission {mission}")

    sys.path.append(str(parent_dir / "voto-web/voto/bin"))
    # noinspection PyUnresolvedReferences
    from add_profiles import init_db, add_complete_profiles

    init_db()
    add_complete_profiles(
        pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}"),
    )
    _log.info("Finished add to database")

    from votoutils.ad2cp.ad2cp_proc import adcp_data_present

    if adcp_data_present(glider, mission):
        _log.info("TODO process adcp data")
        # proc_gliderad2cp(glider, mission)
    update_processing_time(glider, mission, start)

    sys.path.append(str(parent_dir / "quick-plots"))
    # noinspection PyUnresolvedReferences
    from complete_mission_plots import complete_plots

    complete_plots(glider, mission)
    _log.info("Finished plot creation")

    subprocess.check_call(
        [
            "/usr/bin/bash",
            sync_script_dir / "send_to_erddap.sh",
            str(glider),
            str(mission),
        ],
    )
    _log.info("Sent file to erddap")

    remove_proc_files(glider, mission)
    _log.info("Finished processing")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="process SX files with pyglider")
    parser.add_argument("glider", type=int, help="glider number, e.g. 70")
    parser.add_argument("mission", type=int, help="Mission number, e.g. 23")
    args = parser.parse_args()
    process(args.glider, args.mission)
