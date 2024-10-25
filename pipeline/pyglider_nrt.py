import glob
import os
import sys
import pathlib
import logging
import numpy as np
import xarray as xr
import pandas as pd
from votoutils.glider.process_pyglider import proc_pyglider_l0
from votoutils.utilities.utilities import natural_sort
from votoutils.glider.metocc import create_csv

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)
logging.basicConfig(
    filename="/data/log/pyglider_nrt.log",
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
glider_no_proc = []


def proc_nrt():
    _log.info("Start nrt processing")
    all_glider_paths = pathlib.Path("/data/data_raw/nrt").glob("SEA*")
    for glider_path in all_glider_paths:
        glider = str(glider_path)[-3:].lstrip("0")
        if int(glider) in glider_no_proc:
            _log.info(f"SEA{glider} is not to be processed. Skipping")
            continue
        _log.info(f"Checking SEA{glider}")
        mission_paths = list(glider_path.glob("00*"))
        if not mission_paths:
            _log.warning(f"No missions found for SEA{glider}. Skipping")
            continue
        mission_paths.sort()
        mission = str(mission_paths[-1])[-3:].lstrip("0")
        _log.info(f"Checking SEA{glider} M{mission}")
        input_dir = f"/data/data_raw/nrt/SEA{glider.zfill(3)}/{mission.zfill(6)}/C-Csv/"
        output_dir = f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/"
        gridfiles_dir = f"{output_dir}gridfiles/"
        ts_dir = f"{output_dir}/timeseries/"
        try:
            nc_file = list(pathlib.Path(ts_dir).glob("*.nc"))[0]
            ds = xr.open_dataset(nc_file)
            max_time = ds.time.values.max()
            ds.close()
        except IndexError:
            _log.info(f"no nc file found int {gridfiles_dir}. Reprocessing all data")
            max_time = np.datetime64("1970-01-01")
        in_files = natural_sort(glob.glob(f"{input_dir}*pld1*"))
        max_dive_file = in_files[-1]
        df = pd.read_csv(
            max_dive_file,
            sep=";",
            parse_dates=True,
            index_col=0,
            dayfirst=True,
            nrows=10,
        )
        file_time = df.index.max()
        if max_time + np.timedelta64(10, "m") > file_time:
            _log.info(f"No new SEA{glider} M{mission} input files")
            continue
        if not pathlib.Path(
            f"/data/deployment_yaml/mission_yaml/SEA{glider}_M{mission}.yml",
        ).exists():
            _log.warning(f"yml file for SEA{glider} M{mission} not found.")
            continue
        _log.info(f"Processing SEA{glider} M{mission}")
        proc_pyglider_l0(glider, mission, "sub", input_dir, output_dir)
        _log.info("creating metocc csv")
        timeseries_dir = pathlib.Path(output_dir) / "timeseries"
        timeseries_nc = list(timeseries_dir.glob("*.nc"))[0]
        metocc_base = create_csv(timeseries_nc)
        _log.info(f"created metocc files with base {metocc_base}")
    _log.info("Finished nrt processing")


if __name__ == "__main__":
    proc_nrt()
