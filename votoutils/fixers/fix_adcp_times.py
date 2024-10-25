import xarray as xr
from pathlib import Path
import shutil
from votoutils.utilities.utilities import encode_times
import logging
import pandas as pd

_log = logging.getLogger(__name__)


def fix_all_adcp_times():
    glider_paths = list(Path("/data/data_l0_pyglider/complete_mission").glob("SEA*"))
    for glider_path in glider_paths[1:]:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            df = pd.read_csv("/data/log/fixer.log", names=["data"])
            done = "".join(df.data)
            if str(mission_path) in done:
                _log.warning(f"already processed {str(mission_path)} skipping")
                continue
            glider = int(glider_path.parts[-1][3:])
            mission = int(mission_path.parts[-1][1:])
            adcp_time_fixer(glider, mission)


def adcp_time_fixer(glider, mission):
    input_nc = f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/timeseries/mission_timeseries.nc"
    ds = xr.open_dataset(input_nc)
    if "ad2cp_time" in list(ds):
        ds = encode_times(ds)
        tempfile = f"/data/tmp/SEA{glider}_M{mission}"
        ds.to_netcdf(tempfile)
        ds.close()
        shutil.move(tempfile, input_nc)
        _log.info(f"fix {input_nc}")


if __name__ == "__main__":
    logf = "/data/log/fixer.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fix_all_adcp_times()
    _log.info("done")
