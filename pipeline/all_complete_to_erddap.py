import pathlib
import pandas as pd
import logging
import subprocess
from votoutils.upload.sync_functions import sync_script_dir

_log = logging.getLogger(__name__)


if __name__ == "__main__":
    logf = "/data/log/complete_mission_to_erddap.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _log.info("Start send to erddap")
    df_reprocess = pd.read_csv(
        "/home/pipeline/reprocess.csv",
        parse_dates=["proc_time"],
    )
    df_reprocess.sort_values("proc_time", inplace=True)
    df_reprocess.reset_index(inplace=True)
    total = len(df_reprocess)
    _log.info(f"will send {total} files to erddap")
    for i, row in df_reprocess.iterrows():
        glider, mission, proc = row.glider, row.mission, row.proc_time
        print(f"Will send file {i}/{total}: SEA{glider} M{mission}")
        _log.info(f"Send file {i}/{total}: SEA{glider} M{mission}")
        subprocess.check_call(
            [
                "/usr/bin/bash",
                sync_script_dir / "send_to_erddap.sh",
                str(glider),
                str(mission),
            ],
        )
        if pathlib.Path(
            f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/ADCP/adcp.nc",
        ).exists():
            _log.info(f"Send adcp file {i}/{total}: SEA{glider} M{mission}")
            subprocess.check_call(
                [
                    "/usr/bin/bash",
                    sync_script_dir / "send_to_erddap_adcp.sh",
                    str(glider),
                    str(mission),
                ],
            )

    _log.info("Complete send to erddap")
