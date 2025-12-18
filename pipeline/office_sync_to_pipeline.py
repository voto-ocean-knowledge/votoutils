from votoutils.monitor.office_check_glider_files import (
    list_missions,
    skip_projects,
    erddap_download,
    explained_missions,
    good_mission,
)
from votoutils.upload.sync_functions import sync_script_dir
import subprocess
import glob
import logging

_log = logging.getLogger(__name__)


def sync_sailbuoy():
    upload_script = sync_script_dir / "upload_sailbuoy.sh"
    sailbuoy_missions = glob.glob("/mnt/samba/*/*/3_Non_Processed/*SB*/SB*M*") + glob.glob("/mnt/samba/*/*/3_Non_Processed/*SB*/SB*M*")
    for input_dir in sailbuoy_missions:
        mission_str = input_dir.split('/')[-1]
        _log.info(f"upload sailbuoy mission {mission_str}")
        platform_serial, mission = mission_str.split('_M')
        subprocess.check_call(
            [
                "/usr/bin/bash",
                upload_script,
                str(platform_serial),
                str(mission),
                str(input_dir),
            ]
        )


def sync_glider():
    mission_list = list_missions(to_skip=skip_projects)
    processed_missions = erddap_download()
    for mission in mission_list:
        good_mission(
            mission,
            processed_missions,
            explained=explained_missions,
            upload_script=sync_script_dir / "upload.sh",
        )
if __name__ == "__main__":
    logf = "/data/log/office_sync_to_pipeline.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _log.info("start check of office fileserver for new complete missions")
    sync_glider()
    sync_sailbuoy()
    _log.info("complete")
