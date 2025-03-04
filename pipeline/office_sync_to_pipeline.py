from votoutils.monitor.office_check_glider_files import (
    list_missions,
    skip_projects,
    erddap_download,
    explained_missions,
    good_mission,
)
from votoutils.upload.sync_functions import sync_script_dir
import logging

_log = logging.getLogger(__name__)

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
    mission_list = list_missions(to_skip=skip_projects)
    processed_missions = erddap_download()
    for mission in mission_list:
        good_mission(
            mission,
            processed_missions,
            explained=explained_missions,
            upload_script=sync_script_dir / "upload.sh",
        )
    _log.info("complete")
