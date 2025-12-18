import os
import sys
from pathlib import Path
import logging
from votoutils.utilities.utilities import missions_no_proc
from votoutils.sailbuoy.sailbuoy_raw_to_nc import Sailbuoy

script_dir = Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)
logging.basicConfig(
    filename="/data/log/sailbuoy_all_complete.log",
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def proc_all_complete(reprocess=True):
    _log.info("Start complete reprocessing")
    yml_files = list(Path("/data/deployment_yaml/sailbuoy_yaml").glob("SB*.yml"))
    sailbuoy_missions = []
    for yml_path in yml_files:
        fn = yml_path.name.split(".")[0]
        sb_name, mission_name = fn.split("_")
        try:
            sailbuoy_missions.append((sb_name, int(mission_name[1:])))
        except ValueError:
            _log.warning(f"Could not process {fn}")

    for platform_serial, mission in sailbuoy_missions:
        if (platform_serial, int(mission)) in missions_no_proc:
            _log.info(f"skipping {platform_serial, mission}")
            continue
        input_dir = f"/data/data_raw/complete_mission/{platform_serial}/M{mission}/"
        if not Path(input_dir).exists():
            _log.warning(
                f"{input_dir} does not have sailbuoy files. skipping",
            )
            continue
        output_dir = f"/data/data_l0/complete_mission/{platform_serial}/M{mission}/"
        outfiles = list(Path(output_dir).glob(f"*{platform_serial}*.nc"))
        if outfiles and not reprocess:
            _log.info(f"Will not reprocess {platform_serial} M{mission}")
            continue
        _log.info(f"Reprocessing {platform_serial} M{mission}")
        data_in = f"/data/data_raw/complete_mission/{platform_serial}/M{mission}"
        data_out = f"/data/data_l0/complete_mission/{platform_serial}/M{mission}"
        if not Path(f"/data/data_l0/complete_mission/{platform_serial}").exists():
            Path(f"/data/data_l0/complete_mission/{platform_serial}").mkdir(parents=True)
        sb = Sailbuoy(
            data_in,
            data_out,
            f"/data/deployment-yaml/sailbuoy_yaml/{platform_serial}_M{mission}.yml"
        )
        sb.process()
    _log.info("Finished complete processing")


if __name__ == "__main__":
    proc_all_complete()
