import os
import sys
import pathlib
import logging
from votoutils.utilities.utilities import missions_no_proc
from pyglider_single_mission import process

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)
logging.basicConfig(
    filename="/data/log/pyglider_all_complete.log",
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def proc_all_complete(reprocess = True):
    _log.info("Start complete reprocessing")
    yml_files = list(pathlib.Path("/data/deployment_yaml/mission_yaml").glob("*.yml"))
    yml_files.sort()
    glidermissions = []
    for yml_path in yml_files:
        fn = yml_path.name.split(".")[0]
        glider_name, mission_name = fn.split("_")
        try:
            glidermissions.append((glider_name, int(mission_name[1:])))
        except ValueError:
            _log.warning(f"Could not process {fn}")

    for platform_serial, mission in glidermissions:
        if (platform_serial, int(mission)) in missions_no_proc:
            _log.info(f"skipping {platform_serial, mission}")
            continue
        input_dir = f"/data/data_raw/complete_mission/{platform_serial}/M{mission}/"
        if not pathlib.Path(input_dir).exists():
            _log.info(
                f"{platform_serial} M{mission} does not have raw alseamar raw files. skipping",
            )
            continue
        output_dir = f"/data/data_l0_pyglider/complete_mission/{platform_serial}/M{mission}/"
        if pathlib.Path(output_dir).exists() and not reprocess:
            _log.info(f"Will not reprocess {platform_serial} M{mission}")
            continue
        _log.info(f"Reprocessing {platform_serial} M{mission}")
        process(platform_serial, mission)
    _log.info("Finished complete processing")


if __name__ == "__main__":
    proc_all_complete()
