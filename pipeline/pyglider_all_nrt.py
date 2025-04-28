import os
import sys
import pathlib
import logging
from votoutils.glider.process_pyglider import proc_pyglider_l0

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)
logging.basicConfig(
    filename="/data/log/pyglider_all_nrt.log",
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def proc_all_nrt(reprocess = False):
    _log.info("Start nrt reprocessing")
    yml_files = list(pathlib.Path("/data/deployment_yaml/mission_yaml").glob("*.yml"))
    glidermissions = []
    for yml_path in yml_files:
        fn = yml_path.name.split(".")[0]
        glider_name, mission_name = fn.split("_")
        try:
            glidermissions.append((glider_name, int(mission_name[1:])))
        except ValueError:
            _log.warning(f"Could not process {fn}")

    for platform_serial, mission in glidermissions:
        input_dir = f"/data/data_raw/nrt/{platform_serial}/{str(mission).zfill(6)}/C-Csv/"
        if not pathlib.Path(input_dir).exists():
            _log.info(
                f"{platform_serial} M{mission} does not have nrt alseamar raw files. skipping",
            )
            continue
        output_dir = f"/data/data_l0_pyglider/nrt/{platform_serial}/M{mission}/"
        if pathlib.Path(output_dir).exists() and not reprocess:
            _log.info(f"Will not reprocess {platform_serial} M{mission}")
            continue
        _log.info(f"Reprocessing {platform_serial} M{mission}")
        proc_pyglider_l0(platform_serial, mission, "sub", input_dir, output_dir)
    _log.info("Finished nrt processing")


if __name__ == "__main__":
    proc_all_nrt()
