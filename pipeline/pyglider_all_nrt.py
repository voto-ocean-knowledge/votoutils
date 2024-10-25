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


def proc_all_nrt():
    _log.info("Start nrt reprocessing")
    yml_files = list(pathlib.Path("/data/deployment_yaml/mission_yaml").glob("*.yml"))
    glidermissions = []
    for yml_path in yml_files:
        fn = yml_path.name.split(".")[0]
        glider_name, mission_name = fn.split("_")
        try:
            glidermissions.append((int(glider_name[3:]), int(mission_name[1:])))
        except ValueError:
            _log.warning(f"Could not process {fn}")

    for glider, mission in glidermissions:
        input_dir = f"/data/data_raw/nrt/SEA{str(glider).zfill(3)}/{str(mission).zfill(6)}/C-Csv/"
        if not pathlib.Path(input_dir).exists():
            _log.info(
                f"SEA{glider} M{mission} does not have nrt alseamar raw files. skipping",
            )
            continue
        output_dir = f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/"
        _log.info(f"Reprocessing SEA{glider} M{mission}")
        proc_pyglider_l0(glider, mission, "sub", input_dir, output_dir)
    _log.info("Finished nrt processing")


if __name__ == "__main__":
    proc_all_nrt()
