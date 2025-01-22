from votoutils.monitor.office_check_glider_files import list_missions, skip_projects
import shutil
import subprocess
from pathlib import Path
import logging
import tempfile
import json

_log = logging.getLogger(__name__)
script_dir = Path(__file__).parent.absolute()
secrets_dir = Path(__file__).parent.parent.parent.absolute()

with open(secrets_dir / "email_secrets.json") as json_file:
    secrets = json.load(json_file)
nortek_jar_path = secrets["nortek_jar_path"]

def convert_from_ad2cp(dir_in, outfile, reprocess=False):
    _log.debug(f"looking for ad2cp files in {dir_in}")
    ad2cp_files = list(Path(dir_in).glob("*.ad2cp"))
    if len(ad2cp_files) == 0:
        _log.error(f"no input ad2cp files in {dir_in}")
        return
    elif len(ad2cp_files) > 1:
        _log.error(f"multiple input ad2cp files in {dir_in}")
        return
    infile = ad2cp_files[0]
    fn = infile.name
    if outfile.exists() and not reprocess:
        _log.info(f"outfile {outfile} already exists. Not reprocessing")
        return
    _log.debug(f"Converting {infile}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        _log.debug(f'created temporary directory {tmpdirname}')
        tmp_ad2cp = f"{tmpdirname}/{fn}"
        tmp_nc = f"{tmpdirname}/{outfile.name}"
        shutil.copy(infile, tmp_ad2cp)

        subprocess.check_call(
            [
                "/usr/bin/bash",
                str(script_dir / "convert_from_nortek.sh"),
                str(nortek_jar_path),
                str(tmpdirname),
                str(fn)
            ],
        )
        out_clean = str(outfile).replace(".022.25.AD2CPRaw.00000", "")
        shutil.copy(tmp_nc, out_clean)
    _log.info(f"Converted {outfile}")


def convert_ad2cp_to_nc(mission_dir):
    _log.debug(f"copy ad2cp data for {mission_dir}")
    if "XXX" in str(mission_dir):
        return
    sub_directories = list(mission_dir.glob("*/")) + list(mission_dir.glob("*/*"))
    names = list(sub.name for sub in sub_directories)
    if "ADCP" not in names:
        _log.debug(f"No raw ADCP in 1_Downloaded {mission_dir}")
        return

    dir_parts = list(mission_dir.parts)
    dir_parts[4] = "3_Non_Processed"
    source_dir = Path(*dir_parts) / "ADCP"
    dir_parts[4] = "4_Processed"
    destination_dir = Path(*dir_parts) / "ADCP"
    glider_str, mission_str = dir_parts[-1].split("_")
    glider = int(glider_str[3:])
    mission = int(mission_str[1:])
    destination_file = destination_dir / f"SEA{str(glider).zfill(3)}_M{mission}.ad2cp"
    nc_out_fn =  f"SEA{str(glider).zfill(3)}_M{mission}.ad2cp.022.25.AD2CPRaw.00000.nc"
    nc_out_file = destination_dir / nc_out_fn
    if nc_out_file.exists():
        _log.debug(f"destination file {nc_out_file} already exists")
        return
    if not source_dir.exists():
        _log.error(f"No ADCP dir found {source_dir}")
        return
    source_files = list(source_dir.glob(f"*{glider}*{mission}*ad2cp"))
    if len(source_files) == 0:
        _log.error(f"no input ad2cp files in {source_dir}")
        return
    elif len(source_files) > 1:
        _log.error(f"multiple input ad2cp files in {source_dir}")
        return
    source_file = source_files[0]
    if not destination_dir.exists():
        destination_dir.mkdir(parents=True)
    if not destination_file.exists():
        shutil.copy(source_file, destination_file)
    convert_from_ad2cp(destination_dir, nc_out_file)


def convert_all_ad2cp():
    mission_list = list_missions(to_skip=skip_projects)
    for mission_dir in mission_list[:2]:
        convert_ad2cp_to_nc(mission_dir)

if __name__ == '__main__':
    logf = "/data/log/ad2cp_to_nc.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    convert_all_ad2cp()