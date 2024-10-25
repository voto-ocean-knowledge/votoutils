import xarray as xr
from pathlib import Path
import subprocess
from votoutils.monitor.office_check_glider_files import list_missions, skip_projects
from votoutils.utilities.utilities import mailer
import logging

_log = logging.getLogger(__name__)
explained_issues = [(66, 45)]


def proc(mission_dir, reprocess=False, upload_script="upload_adcp.sh"):
    _log.info(f"clean ad2cp data for {mission_dir}")
    if "XXX" in str(mission_dir):
        return
    sub_directories = list(mission_dir.glob("*/")) + list(mission_dir.glob("*/*"))
    names = list(sub.name for sub in sub_directories)
    if "ADCP" not in names:
        # No raw ADCP data in 1_Downloaded
        return

    dir_parts = list(mission_dir.parts)
    dir_parts[4] = "4_Processed"
    adcp_dir = Path(*dir_parts) / "ADCP"
    pretty_mission = str(mission_dir)[85:]
    glider_str, mission_str = dir_parts[-1].split("_")
    glider = int(glider_str[3:])
    mission = int(mission_str[1:])
    if (glider, mission) in explained_issues:
        return
    if not adcp_dir.exists():
        # TODO check if this is a mission with ADCP data or not
        print(f"no adcp data for {mission_dir}")
        mailer("uploaded ADCP", f"no ADCP directory or files in {adcp_dir}")
        return
    files = list(adcp_dir.glob(f"*{glider}*{mission}*000*.nc"))
    if not files:
        mailer("uploaded ADCP", f"no ADCP files in {adcp_dir}")
        print(f"no files found in {adcp_dir}")
        return
    nc = files[0]
    fn = nc.name
    insize = nc.lstat().st_size
    fout = adcp_dir / f"sea{glider}_m{mission}_ad2cp.nc"
    if fout.exists():
        outsize = fout.lstat().st_size
        if outsize == 0:
            print(f"oh no {pretty_mission}")
            return
        if insize / outsize > 10:
            print(f"warning: resultant file more than 10x smaller {pretty_mission}")
        if not reprocess:
            return
    print(f"opening {pretty_mission} {fn}")
    config = xr.open_dataset(nc, group="Config").attrs
    data = xr.open_dataset(nc, group="Data/Average")
    # TODO decide on using bottom track data or not
    # data_btrack = xr.open_dataset(nc, group="Data/AverageBT")
    attrs = {}
    skip_attrs = [
        "fileName",
        "fileName_description",
        "File_file_directory",
        "File_file_directory_description",
    ]
    for i, (key, val) in enumerate(config.items()):
        if key in skip_attrs:
            continue
        # "rawConfiguration" causes problems. HD5 doesn't allow attributes over 64k, which is 4096 chars, so split long strings up
        if type(val) is str and len(val) > 4000:
            attrs[key] = val[:4000]
            attrs[f"{key}_continued"] = val[4000:]
            continue
        if key == "rawConfiguration":
            val = val[:4096]
            # continue
        attrs[key] = val
    data.attrs = attrs
    print(f"writing {fout}")
    data = data.drop_vars("MatlabTimeStamp")
    data.to_netcdf(fout)
    print("send to pipeline")
    subprocess.check_call(
        [
            "/usr/bin/bash",
            upload_script,
            str(glider),
            str(mission),
            str(fout),
        ],
    )
    msg = f"uploaded ADCP data for {pretty_mission} SEA{glider} M{mission}"
    mailer("uploaded ADCP", msg)
    print("finished")


def main():
    mission_list = list_missions(to_skip=skip_projects)
    for mission in mission_list:
        proc(mission, reprocess=False)


if __name__ == "__main__":
    main()
