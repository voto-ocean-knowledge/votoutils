from pathlib import Path
import pandas as pd
from itertools import chain
from votoutils.ad2cp.ad2cp_file_move import adcp_proc_check
import subprocess
from votoutils.utilities.utilities import mailer


explained_missions = (
    (67, 15),
    (61, 63),
    (56, 27),
    (66, 31),
    (45, 58),
    (61, 48),
    (45, 37),
    (45, 54),
    (44, 48),
    (55, 16),
    (63, 40),
    (66, 45),
    (45, 74),
    (66, 50),
    (55, 81),
    (44, 23),
    (56, 22),
    (44, 43)
)
skip_projects = [
    "1_Folder_Template",
    "00_Folder_Template",
    "2_Simulations",
    "3_SAT_Missions",
    "10_Oman_001",
    "8_KAMI-KZ_001",
    "11_Amundsen_Sea",
    "temprary_data_store"
]


def erddap_download():
    datasets_url = "https://erddap.observations.voiceoftheocean.org/erddap/tabledap/allDatasets.csv"
    df_erddap = pd.read_csv(datasets_url)
    df_erddap.drop(0, inplace=True)
    complete_glider_missions = df_erddap[
        df_erddap.datasetID.str[:7] == "delayed"
    ].datasetID
    erddap_missions = []
    for mission_str in complete_glider_missions:
        __, glider_str, mission_str = mission_str.split("_")
        glider = int(glider_str[3:])
        mission = int(mission_str[1:])
        erddap_missions.append((glider, mission))
    return erddap_missions


def good_mission(
    download_mission_path,
    processed_missions,
    explained=(),
    upload_script="upload.sh",
):
    if "XXX" in str(download_mission_path):
        return
    parts = list(download_mission_path.parts)
    parts[-3] = "3_Non_Processed"
    mission_path = Path(*parts)
    pretty_mission = str(mission_path)
    glidermission = mission_path.parts[-1]
    try:
        glider_str, mission_str = glidermission.split("_")
        glider = int(glider_str[3:])
        mission = int(mission_str[1:])
    except ValueError:
        print(f"Could not proc {pretty_mission}")
        return
    if (glider, mission) in explained:
        print(f"known bad mission {glider, mission}. Skipping")
        return
    if not mission_path.is_dir():
        msg = f"Downloaded but not processed {pretty_mission}"
        mailer("mission not processed", msg)
        return
    pld_path = mission_path / "PLD_raw"
    if not pld_path.is_dir():
        msg = f"no pld, {pretty_mission}"
        mailer("mission not processed", msg)
        return
    nav_path = mission_path / "NAV"
    if not nav_path.is_dir():
        msg = f"no nav, {pretty_mission}"
        mailer("mission not processed", msg)
        return
    pld_files = list(pld_path.glob(f"sea{str(glider).zfill(3)}.{mission}.pld1.raw*"))
    nav_files = list(nav_path.glob(f"sea{str(glider).zfill(3)}.{mission}.gli.sub*"))
    if len(pld_files) == 0 or len(nav_files) == 0:
        msg = f"No matching files {pretty_mission} "
        mailer("mission not processed", msg)
        return
    missmatch = abs(len(pld_files) - len(nav_files))
    if missmatch > 50:
        msg = f"Missmatch {len(nav_files)} nav files vs {len(pld_files)} pld files {pretty_mission}"
        mailer("mission not processed", msg)
        return
    if (glider, mission) not in processed_missions:
        msg = f"Not processed {pretty_mission}"
        mailer("mission not processed", msg)

        if pld_path.is_dir() and nav_path.is_dir():
            subprocess.check_call(
                [
                    "/usr/bin/bash",
                    upload_script,
                    str(glider),
                    str(mission),
                    mission_path,
                ],
            )
            msg = f"uploaded raw data for {pretty_mission}"
            mailer("new mission uploaded", msg)


def list_missions(to_skip=()):
    base = Path("/mnt/samba")
    projects = list(base.glob("*_*"))
    glider_dirs = []
    for proj in projects:
        good = True
        str_proj = str(proj)
        for skip in to_skip:
            if skip in str_proj:
                print(f"skipping {skip}")
                good = False
        if not good:
            continue
        non_proc = proj / "1_Downloaded"
        if non_proc.is_dir():
            proj_glider_dirs = non_proc.glob("SEA*")
            glider_dirs.append(list(proj_glider_dirs))
            continue
        sub_dirs = proj.glob("*")
        for sub_dir in sub_dirs:
            non_proc = sub_dir / "1_Downloaded"
            if non_proc.is_dir():
                for skip in to_skip:
                    if skip in str(non_proc):
                        print(f"skipping {skip}")
                        continue
                proj_glider_dirs = non_proc.glob("SEA*")
                glider_dirs.append(list(proj_glider_dirs))

    glider_dirs = list(chain(*glider_dirs))

    all_mission_paths = []
    for glider_dir in glider_dirs:
        mission_dirs = list(glider_dir.glob("SEA*"))
        all_mission_paths.append(mission_dirs)
    all_mission_paths = list(chain(*all_mission_paths))
    good_missions = []
    for mission_path in all_mission_paths:
        mission_name = mission_path.parts[-1]
        try:
            glider_str, mission_str = mission_name.split('_')
            glider_num = int(glider_str[3:])
            mission_num = int(mission_str[1:])
            good_missions.append(mission_path)
        except:
            print(f"{mission_path} is a bad one")

    return good_missions


if __name__ == "__main__":
    mission_paths = list_missions(to_skip=skip_projects)
    processed_missions = erddap_download()
    for mission_dir in mission_paths:
        good_mission(mission_dir, processed_missions, explained=explained_missions)
        adcp_proc_check(mission_dir)
