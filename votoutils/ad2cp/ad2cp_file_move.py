from pathlib import Path


def clean_adcp_fn(fn):
    fn = fn.lower()
    name, ext = fn.split(".")
    glider, mission = name.split("_")
    glider_fix = f"{glider[:3]}{str(int(glider[3:]))}"
    mission_fix = f"m{str(int(mission[1:]))}"
    return f"{glider_fix}_{mission_fix}.{ext}"


def adcp_proc_check(download_mission_path):
    parts = list(download_mission_path.parts)
    parts[-3] = "3_Non_Processed"
    mission_path = Path(*parts)
    pretty_mission = str(mission_path)
    adcp_dir = mission_path / "ADCP"
    if not adcp_dir.exists():
        return
    try:
        all_adcp_files = list(adcp_dir.glob("*.ad2cp"))
        if len(all_adcp_files) > 1:
            print(f"multiple .adcp files in {pretty_mission}")
            return
        adcp_file = all_adcp_files[0]
    except IndexError:
        print(f"no .ad2cp file found in {pretty_mission}")
        return
    adcp_parts = list(adcp_file.parts)
    adcp_parts[-5] = "4_Processed"
    ad2cp_path_clean = Path(*adcp_parts[:-1])
    pretty_mission_proc = str(ad2cp_path_clean)[11:]
    if len(list(ad2cp_path_clean.glob("*.nc"))) < 1:
        print(f"no nc found in {pretty_mission_proc}")
    return
