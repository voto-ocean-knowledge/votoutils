import os
from pathlib import Path
import logging
import datetime
import polars as pl
import shutil
import gzip
from votoutils.utilities.utilities import missions_no_proc

_log = logging.getLogger(__name__)
logging.basicConfig(
    filename="/data/log/synthetic_nrt_from_complete.log",
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def match_input_files(gli_infiles, pld_infiles):
    gli_nums = []
    for fname in gli_infiles:
        parts = fname.name.split(".")
        try:
            gli_nums.append(int(parts[-1]))
        except ValueError:
            try:
                gli_nums.append(int(parts[-2]))
            except ValueError:
                raise ValueError(
                    "Unexpected gli file filename found in input. Aborting",
                )
    pld_nums = []
    for fname in pld_infiles:
        parts = fname.name.split(".")
        try:
            pld_nums.append(int(parts[-1]))
        except ValueError:
            try:
                pld_nums.append(int(parts[-2]))
            except ValueError:
                raise ValueError(
                    "Unexpected pld file filename found in input. Aborting",
                )
    good_cycles = set(pld_nums) & set(gli_nums)
    good_gli_files = []
    good_pld_files = []
    for i, num in enumerate(gli_nums):
        if num in good_cycles:
            good_gli_files.append(gli_infiles[i])
    for i, num in enumerate(pld_nums):
        if num in good_cycles:
            good_pld_files.append(pld_infiles[i])
    return good_gli_files, good_pld_files


def fix_seconds(first_timestamp):
    seconds = int(str(first_timestamp)[-2:])
    if seconds in [1, 31]:
        return 0
    if seconds < 31:
        return 31 - seconds
    return 61 - seconds


def set_first_timestamp(df):
    first_time = df["time"][0]
    add_seconds = fix_seconds(first_time)
    if add_seconds == 0:
        return df
    return df.filter(
        pl.col("time") >= first_time + datetime.timedelta(seconds=add_seconds),
    )


def subsample_pld_file(pldfile):
    _log.debug(f"proc {pldfile}")
    fn = pldfile.name
    platform_serial, mission, _ = fn.split(".", maxsplit=2)
    df = pl.read_csv(pldfile, separator=";", truncate_ragged_lines=True)
    df = df.with_columns(
        (pl.col("PLD_REALTIMECLOCK").str.slice(0, 18) + "1").alias("time_seconds"),
    )
    df = df.with_columns(
        pl.col("time_seconds")
        .str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S")
        .alias("time"),
    )
    df = df.unique(subset="time_seconds", keep="first")
    df = df.sort("time")
    if df.is_empty():
        return
    df = set_first_timestamp(df)
    if df.is_empty():
        return
    df_out = df.upsample(time_column="time", every="30s").fill_null(strategy="forward")
    if df_out.is_empty():
        return
    df_out = (
        df_out.unique(subset="time_seconds", keep="first")
        .sort("time")
        .drop(["time", "time_seconds"])
    )
    fn_out = fn.replace("raw", "sub").replace(".gz", "")
    outfile = (
        Path(
            f"/data/data_raw/nrt/{platform_serial.upper()}/{str(mission).zfill(6)}/C-Csv/",
        )
        / fn_out
    )
    df_out.write_csv(outfile, separator=";")
    with open(outfile, "rb") as f_in, gzip.open(str(outfile) + ".gz", "wb") as f_out:
        f_out.writelines(f_in)
    os.unlink(outfile)


def synthetic_nrt_files_from_complete(platform_serial, mission, dive_stride=5):
    nrt_path = Path(
        f"/data/data_raw/nrt/{platform_serial}/{str(mission).zfill(6)}/C-Csv/",
    )
    delayed_path = Path(f"/data/data_raw/complete_mission/{platform_serial}/M{mission}")
    if not nrt_path.exists():
        nrt_path.mkdir(parents=True)
    in_files_gli = sorted(
        delayed_path.glob("*gli.sub*.gz"),
        key=lambda path: int(path.stem.split(".")[4]),
    )
    in_files_pld = sorted(
        delayed_path.glob("*pld1.raw*.gz"),
        key=lambda path: int(path.stem.split(".")[4]),
    )
    in_files_gli, in_files_pld = match_input_files(in_files_gli, in_files_pld)

    with open(nrt_path / "synthetic_nrt_data.txt", "w") as f_out:
        f_out.write("synthetic")

    for glifile in in_files_gli[::dive_stride]:
        outfile = str(nrt_path / glifile.name)
        shutil.copy(glifile, outfile)

    for pldfile in in_files_pld[::dive_stride]:
        subsample_pld_file(pldfile)


def all_nrt_from_complete(reprocess=True):
    _log.info("Start nrt from complete")
    glider_paths = list(Path("/data/data_l0_pyglider/complete_mission").glob("S*"))
    glidermissions = []
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            platform_serial = glider_path.parts[-1]
            mission = int(mission_path.parts[-1][1:])
            if (platform_serial, mission) in missions_no_proc:
                _log.info(f"mission {platform_serial} M{mission} is in missions no proc. skipping")
            nrt_path = Path(
                f"/data/data_raw/nrt/{platform_serial}/{str(mission).zfill(6)}/C-Csv/",
            )
            syn_marker_path = nrt_path / "synthetic_nrt_data.txt"
            if nrt_path.exists() and not syn_marker_path.exists():
                _log.debug(f"nrt path {nrt_path} is non-synthetic. skipping")
                continue
            try:
                glidermissions.append(
                    (glider_path.parts[-1], int(mission_path.parts[-1][1:])),
                )
            except ValueError:
                _log.warning(f"Could not process {mission_path}")

    _log.info(f"will process {len(glidermissions)} missions")
    for i, (platform_serial, mission) in enumerate(glidermissions):
        nrt_path = Path(
            f"/data/data_raw/nrt/{platform_serial}/{str(mission).zfill(6)}/C-Csv/",
        )
        if nrt_path.exists() and not reprocess:
            _log.info(f"{platform_serial} M{mission} already exists. Skipping")
            continue
        synthetic_nrt_files_from_complete(platform_serial, mission)
        _log.info(f"{i + 1}/{len(glidermissions)} Processed {platform_serial} M{mission}")

    _log.info("Complete synthetic nrt from complete")


if __name__ == "__main__":
    # synthetic_nrt_files_from_complete(44, 85)
    all_nrt_from_complete()
