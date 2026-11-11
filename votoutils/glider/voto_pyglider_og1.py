import datetime
import numpy as np
import polars as pl
import xarray as xr
import logging
from pathlib import Path
import subprocess
import glob
import pandas as pd
import argparse
from votoutils.upload.sync_functions import sync_script_dir
from votoutils.glider.process_pyglider_og1 import proc_pyglider_og1
from votoutils.glider.post_process_dataset import post_process, remove_gps_jammed_cycles
from votoutils.utilities.geocode import get_seas_merged_nav_nc
from votoutils.utilities.utilities import encode_times
from votoutils.utilities import database
from votoutils.utilities.utilities import natural_sort, platforms_no_proc, missions_no_proc
_log = logging.getLogger(__name__)


def set_profile_numbers(ds):
    ds["DIVE_NUMBER"] = np.around(ds["DIVE_NUMBER"]).astype(int)
    df = ds.to_pandas()
    df["profile_index"] = 1
    deepest_points = []
    dive_nums = np.unique(df.DIVE_NUMBER)
    for num in dive_nums:
        df_dive = df[df.DIVE_NUMBER == num]
        if np.isnan(df_dive.PRES).all():
            deep_inflect = df_dive.index[int(len(df_dive) / 2)]
        else:
            deep_inflect = df_dive[
                df_dive.PRES== df_dive.PRES.max()
            ].index.values[0]
        deepest_points.append(deep_inflect)

    previous_deep_inflect = deepest_points[0]
    df.loc[df.index[0] : previous_deep_inflect, "profile_index"] = 1
    num = 0
    for i, deep_inflect in enumerate(deepest_points[1:]):
        num = i + 1
        df_deep_to_deep = df.loc[previous_deep_inflect:deep_inflect]
        if np.isnan(df_deep_to_deep.PRES).all():
            shallow_inflect = df_deep_to_deep.index[int(len(df_deep_to_deep) / 2)]
        else:
            shallow_inflect = df_deep_to_deep[
                df_deep_to_deep.PRES== df_deep_to_deep.PRES.min()
            ].index.values[0]
        df.loc[previous_deep_inflect:shallow_inflect, "profile_index"] = num * 2
        df.loc[shallow_inflect:deep_inflect, "profile_index"] = num * 2 + 1
        previous_deep_inflect = deep_inflect
    df.loc[previous_deep_inflect : df.index[-1], "profile_index"] = num * 2 + 2

    df["profile_direction"] = 1
    df.loc[df.profile_index % 2 == 0, "profile_direction"] = -1
    ds["PROFILE_NUMBER"] = df.DIVE_NUMBER.copy()
    ds["PROFILE_DIRECTION"] = df.DIVE_NUMBER.copy()
    ds["PROFILE_NUMBER"].values = df.profile_index
    ds["PROFILE_DIRECTION"].values = df.profile_direction
    ds["PROFILE_NUMBER"].attrs = dict(long_name="profile number", units="1", sources="PRES, TIME, DIVE_NUMBER")
    ds["PROFILE_DIRECTION"].attrs = {"long_name": "profile direction", "units": "1",
                                     "sources": "PRES, TIME, DIVE_NUMBER", "comment": "-1 = ascending, 1 = descending"}
    return ds


def add_voto_stuff(outname):
    out_path = Path(outname)
    coder = xr.coders.CFDatetimeCoder(time_unit="s")
    ds = xr.open_dataset(outname, decode_times=coder)
    ds = remove_gps_jammed_cycles(ds)
    attrs = ds.attrs
    # OG1 VOTO specific
    timeseries_dir = out_path.parent
    rawncdir = timeseries_dir.parent / 'rawnc'
    nav_nc = list(rawncdir.glob("*rawgli.parquet"))[0]
    ds = set_profile_numbers(ds)
    basin = get_seas_merged_nav_nc(nav_nc)
    attrs["basin"] = basin
    # More custom metadata
    pld_nc = list(rawncdir.glob("*pld.parquet"))[0]
    df = pl.read_parquet(pld_nc)
    total_dives = df.select("fnum").unique().shape[0]
    attrs["total_dives"] = total_dives
    dataset_type = "nrt" if 'nrt' in str(outname) else "delayed"
    glider_serial = ds.attrs['platform_serial_number']
    deployment_id = ds.attrs['deployment_id']
    ds_id = ds.attrs['id']
    voto_dataset_id = (
        f"{dataset_type}_{glider_serial}_M{deployment_id}"
    )
    attrs["dataset_id"] = voto_dataset_id
    attrs["data_url"] = f"https://erddap.observations.voiceoftheocean.org/erddap/tabledap/{attrs['dataset_id']}"
    attrs["variables"] = list(ds.variables)
    attrs["glider_serial"] = glider_serial
    attrs['title'] = f'{ds_id} M{deployment_id} {attrs["glider_name"]}'
    attrs['institution'] = 'VOTO'
    # TODO the following properly from yaml
    attrs['contributor_name'] = 'Callum Rollo, Louise Biddle, Olle Glader, Aleksandra Mazur, Marcus Melin, Gunnar Johnsson, Andrew Birkett, Michaela Edwinson, Anna Willstrand Wranne,Adele Maciute, Martin Mohrmann'
    attrs['contributor_role'] = 'Data Processor, PI, Head of Operations, Glider Technician, Glider Technician, Glider Technician, Glider Technician, Platform Coordinator, Project Manager, Glider Technician, Scientific Data Analyst'
    ds.attrs = attrs
    outname_voto = f"{ds_id}.nc"
    outpath_voto = out_path.parent / outname_voto
    drop_vars = {'profile_index', 'profile_direction'}.intersection(set(ds.data_vars))
    ds = ds.drop_vars(drop_vars)
    ds = post_process(ds)
    ds = encode_times(ds)
    ds.to_netcdf(outpath_voto)
    ds.close()
    out_path.unlink()
    mission_id = f'OG_{dataset_type}_{glider_serial}_M{deployment_id}'
    database.update_processed_time(mission_id, datetime.datetime.now())


def proc_one(glider, mission, kind):
    if kind =='raw':
        nc_out = proc_pyglider_og1(f"/data/data_raw/complete_mission/{glider}/M{mission}/",
                                   f"/data/data_l0_pyglider/OG_delayed/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'raw', reprocess=True)
    else:
        nc_out = proc_pyglider_og1(f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv",
                                   f"/data/data_l0_pyglider/OG_nrt/{glider}/M{mission}/",
                                   #f"/data/deployment_yaml/mission_yaml/OG_{glider}_M{str(mission)}.yml",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   'sub', reprocess=True)
    if not nc_out:
        return
    add_voto_stuff(nc_out)
    send_kind = 'nrt'
    if kind == 'delayed':
        send_kind = 'complete'
    subprocess.check_call(
        [
            "/usr/bin/bash",
            sync_script_dir / "send_to_erddap_og.sh",
            str(glider),
            str(mission),
            send_kind
        ],
    )

def proc_all(kind='nrt', days=7):
    cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
    logf = f"/data/log/{kind}_pyglider_og1.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    all_yamls = list(Path("/data/deployment_yaml/og1").glob("*.yaml"))
    mission_yamls = [yml for yml in all_yamls if "pyglider_mod" not in str(yml)]
    mission_yamls.sort()

    for yml_file in mission_yamls:
        fn = yml_file.name.split('.')[0]
        skips = ["SEA076_M9", "SEA076_M8", "SEA076_M48", "SEA057_M75"]
        if fn in skips:
            _log.error(f"skip {fn} for now")
            continue

        mission_id = f'OG_{kind}_{fn}'
        last_proc = database.last_processed_time(mission_id)
        if last_proc > cutoff:
            _log.info(f'{mission_id} processed after cutoff, skipping')
            continue
        glider, mission = fn.split('_M')
        pyglider_kind = 'sub' if kind=='nrt' else 'raw'
        if kind == 'nrt':
            indir = f"/data/data_raw/nrt/{glider}/{str(mission).zfill(6)}/C-Csv"
        else:
            indir = f"/data/data_raw/complete_mission/{glider}/M{mission}"
        nc_out = proc_pyglider_og1(indir,
                                   f"/data/data_l0_pyglider/OG_{kind}/{glider}/M{mission}/",
                                   f"/data/deployment_yaml/og1/{glider}_M{str(mission)}.yaml",
                                   pyglider_kind, reprocess=True)
        if not nc_out:
            continue
        add_voto_stuff(nc_out)
        send_kind = 'nrt'
        if kind=='delayed':
            send_kind = 'complete'
        subprocess.check_call(
            [
                "/usr/bin/bash",
                sync_script_dir / "send_to_erddap_og.sh",
                str(glider),
                str(mission),
                send_kind
            ],
        )
        database.update_processed_time(mission_id, datetime.datetime.now())


def process_new_nrt():
    logging.basicConfig(
        filename="/data/log/og1_pyglider_pipeline.log",
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _log.info("Start nrt processing")
    all_glider_paths = Path("/data/data_raw/nrt").glob("*")
    for glider_path in all_glider_paths:
        platform_serial = str(glider_path.parts[-1])
        if platform_serial in platforms_no_proc:
            _log.info(f"{platform_serial} is not to be processed. Skipping")
            continue
        _log.info(f"Checking {platform_serial}")
        mission_paths = list(glider_path.glob("00*/C-Csv"))
        if not mission_paths:
            _log.warning(f"No missions found for {platform_serial}. Skipping")
            continue
        mission_paths.sort()
        mission = str(mission_paths[-1].parts[-2]).lstrip("0")
        if (platform_serial, int(mission)) in missions_no_proc:
            _log.info(f"Will not process {platform_serial}, M{mission} as it is in missions_no_proc")
            continue
        _log.info(f"Checking {platform_serial} M{mission}")
        input_dir = f"/data/data_raw/nrt/{platform_serial}/{mission.zfill(6)}/C-Csv/"
        mission_id = f'OG_nrt_{platform_serial}_M{mission}'
        last_proc = database.last_processed_time(mission_id)
        max_time = np.datetime64(last_proc)
        in_files = natural_sort(glob.glob(f"{input_dir}*gli*"))
        if len(in_files) == 0:
            _log.info(f"no input gli files for {input_dir}. Skipping")
            continue
        max_dive_file = in_files[-1]
        try:
            df = pd.read_csv(
                max_dive_file,
                sep=";",
                parse_dates=True,
                index_col=0,
                dayfirst=True,
                nrows=10,
            )
            file_time = pd.Timestamp(df.index.max())
            if pd.Timestamp(max_time + np.timedelta64(10, "m")) > file_time:
                _log.info(f"No new {platform_serial} M{mission} input files")
                continue
        except:
            _log.info(f"failed time check on {max_dive_file}")
        proc_one(platform_serial, mission, 'nrt')
    _log.info("Finished nrt processing")


def main():
    parser = argparse.ArgumentParser(description="Generate ERDDAP dataset xml and rebuild datasets.xml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Parser for processing all nrt or delayed data
    all_parser = subparsers.add_parser("all", help="process OG1 datasets older than a number of days (proc_pyglider_og1)")
    all_parser.add_argument("--kind", choices=["nrt", "delayed"], default="nrt",
                            help="type of dataset to process (default: nrt)")
    all_parser.add_argument("--days", type=int, default=7,
                            help="skip datasets processed within this many days (default: 3)")

    # Parser for processing one mission

    one_parser = subparsers.add_parser("one", help="process one OG1 dataset (proc_pyglider_og1)")
    one_parser.add_argument("--kind", choices=["nrt", "delayed"], default="nrt",
                            help="type of dataset to process (default: nrt)")
    one_parser.add_argument("glider", type=str, help="glider serial")
    one_parser.add_argument("mission", type=int, help="glider mission")

    # Parser for cron driven nrt processing
    subparsers.add_parser("pipeline", help="process fresh nrt data, run as pipeline")



    args = parser.parse_args()
    if args.command == "all":
        proc_all(kind=args.kind, days=args.days)
    elif args.command == "one":
        glider = args.glider
        if len(glider) < 3:
            glider = f"SEA{str(glider).zfill(3)}"
        proc_one(glider, args.mission, args.kind)
    elif args.command == "pipeline":
        process_new_nrt()


if __name__ == "__main__":
    main()