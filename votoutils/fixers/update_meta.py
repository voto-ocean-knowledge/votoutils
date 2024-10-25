import os
import sys
import pathlib
import argparse
import logging
import shutil
import xarray as xr
import yaml
from itertools import chain

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
_log = logging.getLogger(__name__)


def nc_update(nc_path, yaml_path, tempfile):
    _log.info(f"working on {nc_path}")
    ds = xr.open_dataset(nc_path)
    with open(yaml_path) as fin:
        deployment = yaml.safe_load(fin)
    _log.info("read files successfully")
    meta = ds.attrs
    new_meta = deployment["metadata"]
    for key, value in new_meta.items():
        if key not in meta.keys():
            meta[key] = value
            _log.info(f"added {key}: {value}")
            continue
        if meta[key] == value:
            continue
        _log.info(f"updated {key}. old: {meta[key]}, new: {value}")
        meta[key] = value

    ds.attrs = meta
    ds.to_netcdf(tempfile)
    shutil.move(tempfile, nc_path)
    _log.info("Successfully saved nc")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="update processed netCDFs after changing yml metadata",
    )
    parser.add_argument("glider", type=int, help="glider number, e.g. 70")
    parser.add_argument("mission", type=int, help="Mission number, e.g. 23")
    parser.add_argument(
        "--kind",
        type=str,
        help="Kind of input. Cana specify nrt or full. Defaults to both",
    )
    args = parser.parse_args()
    if args.kind not in ["raw", "sub", None]:
        raise ValueError("kind must be raw or sub")
    logf = f"/data/log/update_meta/SEA{str(args.glider)}_M{str(args.mission)}.log"
    logging.basicConfig(
        filename=logf,
        filemode="w",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    yml_file = f"/data/deployment_yaml/mission_yaml//SEA{str(args.glider)}_M{str(args.mission)}.yml"
    types = []
    if args.kind in ["sub", None]:
        types.append("nrt")
    if args.kind in ["raw", None]:
        types.append("complete_mission")
    for sub_dir in types:
        root_dir = f"/data/data_l0_pyglider/{sub_dir}/SEA{str(args.glider)}/M{str(args.mission)}"
        _log.info(f"working on ncs in {root_dir}")
        nc_files = []
        for sub in ("profiles", "timeseries", "gridfiles"):
            nc_files.append(list(pathlib.Path(root_dir).glob(f"**/{sub}/*.nc")))
        nc_files_flat = list(chain.from_iterable(nc_files))
        if not nc_files:
            _log.error(f"no ncs found in path {root_dir}")
        _log.info(f"Found {len(nc_files_flat)}")
        for nc in nc_files_flat:
            temp_nc = pathlib.Path("/tmp") / pathlib.Path(nc).name
            nc_update(nc, yml_file, temp_nc)
        _log.info(f"Updated all ncs in {root_dir}")
    _log.info("Success! Updated all ncs")
