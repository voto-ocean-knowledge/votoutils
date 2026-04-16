import polars as pl
import numpy as np
import pandas as pd
import datetime
import logging
import glob
from pathlib import Path

_log = logging.getLogger(__name__)


def clean_2019(infile):
    filepath = Path(infile)
    if ".gli." not in infile and ".pld1." not in infile:
        return
    try:
        df = pl.read_csv(infile, separator=";", ignore_errors=True)
    except Exception as e:
        _log.warning(f"Exception reading {infile}: {e}")
        _log.warning(f"Could not read {infile}. Deleting")
        filepath.unlink()
        return
    try:
        if "Timestamp" in df.columns:
            df = df.with_columns(
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, format="%d/%m/%Y %H:%M:%S"
                ),
            )
            df = df.rename({"Timestamp": "time"})
        else:
            df = df.with_columns(
                pl.col("PLD_REALTIMECLOCK").str.strptime(
                    pl.Datetime, format="%d/%m/%Y %H:%M:%S.%3f"
                ),
            )
            df = df.rename({"PLD_REALTIMECLOCK": "time"})
    except:
        _log.warning(f"{infile} cannot parse datetime columns. Deleting")
        filepath.unlink()
        return
    try:
        min_time = df["time"].min()
    except:
        _log.warning(f"{infile} cannot get min time. Deleting")
        filepath.unlink()
        return
    if not min_time:
        _log.warning(f"{infile} has no min time. Deleting")
        filepath.unlink()
        return
    dfa = pd.read_csv(filepath, names=['rawstring'])
    sep_count = dfa['rawstring'].str.count(';').values
    if sep_count[-1] < np.nanmedian(sep_count):
        _log.warning(f"{infile} final line is incomplete. removing")
        with open(infile) as fin:
            lines = fin.readlines()
        with open(infile, 'w') as fout:
            fout.writelines(lines[:-1])
    if df["time"].min() > datetime.datetime(2020, 1, 1):
        return True
    years = np.array(df["time"].dt.year().to_list())
    if len(years[years < 2020]) / len(years) > 0.8:
        _log.warning(f"{infile} has > 80 % invalid dates. Deleting")
        filepath.unlink()
        return
    return True


def clean_infiles(in_dir):
    all_infiles = glob.glob(f"{str(in_dir)}/sea*")
    for infile in all_infiles:
        clean_2019(infile)
