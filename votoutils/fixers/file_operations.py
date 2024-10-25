import subprocess
import logging
import polars as pl
from pathlib import Path

_log = logging.getLogger(__name__)

bad_dives = [
    "sea078.15.gli.sub.1",
    "sea078.15.pld1.sub.1",
    "sea077.25.gli.sub.1",
    "sea077.25.pld1.sub.1",
]


def clean_nrt_bad_files(in_dir):
    _log.info(f"Start cleanup of nrt files from {in_dir}")
    in_dir = Path(in_dir)
    for file_path in in_dir.glob("sea*sub*"):
        fn = file_path.name
        if fn in bad_dives:
            _log.info(f"Removing bad dive {fn}")
            subprocess.check_call(["/usr/bin/rm", str(file_path)])
            continue
        try:
            out = pl.read_csv(file_path, separator=";")
            if "Timestamp" in out.columns:
                out.with_columns(
                    pl.col("Timestamp").str.strptime(
                        pl.Datetime,
                        format="%d/%m/%Y %H:%M:%S",
                    ),
                )
            else:
                out.with_columns(
                    pl.col("PLD_REALTIMECLOCK").str.strptime(
                        pl.Datetime,
                        format="%d/%m/%Y %H:%M:%S.%3f",
                    ),
                )
            for col_name in out.columns:
                if "time" not in col_name.lower() or col_name == "NOC_SAMPLE_TIME":
                    out = out.with_columns(pl.col(col_name).cast(pl.Float64))
        except pl.exceptions.ComputeError:
            _log.info(f"Error reading {fn}. Cutting the last line from this file")
            subprocess.run(["sed", "-i", "$ d", str(file_path)])
    _log.info(f"Complete cleanup of nrt files from {in_dir}")


if __name__ == "__main__":
    logging.basicConfig(
        filename="/data/log/clean_files.log",
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _log.info("Start cleanup")
    clean_nrt_bad_files(Path("/data/data_raw/nrt/SEA044/000090/C-Csv/"))
    _log.info("Complete cleanup")
