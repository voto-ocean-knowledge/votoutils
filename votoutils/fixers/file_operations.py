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
    file_paths = in_dir.glob("sea*sub*")
    for file_path in file_paths:
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
        except (pl.exceptions.ComputeError, pl.exceptions.InvalidOperationError):
            _log.info(f"Error reading {fn}. Removing whitespace from this file")
            with open(file_path, 'r') as infile:
                content = infile.read()
            content = content.replace(' ', '')
            with open(file_path, 'w') as infile:
                infile.write(content)
            goodlines = []
            with open(file_path) as f:
                goodline_len = 0
                i = 0
                num_semi = 0
                for line in f.readlines():
                    if i > 3:
                        if not num_semi:
                            num_semi = line.count(';')
                        if line.count("9999.0") < 4 and not goodline_len:
                            goodline_len = len(line)
                    if line.count(';') < num_semi:
                        _log.info(f"MISSING ; in {fn}: {line}")
                        continue
                    if len(line) + 30 < goodline_len:
                        _log.info(f"SHORT LINE {fn}: {line}")
                        continue
                    goodlines.append(line)
                    i += 1

            with open(file_path, "w") as f:
                for line in goodlines:
                    f.write(line)
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
    clean_nrt_bad_files(Path("/home/callum/Downloads/test-csv"))
    _log.info("Complete cleanup")
