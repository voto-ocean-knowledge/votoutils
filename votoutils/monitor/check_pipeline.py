"""
This script checks that the other steps of the pipeline are running as expected.
To monitor a file, add the path to it and part of its expected last line to the files_dict
"""

import datetime

import re
import pandas as pd
from votoutils.utilities.utilities import mailer
import logging

_log = logging.getLogger(__name__)

files_collection = (
    ("voto_stats_data.log", "Finished computing stats", 2),
    ("pyglider_nrt.log", "Finished nrt processing", 2),
    ("voto_add_sailbuoy.log", "Finished download of sailbuoy data", 2),
    ("sailbuoy.log", "Finished processing nrt sailbuoy data", 2),
    ("voto_add_data.log", "nrt mission add complete", 2),
    ("nrt_plots.log", "End plot creation", 2),
    ("office_sync_to_pipeline.log", "complete", 25),
    ("rsync_nrt.log", "total size is", 2),
    ("rsync_metocc.log", "total size is", 2),
    ("rsync_web.log", "total size is", 2),
    ("erddap_rsync.log", "total size is", 2),
    ("seaex-rsync.log", "total size is", 2),
    ("new_complete_mission.log", "Complete", 25),
    ("ctd_plots.log", "completed process all CTDs", 25),
    #("glider_transect.log", "End analysis", 2),
    ("metadata_tables.log", "End processing", 25),
    ("cherrdap.log", "ERDDAP checks complete", 25),
)


def check_log_file(file, expected_last_line, hours):
    file_loc = f"/data/log/{file}"
    try:
        df = pd.read_csv(file_loc, sep="never in a million yrs", engine="python")
        skiplines = len(df) - 50
    except pd.errors.ParserError:
        skiplines = 0
    last_line = ""
    dt_base = datetime.datetime(1970, 1, 1)
    dt_sh = datetime.datetime(1970, 1, 1)
    dt_py = datetime.datetime(1970, 1, 1)
    with open(file_loc) as f:
        for i, line in enumerate(f):
            if i < skiplines:
                continue
            if len(line) > 5:
                last_line = line
                match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line)
                if match:
                    dt_py = datetime.datetime.strptime(
                        match.group(),
                        "%Y-%m-%d %H:%M:%S",
                    )
                match_iso = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", line)
                if match_iso:
                    dt_sh = datetime.datetime.strptime(
                        match_iso.group(),
                        "%Y-%m-%dT%H:%M:%S",
                    )
    if expected_last_line not in last_line:
        msg = f"failed process: {file} ends in {last_line}"
        mailer("pipeline-error", msg)
        return
    dt = max((dt_base, dt_py, dt_sh))
    if dt < datetime.datetime.now() - datetime.timedelta(hours=hours):
        msg = f"failed process: {file} last updates {dt}"
        mailer("pipeline-stale", msg)


if __name__ == "__main__":
    logging.basicConfig(
        filename="/data/log/pipeline.log",
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    check_log_file("pipeline.log", "log checks complete", 1)
    _log.info("start pipeline check")
    for filename, file_last_line, hours in files_collection:
        _log.info(f"Check {filename}")
        check_log_file(filename, file_last_line, hours)
    _log.info("log checks complete")
