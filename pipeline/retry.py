from pathlib import Path
import datetime
import pyglider_all_complete


if __name__ == "__main__":
    logf = Path("/data/log/pyglider_all_complete.log")
    mtime = datetime.datetime.fromtimestamp(logf.lstat().st_mtime)
    time_elapsed = datetime.datetime.now() - mtime
    if time_elapsed > datetime.timedelta(minutes=5):
        pyglider_all_complete.proc_all_complete()
