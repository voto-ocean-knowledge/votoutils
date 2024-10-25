from pathlib import Path
import datetime
import new_complete_missions


if __name__ == "__main__":
    logf = Path("/data/log/new_complete_mission.log")
    mtime = datetime.datetime.fromtimestamp(logf.lstat().st_mtime)
    time_elapsed = datetime.datetime.now() - mtime
    if time_elapsed > datetime.timedelta(minutes=5):
        new_complete_missions.main()
