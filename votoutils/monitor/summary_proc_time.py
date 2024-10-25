import pandas as pd
import datetime

df = pd.read_csv("/home/pipeline/reprocess.csv", parse_dates=["proc_time"])

dts = []
for i, row in df.iterrows():
    dur = row.duration
    if "day" in dur:
        dur = dur.split(" ")[-1]

    hours, mins, secs = dur.split(":")
    dt = datetime.timedelta(hours=int(hours), minutes=int(mins), seconds=int(secs[:2]))
    dts.append(dt)
df["dtime"] = dts

print(f"Total time: {df.dtime.sum()}")
print(df.dtime.describe())
longest_wait = datetime.datetime.now() - df.proc_time.min()
print(f"longest wait: {longest_wait}")
