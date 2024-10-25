from pathlib import Path
import pandas as pd
import numpy as np
from votoutils.utilities import utilities


def compute_glider_stats():
    try:
        df_stats = pd.read_csv("/home/pipeline/stats.csv")
    except FileNotFoundError:
        df_stats = pd.DataFrame(
            {"glider": [], "mission": [], "ctd_period": [], "oxy_period": []},
        )

    glider_paths = list(Path("/data/data_raw/complete_mission").glob("SEA*"))
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            glider = int(glider_path.parts[-1][3:])
            mission = int(mission_path.parts[-1][1:])
            a = [np.logical_and(df_stats.glider == glider, df_stats.mission == mission)]
            if sum(sum(a)):
                continue
            try:
                sampling_periods = utilities.sensor_sampling_period(glider, mission)
                new_row = pd.DataFrame(
                    sampling_periods,
                    index=[len(df_stats)],
                )
                df_stats = pd.concat((df_stats, new_row))
            except IndexError:
                print(f"fail for SEA{glider} M{mission}")
        df_stats.to_csv("/home/pipeline/stats.csv", index=False)


if __name__ == "__main__":
    compute_glider_stats()
