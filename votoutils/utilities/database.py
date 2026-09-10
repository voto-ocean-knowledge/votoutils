import datetime
import pandas as pd
import sqlite3
from contextlib import closing

db_file = "/data/log/proc2.db"

def init_db(connection):
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS processed ("
        "id INTEGER PRIMARY KEY, platform_mission TEXT, proc_time TEXT)"
    )
    connection.commit()


def display_processed():
    with closing(sqlite3.connect(db_file)) as connection:
        cursor = connection.cursor()
        output = list(cursor.execute("SELECT * FROM processed").fetchall())
        missions = [result[1] for result in output]
        ptimes = [result[2] for result in output]
        df = pd.DataFrame({'missions': missions, 'datetime': ptimes})
        df = df.sort_values('missions')
        print(df)


def last_processed_time(mission_id):
    with closing(sqlite3.connect(db_file)) as connection:
        init_db(connection)
        cursor = connection.cursor()
        existing = cursor.execute('''SELECT id, platform_mission, proc_time FROM processed WHERE platform_mission=?''',
                                  (mission_id,)).fetchone()
        if existing:
            return datetime.datetime.fromisoformat(existing[2])
        return datetime.datetime(1970,1,1)
    


def update_processed_time(mission_id, proc_time):
    proc_time = proc_time.isoformat()
    with closing(sqlite3.connect(db_file)) as connection:
        init_db(connection)
        cursor = connection.cursor()
        existing = cursor.execute('''SELECT id, platform_mission, proc_time FROM processed WHERE platform_mission=?''',
                                  (mission_id,)).fetchone()
        if existing:
            cursor.execute(f"UPDATE processed SET proc_time = '{proc_time}' WHERE id = {existing[0]}")
        else:
            cursor.execute(
                f"INSERT INTO processed (platform_mission, proc_time) VALUES (?, ?)",
                (mission_id, proc_time))

        connection.commit()


if __name__ == '__main__':
    display_processed()