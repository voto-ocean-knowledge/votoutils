import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import json
from votoutils.utilities.utilities import mailer

script_dir = Path(__file__).parent.parent.parent.absolute()
with open(script_dir / 'contacts_secrets.json', 'r') as secrets_file:
    contacts = json.load(secrets_file)
    
with open(script_dir / 'alarm_secrets.json', 'r') as secrets_file:
    secrets_dict = json.load(secrets_file)


def parse_schedule():
    schedule = pd.read_csv('https://docs.google.com/spreadsheets/d/' +
                           secrets_dict['google_sheet_id'] +
                           '/export?gid=0&format=csv',
                           index_col=0,
                           ).rename({'handover-am (UTC)': 'handover-am', 'handover-pm (UTC)': 'handover-pm'}, axis=1)
    schedule.dropna(subset='pilot-day', inplace=True)
    schedule.index = pd.to_datetime(schedule.index)
    schedule.loc[schedule['handover-am'] > 24, 'handover-am'] = np.nan
    schedule.loc[schedule['handover-am'] < 0, 'handover-am'] = np.nan
    schedule.loc[schedule['handover-pm'] > 24, 'handover-pm'] = np.nan
    schedule.loc[schedule['handover-pm'] < 0, 'handover-pm'] = np.nan

    now = datetime.datetime.now()
    local_now = now.astimezone()
    offset_dt = local_now.utcoffset()
    offset = int(offset_dt.seconds / 3600)

    schedule['handover-am'] = schedule['handover-am'].fillna(9 - offset)
    schedule['handover-pm'] = schedule['handover-pm'].fillna(17 - offset)

    df = pd.DataFrame({'pilot': ['callum']}, index=[pd.to_datetime('1970-01-01')])
    for i, row in schedule.iterrows():
        day_start = i + np.timedelta64(int(row["handover-am"]), 'h')
        day_row = pd.DataFrame({'pilot': [row['pilot-day']],
                                'supervisor': [row['on-call']],
                                #'surface-text': [row['surface-text-day']],
                                }, index=[day_start])
        df = pd.concat([df, day_row])

        night_start = i + np.timedelta64(int(row["handover-pm"]), 'h')
        night_row = pd.DataFrame({'pilot': [row['pilot-night']],
                                  'supervisor': [row['on-call']],
                                  #'surface-text': [row['surface-text-night']],
                                  }, index=[night_start])
        df = pd.concat([df, night_row])

    for col in df.columns:
        df[col] = df[col].str.lower()

    strings = list(pd.unique(df[df.columns].values.ravel('K')))
    names = []
    for name_str in strings:
        if type(name_str) is not str:
            continue
        name_str = name_str.replace(' ', '')
        if ',' in name_str:
            parts = name_str.split(',')
            for part in parts:
                names.append(part)
        else:
            names.append(name_str)

    bad_names = []
    for name in set(names):
        if name not in contacts.keys():
            df.replace(name, "", inplace=True, regex=True)
            bad_names.append(name)
    if len(bad_names) > 0:
        mailer("bad names in schedule", f"The following names have been ignored: {bad_names}")
    for name, number in contacts.items():
        df.replace(name, number, inplace=True, regex=True)
    df.to_csv("/data/log/schedule.csv", sep=';')


if __name__ == '__main__':
    try:
        parse_schedule()
    except:
        mailer("schedule", "parsing the schedule failed! Using the last good one")
