import json
import pandas as pd
from pathlib import Path
import requests
import logging
import datetime
import time
from votoutils.alerts.read_mail import read_email_from_gmail

script_dir = Path(__file__).parent.parent.parent.absolute()
format_basic = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
format_alarm = logging.Formatter('%(asctime)s,%(message)s', datefmt="%Y-%m-%d %H:%M:%S")


def setup_logger(name, log_file, level=logging.INFO, formatter=format_basic):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# first file logger
_log = setup_logger('first_logger', '/data/log/alarms.log')

# second file logger
alarm_log = setup_logger('second_logger', '/data/log/alarms_sent.log', formatter=format_alarm)

alarms_json = '/data/log/alarms.json'
with open(script_dir / 'alarm_secrets.json', 'r') as secrets_file:
    secrets_dict = json.load(secrets_file)
pilot_phone = secrets_dict['recipient']
supervisor_phone = secrets_dict['recipient']


def fill(glider_num):
    return str(glider_num).zfill(3)


def parse_mrs(comm_log_file):
    df_in = pd.read_csv(comm_log_file, names=['everything'], sep='Neverin100years', engine='python',
                        on_bad_lines='skip', encoding='latin1')
    if "trmId" in df_in.everything[0]:
        _log.error(f"old logfile type in {comm_log_file}. skipping")
        return pd.DataFrame()
    df_mrs = df_in[df_in['everything'].str.contains('SEAMRS')].copy()
    parts = df_mrs.everything.str.split(';', expand=True)
    df_mrs['datetime'] = pd.to_datetime(parts[0].str[1:-1], dayfirst=True)
    df_mrs['message'] = parts[5]
    msg_parts = df_mrs.message.str.split(',', expand=True)
    df_mrs['glider'] = msg_parts[1].str[3:].astype(int)
    df_mrs['mission'] = msg_parts[2].astype(int)
    df_mrs['cycle'] = msg_parts[3].astype(int)
    df_mrs['security_level'] = msg_parts[4].astype(int)
    df_mrs = df_mrs[['cycle', 'datetime', 'glider', 'mission', 'security_level']]
    df_mrs['alarm'] = False
    df_mrs.loc[df_mrs.security_level > 0, 'alarm'] = True
    df_mrs = df_mrs.sort_values('datetime')
    return df_mrs


def get_last_check_time(glider_num):
    default_time = pd.to_datetime('1970-01-01')
    if not Path('alarms.json').exists():
        return default_time
    with open('alarms.json', 'r') as f:
        glider_dict = json.load(f)
    if glider_num not in glider_dict.keys():
        return default_time
    try:
        prev_last_line_dict = glider_dict[glider_num]
        last_check = pd.to_datetime(prev_last_line_dict['datetime'])
    except:
        return default_time
    return last_check


def update_glider_dict(glider_num, last_line_dict):
    if Path(alarms_json).exists():
        with open(alarms_json, 'r') as f:
            glider_dict = json.load(f)
    else:
        glider_dict = {}
    last_line_dict['datetime'] = str(last_line_dict['datetime'])

    glider_dict[glider_num] = last_line_dict
    with open('alarms.json', 'w') as f:
        json.dump(glider_dict, f)


def elks_text(ddict, recipient=pilot_phone, user='pilot'):
    message = f"SEA{fill(ddict['glider'])} M{ddict['mission']} cycle {ddict['cycle']} alarm code {ddict['security_level']}"
    response = requests.post('https://api.46elks.com/a1/sms',
                             auth=(secrets_dict['elks_username'], secrets_dict['elks_password']),
                             data={
                                 'from': 'GliderAlert',
                                 'to': recipient,
                                 'message': message,
                                 'dryrun': 'yes',
                             }
                             )
    _log.warning(f"ELKS SEND: {response.text}")
    if response.status_code == 200:
        alarm_log.info(f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},text_{user}")
    else:
        _log.error(f"failed elks text {response.text}")


def elks_call(ddict, recipient=pilot_phone, user='pilot'):
    response = requests.post('https://api.46elks.com/a1/sms',
                             auth=(secrets_dict['elks_username'], secrets_dict['elks_password']),
                             data={
                                 'from': 'GliderAlert',
                                 'to': recipient,
                                 'message': "this is a fake call",
                                 'dryrun': 'yes',
                             }
                             )

    """
    response = requests.post('https://api.46elks.com/a1/calls',
                             auth=(secrets_dict['elks_username'], secrets_dict['elks_password']),
                             data={
                                 'from': secrets_dict['elks_phone'],
                                 'to': recipient,
                                 'voice_start': '{"play":"https://46elks.com/static/sound/make-call.mp3"}'
                             }
                             )
    """
    _log.warning(f"ELKS CALL: {response.text}")
    if response.status_code == 200:
        alarm_log.info(f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},call_{user}")
    else:
        _log.error(f"failed elks call {response.text}")


def find_previous_action(ddict):
    if not Path('/data/log/alarms_sent.log'):
        return pd.DataFrame()
    df = pd.read_csv('/data/log/alarms_sent.log', names=['datetime', 'glider', 'mission', 'cycle', 'alarm', 'action'],
                     parse_dates=['datetime'])
    if df.empty:
        return pd.DataFrame()
    df = df[(df.glider == ddict['glider']) & (df.mission == ddict['mission']) & (df.cycle == ddict['cycle'])]
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values('datetime')
    return df


def contact_pilot(ddict):
    _log.warning(f"PILOT")
    elks_text(ddict)
    time.sleep(1)
    elks_call(ddict)


def contact_supervisor(ddict):
    _log.warning(f"ESCALATE")
    elks_text(ddict, recipient=supervisor_phone, user='supervisor')
    time.sleep(1)
    elks_call(ddict, recipient=supervisor_phone, user='supervisor')


def alarm(ddict):
    glider = ddict['glider']
    if not ddict['security_level']:
        _log.info(f"SEA{str(glider).zfill(3)} M{ddict['mission']} cycle {ddict['cycle']} alarm cleared")
        return
    df_action = find_previous_action(ddict)
    if df_action.empty:
        previous_action = "None"
    else:
        previous_action = df_action.iloc[-1].to_dict()['action']
    _log.warning(f"previous action: {previous_action}")
    if previous_action == "None":
        contact_pilot(ddict)

    if 'pilot' in previous_action:
        _log.warning(f"Will we escalate? {df_action.iloc[-1].to_dict()['datetime']} ")
        if df_action.iloc[-1].to_dict()['datetime'] < datetime.datetime.now() - datetime.timedelta(seconds=30):
            contact_supervisor(ddict)
    return


def check_glider(base_dir, glider_num):
    comm_log_files = list(base_dir.glob("0*/G-Logs/*com.raw.log"))
    comm_log_files.sort()
    if len(comm_log_files) == 0:
        _log.error(f"No comm log file found in {base_dir}")
        return
    comm_log_file = comm_log_files[-1]
    df = parse_mrs(comm_log_file)
    if df.empty:
        return

    last_check = get_last_check_time(glider_num)
    df = df[df.datetime > last_check]
    if df.empty:
        _log.debug(f'no new lines from SEA{str(glider_num).zfill(3)}')
        return
    last_line_dict = df.iloc[-1].to_dict()

    if not df[df.alarm].empty:
        last_line_dict['glider'] = glider_num
        alarm(last_line_dict)

    update_glider_dict(glider_num, last_line_dict)


def check_all_gliders(base_dir):
    all_glider_dirs = list(base_dir.glob("SEA*"))
    all_glider_dirs.sort()
    for glider_dir in all_glider_dirs:
        glider_num = int(glider_dir.parts[-1][3:])
        if glider_num in (57, 70):
            _log.debug(f"Skip Bastiens glider {fill(glider_num)}")
            continue
        _log.debug(f'check SEA{fill(glider_num)}')
        check_glider(glider_dir, glider_num)
        _log.debug(f'complete SEA{fill(glider_num)}')


def mail_alert(subject_line):
    if subject_line[:3] == 'FW:':
        subject_line = subject_line[4:]
    parts = subject_line.strip(' ').split(' ')
    ddict = {'glider': int(parts[0][4:-1]),
             'mission': int(parts[1][1:]),
             'cycle': int(parts[3][1:]),
             'security_level': int(parts[-1][6:-1])}
    alarm(ddict)


if __name__ == '__main__':
    _log.info("START CHECK")
    _log.info("start email check")
    read_email_from_gmail(mail_alert)
    _log.info("complete email check")
    base_data_dir = Path(f"/data/data_raw/nrt/")
    check_all_gliders(base_data_dir)
    _log.info("COMPLETE CHECK")

