import json
import pandas as pd
from pathlib import Path
import requests
import logging
import datetime
import email
import imaplib
_log = logging.getLogger(name='core_log')

script_dir = Path(__file__).parent.parent.parent.absolute()

format_basic = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
format_alarm = logging.Formatter('%(asctime)s,%(message)s', datefmt="%Y-%m-%d %H:%M:%S")

mail_alarms_json = Path('/data/log/mail_alarms.json')
with open(script_dir / 'alarm_secrets.json', 'r') as secrets_file:
    secrets_dict = json.load(secrets_file)

schedule = pd.read_csv('/data/log/schedule.csv', parse_dates=True, index_col=0, sep=';', dtype=str)
now = datetime.datetime.now()
row = schedule[schedule.index < now].iloc[-1]
pilot_phone = row['pilot']
supervisor_phone = row['supervisor']


def setup_logger(name, log_file, level=logging.INFO, formatter=format_basic):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def find_previous_action(df, ddict):
    if df.empty:
        return df
    df = df[(df.mission == ddict['mission']) & (df.cycle == ddict['cycle'])]
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values('datetime')
    return df


def parse_mrs(comm_log_file):
    df_in = pd.read_csv(comm_log_file, names=['everything'], sep='Neverin100years', engine='python',
                        on_bad_lines='skip', encoding='latin1')
    if "trmId" in df_in.everything[0]:
        _log.warning(f"old logfile type in {comm_log_file}. skipping")
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


def elks_text(ddict, recipient=pilot_phone, user='pilot', fake=True):
    alarm_log = logging.getLogger(name=ddict['platform_id'])

    message = f"{ddict['platform_id']} M{ddict['mission']} cycle {ddict['cycle']} alarm code {ddict['security_level']}"
    data = {
        'from': 'VOTOalert',
        'to': recipient,
        'message': message,
    }
    if fake:
        data['dryrun'] = 'yes'
    response = requests.post('https://api.46elks.com/a1/sms',
                             auth=(secrets_dict['elks_username'], secrets_dict['elks_password']),
                             data=data
                             )
    _log.warning(f"ELKS SEND: {response.text}")
    if response.status_code == 200:
        alarm_log.info(f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},text_{user}")
    else:
        _log.error(f"failed elks text {response.text}")


def elks_call(ddict, recipient=pilot_phone, user='pilot', fake=True, timeout_seconds=60):
    alarm_log = logging.getLogger(name=ddict['platform_id'])
    if fake:
        response = requests.post('https://api.46elks.com/a1/sms',
                                 auth=(secrets_dict['elks_username'], secrets_dict['elks_password']),
                                 data={
                                     'from': 'GliderAlert',
                                     'to': recipient,
                                     'message': "this is a fake call",
                                     'dryrun': 'yes',
                                 }
                                 )
    else:
        response = requests.post('https://api.46elks.com/a1/calls',
                                 auth=(secrets_dict['elks_username'], secrets_dict['elks_password']),
                                 data={
                                     'from': secrets_dict['elks_phone'],
                                     'to': recipient,
                                     'voice_start': '{"play":"https://46elks.com/static/sound/make-call.mp3"}',
                                     'timeout': timeout_seconds
                                 }
                                 )
    _log.warning(f"ELKS CALL: {response.text}")
    if response.status_code == 200:
        alarm_log.info(f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},call_{user}")
    else:
        _log.error(f"failed elks call {response.text}")


def contact_pilot(ddict, fake=True):
    _log.warning(f"PILOT")
    elks_text(ddict, fake=False)
    elks_call(ddict, fake=fake)


def contact_supervisor(ddict, fake=True):
    _log.warning(f"ESCALATE")
    elks_text(ddict, recipient=supervisor_phone, user='supervisor', fake=fake)
    elks_call(ddict, recipient=supervisor_phone, user='supervisor', fake=fake)


with open(script_dir / "email_secrets.json") as json_file:
    secrets = json.load(json_file)


def parse_mail_alarms():
    # Check gmail account for emails
    start = datetime.datetime.now()
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(secrets["email_username"], secrets["email_password"])
    mail.select("inbox")
    result, data = mail.search(None, '(SUBJECT "ALARM")')
    mail_ids = data[0]

    id_list = mail_ids.split()

    # read in previous alarms record
    if mail_alarms_json.exists():
        with open(mail_alarms_json, 'r') as f:
            glider_alerts = json.load(f)
    else:
        glider_alerts = {}

    # Check 10 newest emails
    for i in id_list[-10:]:
        result, data = mail.fetch(i, "(RFC822)")
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                email_subject = msg["subject"]
                if "fw" in email_subject.lower():
                    email_subject = email_subject[4:]
                email_from = msg["from"]
                # If email is from alseamar and subject contains ALARM, make some noise
                if (
                    "administrateur@alseamar-cloud.com" in email_from
                    or "calglider" in email_from
                    and "ALARM" in email_subject
                ):
                    _log.debug(f"email alarm parsed {email_subject}")
                    parts = email_subject.split(' ')
                    glider = parts[0][1:-1]
                    mission = int(parts[1][1:])
                    cycle = int(parts[3][1:])
                    alarm = int(parts[4][6:-1])
                    glider_alerts[glider] = (mission, cycle, alarm)
    with open(mail_alarms_json, 'w') as f:
        json.dump(glider_alerts, f, indent=4)
    elapsed = datetime.datetime.now() - start
    _log.info(f"Completed mail check in {elapsed.seconds} seconds")


if __name__ == '__main__':
    parse_mail_alarms()
