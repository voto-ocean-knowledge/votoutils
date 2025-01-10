import json
import pandas as pd
from pathlib import Path
import requests
import logging
import datetime
import email
import imaplib
import sys
from votoutils.utilities.utilities import mailer
_log = logging.getLogger(name='core_log')

script_dir = Path(__file__).parent.parent.parent.absolute()

format_basic = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
format_alarm = logging.Formatter('%(asctime)s,%(message)s', datefmt="%Y-%m-%d %H:%M:%S")

mail_alarms_json = Path('/data/log/mail_alarms.json')
with open(script_dir / 'alarm_secrets.json', 'r') as secrets_file:
    secrets_dict = json.load(secrets_file)
with open(script_dir / 'contacts_secrets.json', 'r') as secrets_file:
    contacts = json.load(secrets_file)

schedule = pd.read_csv('/data/log/schedule.csv', parse_dates=True, index_col=0, sep=';', dtype=str)
for name, number in contacts.items():
    schedule.replace(name, number, inplace=True, regex=True)
now = datetime.datetime.now()
row = schedule[schedule.index < now].iloc[-1]
pilot_phone = row['pilot']
supervisor_phone = row['supervisor']
if type(supervisor_phone) is float:
    supervisor_phone = None
if type(pilot_phone) is str:
    pilot_phone = pilot_phone.replace(" ", "")
if type(supervisor_phone) is str:
    supervisor_phone = supervisor_phone.replace(" ", "")

def extra_alarm_recipients():
    votoweb_dir = secrets_dict["votoweb_dir"]
    sys.path.append(votoweb_dir)
    from voto.data.db_classes import User # noqa
    from voto.bin.add_profiles import init_db # noqa
    init_db()
    users_to_alarm = User.objects(alarm=True)
    users_to_alarm_surface = User.objects(alarm_surface=True)
    numbers = []
    numbers_surface = []
    for user in users_to_alarm:
        if user.name not in contacts.keys():
            _log.error(f"Did not find user {user.name} in contacts")
            mailer("Missing number",f"Did not find user {user.name} in contacts")
            continue
        number = contacts[user.name]
        if number == pilot_phone:
            continue
        numbers.append(number)
    for user in users_to_alarm_surface:
        if user.name not in contacts.keys():
            _log.error(f"Did not find user {user.name} in contacts")
            mailer("Missing number",f"Did not find user {user.name} in contacts")
            continue
        number = contacts[user.name]
        numbers_surface.append(number)
    return numbers, numbers_surface

extra_alarm_numbers = []
extra_alarm_numbers_surface = []

try:
    extra_alarm_numbers, extra_alarm_numbers_surface = extra_alarm_recipients()
except:
    mailer("Failed extra numbers", f"Could not do it")


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
    if ddict['security_level'] == 0:
        message = f"SURFACING {ddict['platform_id']} M{ddict['mission']} cycle {ddict['cycle']}. Source: {ddict['alarm_source']}"

    else:
        message = f"ALARM {ddict['platform_id']} M{ddict['mission']} cycle {ddict['cycle']} alarm code {ddict['security_level']}. Source: {ddict['alarm_source']}"
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
        alarm_log.info(f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},text_{user}, {ddict['alarm_source']}")
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
                                     'voice_start': '{"play":"https://callumrollo.com/files/frederik_short.mp3"}',
                                     'timeout': timeout_seconds
                                 }
                                 )
    _log.warning(f"ELKS CALL: {response.text}")
    if response.status_code == 200:
        alarm_log.info(f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},call_{user}, {ddict['alarm_source']}")
    else:
        _log.error(f"failed elks call {response.text}")


def contact_pilot(ddict, fake=True):
    _log.warning(f"PILOT")
    if "," in pilot_phone:
        for phone_number in pilot_phone.split(','):
            elks_text(ddict, recipient=phone_number, fake=fake)
            elks_call(ddict, recipient=phone_number, fake=fake)
    else:
        elks_text(ddict, fake=fake)
        elks_call(ddict, fake=fake)
    if extra_alarm_numbers:
        for extra_number in extra_alarm_numbers:
            elks_text(ddict, recipient=extra_number, fake=fake, user="self-volunteered")
            elks_call(ddict, recipient=extra_number, fake=fake, user="self-volunteered")


def contact_supervisor(ddict, fake=True):
    if not supervisor_phone:
        _log.warning("No supervisor on duty: no action")
        return
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

def surfacing_alerts(fake=True):
    # check what time email was last checked
    timefile = Path("lastcheck_surface.txt")
    if timefile.exists():
        with open(timefile, "r") as variable_file:
            for line in variable_file.readlines():
                last_check = datetime.datetime.fromisoformat((line.strip()))
    else:
        last_check = datetime.datetime(1970, 1, 1)
    # Write the time of this run
    with open(timefile, "w") as f:
        f.write(str(datetime.datetime.now()))
    if not extra_alarm_numbers_surface:
        _log.info("no one signed up for surfacing alerts")
        return
    _log.info("Check for surfacing emails")
    # Check gmail account for emails
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(secrets["email_username"], secrets["email_password"])
    mail.select("inbox")

    result, data = mail.search(None, "ALL")
    mail_ids = data[0]

    id_list = mail_ids.split()
    first_email_id = int(id_list[0])
    latest_email_id = int(id_list[-1])
    # Cut to last 10 emails
    if len(id_list) > 10:
        first_email_id = int(id_list[-10])

    # Check which emails have arrived since the last run of this script
    unread_emails = []
    for i in range(first_email_id, latest_email_id + 1):
        result, data = mail.fetch(str(i), "(RFC822)")

        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                date_tuple = email.utils.parsedate_tz(msg["Date"])
                if date_tuple:
                    local_date = datetime.datetime.fromtimestamp(
                        email.utils.mktime_tz(date_tuple),
                    )
                    if local_date > last_check:
                        unread_emails.append(i)

    # Exit if no new emails
    if not unread_emails:
        _log.info("No new mail")
        return
    _log.debug("New emails")

    # Check new emails
    for i in unread_emails:
        _log.debug(f"open mail {i}")
        result, data = mail.fetch(str(i), "(RFC822)")
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                email_subject = msg["subject"]
                if email_subject.lower()[:2] == 'fw':
                    email_subject = email_subject[4:]
                email_from = msg["from"]
                # If email is from alseamar and subject contains ALARM, make some noise
                if "administrateur@alseamar-cloud.com" in email_from and "ALARM" not in email_subject:
                    _log.warning(f"Surface {email_subject}")
                    parts = email_subject.split(' ')
                    glider = parts[0][1:-1]
                    mission = int(parts[1][1:])
                    cycle = int(parts[3][1:])
                    ddict = {'glider': int(glider[3:]), 'platform_id': glider, 'mission': mission, 'cycle': cycle, 'security_level': 0, 'alarm_source': "surfacing email"}
                    for surface_number in extra_alarm_numbers_surface:
                        elks_text(ddict, recipient=surface_number, fake=fake)
                        elks_call(ddict, recipient=surface_number, fake=fake)


if __name__ == '__main__':
    print(extra_alarm_recipients())
    surfacing_alerts(fake=True)
