import pandas as pd
from pathlib import Path
import datetime
import json
import logging
from votoutils.alerts.alarm_utils import setup_logger, format_alarm, secrets_dict, contact_pilot, contact_supervisor, \
    find_previous_action, parse_mrs, mail_alarms_json, parse_mail_alarms, surfacing_alerts

_log = setup_logger('core_log', '/data/log/alarms.log', level=logging.DEBUG)


class Dispatcher:
    def __init__(self, platform_id):
        self.platform_num = int(platform_id[-3:])
        self.platform_id = platform_id
        schedule = pd.read_csv('/data/log/schedule.csv', parse_dates=True, index_col=0, sep=';', dtype=str)
        now = datetime.datetime.now()
        row = schedule[schedule.index < now].iloc[-1]
        self.pilot_phone = row['pilot']
        self.supervisor_phone = row['supervisor']
        self.alarm_log = f'/data/log/alarm_{platform_id}.log'
        self.df_alarm = pd.DataFrame()
        self.base_dir = Path(secrets_dict['base_data_dir']) / self.platform_id
        self.df_mrs = pd.DataFrame()
        self.alarm_dict = {}
        self.dummy_calls = False
        self.alarm_source = None
        setup_logger(platform_id, self.alarm_log, formatter=format_alarm, level=logging.INFO)

    def load_alarm_log(self):
        if Path(self.alarm_log).exists():
            self.df_alarm = pd.read_csv(self.alarm_log,
                                        names=["datetime","glider","mission","cycle","security_level","action", "alarm_source"],
                                        parse_dates=["datetime"])

    def load_comm_log(self):
        _log.info(f"Check {self.platform_id}")
        comm_log_files = list(self.base_dir.glob("0*/G-Logs/*com.raw.log"))
        comm_log_files.sort()
        if len(comm_log_files) == 0:
            _log.warning(f"No comm log file found in {self.base_dir}")
            return
        comm_log_file = comm_log_files[-1]
        self.df_mrs = parse_mrs(comm_log_file)

    def check_comm_log(self):
        df = self.df_mrs
        if df.empty:
            return False
        self.alarm_dict = df.iloc[-1].to_dict()
        if df.iloc[-1]['datetime'] < datetime.datetime.now() - datetime.timedelta(hours=6):
            _log.info(f"Stale log from {self.platform_id}")
            return False
        if not self.df_alarm.empty:
            last_action = self.df_alarm['datetime'].values[-1]
            df = df[df.datetime > last_action]
        if df.empty:
            _log.info(f'no new lines from {self.platform_id}')
            return False
        self.alarm_dict = df.iloc[-1].to_dict()
        if df[df.alarm].empty:
            _log.info(f"No alarms for {self.platform_id}")
            return False
        ddict = self.alarm_dict
        if not ddict['security_level']:
            _log.info(f"Alarm cleared {self.platform_id} M{ddict['mission']} cycle {ddict['cycle']}")
            return False
        self.alarm_source = "GLIMPSE comm log"
        return True
    
    def mail_alarm(self):
        if not Path(mail_alarms_json).exists():
            _log.warning("No email alerts json")
            return False
        with open(mail_alarms_json, 'r') as f:
            mail_alerts = json.load(f)

        if self.platform_id not in mail_alerts.keys():
            _log.debug(f"{self.platform_id} not in email alerts json")
            return False

        alarm_tuple = mail_alerts[self.platform_id]
        email_dict = {'cycle': alarm_tuple[1], 'glider': self.platform_num, 'mission': alarm_tuple[0],
                      'security_level': alarm_tuple[2], 'alarm': True}

        if not self.alarm_dict:
            _log.info("email alert to process")
            self.alarm_dict = email_dict
            self.alarm_source = "alseamar email"
            return True
        if self.alarm_dict['mission'] > email_dict['mission']:
            _log.info(f"stale email. Skipping. mission: {self.alarm_dict['mission']} vs {email_dict['mission']}, "
                      f"cycle  {self.alarm_dict['cycle']} vs {email_dict['cycle']}")
            return False

        if self.alarm_dict['mission'] >= email_dict['mission'] and self.alarm_dict['cycle'] >= email_dict['cycle']:
            _log.info(f"stale email. Skipping. mission: {self.alarm_dict['mission']} vs {email_dict['mission']}, "
                       f"cycle  {self.alarm_dict['cycle']} vs {email_dict['cycle']}")
            return False

        self.alarm_dict = email_dict
        self.alarm_source = "alseamar email"
        return True

    def trigger_alarm(self):
        if not self.alarm_dict:
            return
        ddict = self.alarm_dict
        ddict['platform_id'] = self.platform_id
        ddict['alarm_source'] = self.alarm_source
        df_action = find_previous_action(self.df_alarm, ddict)
        if df_action.empty:
            previous_action = "None"
        else:
            previous_action = df_action.iloc[-1].to_dict()['action']
        _log.warning(f"previous action: {previous_action}")
        if previous_action == "None":
            contact_pilot(ddict, fake=self.dummy_calls)

        if 'pilot' in previous_action:
            _log.warning(f"Will we escalate? {df_action.iloc[-1].to_dict()['datetime']} ")
            if df_action.iloc[-1].to_dict()['datetime'] < datetime.datetime.now() - datetime.timedelta(minutes=30):
                contact_supervisor(ddict, fake=self.dummy_calls)

    def execute(self):
        self.load_alarm_log()
        self.load_comm_log()
        if self.check_comm_log():
            self.trigger_alarm()
        _log.debug(f"{self.platform_id} check email")
        if self.mail_alarm():
            self.trigger_alarm()


if __name__ == '__main__':
    _log.info("******** START CHECK **********")
    try:
        parse_mail_alarms()
    except:
        _log.error("failed to process mail alarms")
    base_dir = Path(secrets_dict['base_data_dir'])
    all_glider_dirs = list(base_dir.glob("SEA*"))
    all_glider_dirs.sort()
    for glider_dir in all_glider_dirs:
        platform = glider_dir.parts[-1]
        glider_num = int(platform[3:])
        if glider_num in (57, 70):
            _log.debug(f"Skip Bastiens glider {platform}")
            continue
        dispatch = Dispatcher(platform)
        if secrets_dict["dummy_calls"] == "True":
            dispatch.dummy_calls = True
        dispatch.execute()
    fake = False
    if secrets_dict["dummy_calls"] == "True":
        fake = True
    surfacing_alerts(fake=fake)

    _log.info("******** COMPLETE CHECK *********")
