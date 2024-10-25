"""
Python script to alert the listener to alarm emails from SeaExplorer gliders
requires Python packages gtts and pydub
Requires ffmpeg to play audio on linux
Replace "al.mp3" with a path to an audio track of your choice for the alarm sound
Recommend running as a cron job at a regular interval
"""

import email
import imaplib
from gtts import gTTS
import json
from pydub import AudioSegment
from pydub.playback import play
from datetime import datetime
from pathlib import Path
import os
import sys
import logging

_log = logging.getLogger(__name__)
script_dir = Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)

with open("email_secrets.json") as json_file:
    secrets = json.load(json_file)


def sounds(text):
    _log.info(f"Will play {text}")
    play(AudioSegment.from_mp3("al.mp3"))
    _log.debug("played first sound")
    if "fw" in text.lower():
        text = text[4:]
    try:
        glider, mission, __, __, alarm_code = text.split(" ")
        message = f"sea {glider[4:-1]} has alarmed with code {alarm_code[6:-1]}. Get up"
    except IndexError:
        message = text
    speech = gTTS(text=message, lang="en", tld="com.au")
    speech.save("message.mp3")
    play(AudioSegment.from_mp3("message.mp3"))
    _log.debug("played full message")


def read_email_from_gmail():
    # check what time email was last checked
    timefile = Path("lastcheck.txt")
    if timefile.exists():
        with open(timefile, "r") as variable_file:
            for line in variable_file.readlines():
                last_check = datetime.fromisoformat((line.strip()))
    else:
        last_check = datetime(1970, 1, 1)
    # Write the time of this run
    with open("lastcheck.txt", "w") as f:
        f.write(str(datetime.now()))
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
                    local_date = datetime.fromtimestamp(
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
                if "Fw" in email_subject:
                    email_subject = email_subject[4:]
                email_from = msg["from"]
                # If email is from alseamar and subject contains ALARM, make some noise
                if (
                    "administrateur@alseamar-cloud.com" in email_from
                    or "calglider" in email_from
                    and "ALARM" in email_subject
                ):
                    _log.warning(f"alarm {email_subject}")
                    sounds(email_subject)


if __name__ == "__main__":
    logf = "email.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    read_email_from_gmail()
