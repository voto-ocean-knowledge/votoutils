import email
import imaplib
from datetime import datetime
from pathlib import Path
import logging
import json

_log = logging.getLogger(__name__)
script_dir = Path(__file__).parent.parent.parent.absolute()

with open(script_dir / "email_secrets.json") as json_file:
    secrets = json.load(json_file)


def read_email_from_gmail(call_func):
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
                    call_func(email_subject)
