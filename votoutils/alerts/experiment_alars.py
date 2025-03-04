import json
from pathlib import Path
import requests


script_dir = Path(__file__).parent.parent.parent.absolute()

with open(script_dir / "alarm_secrets.json", "r") as secrets_file:
    secrets_dict = json.load(secrets_file)

with open(script_dir / "contacts_secrets.json", "r") as secrets_file:
    contacts = json.load(secrets_file)


def elks_text(name, recipient, fake=True):
    if fake:
        response = requests.post(
            "https://api.46elks.com/a1/sms",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": "VOTOalert",
                "to": recipient,
                "message": f"Hi {name.title()}! This is a test message for the VOTO alerts system",
                "dryrun": "yes",
            },
        )
    else:
        response = requests.post(
            "https://api.46elks.com/a1/sms",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": "VOTOalert",
                "to": recipient,
                "message": f"Hi {name.title()}! This is a test message for the VOTO alerts system",
            },
        )
    print(f"ELKS SEND: {response.text}")


def elks_call(name, recipient, fake=True, timeout_seconds=30):
    if fake:
        print(f"called it {name}")
    else:
        response = requests.post(
            "https://api.46elks.com/a1/calls",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": secrets_dict["elks_phone"],
                "to": recipient,
                "voice_start": '{"play":"https://callumrollo.com/files/frederik_short.mp3"}',
                "timeout": timeout_seconds,
            },
        )
        print(f"ELKS SEND: {response.text}")
