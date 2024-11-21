"""
Python script to alert the listener to alarm emails from SeaExplorer gliders
requires Python packages gtts and pydub
Requires ffmpeg to play audio on linux
Replace "al.mp3" with a path to an audio track of your choice for the alarm sound
Recommend running as a cron job at a regular interval
"""
from gtts import gTTS
import json
from pydub import AudioSegment
from pydub.playback import play
from pathlib import Path
import os
import sys
import logging
from votoutils.alerts.read_mail import read_email_from_gmail

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


if __name__ == "__main__":
    logf = "email.log"
    logging.basicConfig(
        filename=logf,
        filemode="a",
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    read_email_from_gmail(sounds)
