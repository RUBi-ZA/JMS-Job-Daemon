from .config import config
from .schedulers.base import BaseScheduler

from flask import Flask, request

app = Flask(__name__)
jobs = {}

@app.route("/status", methods=["GET"])
def status():
    scheduler = BaseScheduler(config, request.args.get("token"))
    return scheduler.get_status()