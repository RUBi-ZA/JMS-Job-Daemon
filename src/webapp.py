import redis

from src.config import config
from src.helpers.data_structures import JobQueue, SchedulerStatus

from flask import Flask, request, jsonify

app = Flask(__name__)
cache = redis.StrictRedis(host="localhost", port=6379, db=0)


@app.route("/status", methods=["GET"])
def status():
    return app.response_class(
        response=cache.get("status"),
        status=200,
        mimetype="application/json"
    )


@app.route("/jobs", methods=["GET", "POST"])
def jobs():
    if request.method == "POST":
        return _run_job()
    else:
        return app.response_class(
            response=cache.get("jobs"),
            status=200,
            mimetype="application/json"
        )


def _run_job():
    return jsonify({"message": "Not Implemented"})
