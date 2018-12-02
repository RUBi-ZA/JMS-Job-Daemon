import redis, json, pickle

from src.config import config
from src.schedulers import schedulers
from src.helpers.data_structures import JobMap

from impersonator.client import Impersonator

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
        job_map = pickle.loads(cache.get("jobs"))
        scheduler = _get_scheduler(config, None)
        
        queue = scheduler.transform_job_list_to_queue(job_map.jobs())

        return app.response_class(
            response=queue.to_JSON(),
            status=200,
            mimetype="application/json"
        )


@app.route("/jobs/<job_id>", methods=["GET"])
def job(job_id):
    job_map = pickle.loads(cache.get("jobs"))

    try:
        job = job_map[job_id]
    except Exception:
        job = None

    if job is not None:
        response = job.to_JSON()
        status = 200
    else:
        response = json.dumps({"error": "Job not found"})
        status = 404
    
    return app.response_class(
        response=response,
        status=status,
        mimetype="application/json"
    )


def _run_job():
    return jsonify({"message": "Not Implemented"})


def _get_scheduler(config, token):
    impersonator = Impersonator(config["impersonator"]["host"], config["impersonator"]["port"], token)

    scheduler_name = config["scheduler"]["name"]
    Scheduler = schedulers[scheduler_name]["scheduler"]

    return Scheduler(config, impersonator)