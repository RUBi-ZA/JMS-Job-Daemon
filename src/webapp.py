import redis, json, pickle

from src.config import config
from src.schedulers import schedulers
from src.helpers.data_structures import JobMap
from src.helpers.context_managers import SchedulerTransaction

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
        token = request.args.get("token")
        scheduler = _get_scheduler(config, token)

        job_params = request.get_json()

        return _run_job(scheduler, **job_params)
    else:
        job_map = pickle.loads(cache.get("jobs"))
        scheduler = _get_scheduler(config, None)
        
        queue = scheduler.transform_job_list_to_queue(job_map.jobs())

        return app.response_class(
            response=queue.to_JSON(),
            status=200,
            mimetype="application/json"
        )


@app.route("/jobs/<job_id>", methods=["GET", "DELETE"])
def job(job_id):
    if request.method == "GET":
        job_map = pickle.loads(cache.get("jobs"))

        try:
            job = job_map[job_id]
            response = job.to_JSON()
            status = 200
        except Exception:
            response = json.dumps({"error": "Job not found"})
            status = 404
    else:
        token = request.args.get("token")
        scheduler = _get_scheduler(config, token)

        scheduler.kill_job(job_id)
        response = json.dumps({"message": "Kill request submitted. Job should be killed within 30s."})
        status = 201
    
    return app.response_class(
        response=response,
        status=status,
        mimetype="application/json"
    )


@app.route("/scheduler/server", methods=["GET", "PATCH"])
def scheduler_server():
    token = request.args.get("token")
    scheduler = _get_scheduler(config, token)
        
    server_config = scheduler.get_server_config()

    return app.response_class(
        response=server_config.to_JSON(),
        status=200,
        mimetype="application/json"
    )


@app.route("/scheduler/queues", methods=["GET"])
def scheduler_queues():
    token = request.args.get("token")
    scheduler = _get_scheduler(config, token)
        
    queues = scheduler.get_queues()

    return app.response_class(
        response=queues.to_JSON(),
        status=200,
        mimetype="application/json"
    )


@app.route("/scheduler/nodes", methods=["GET"])
def scheduler_nodes():
    token = request.args.get("token")
    scheduler = _get_scheduler(config, token)
        
    nodes = scheduler.get_nodes()

    return app.response_class(
        response=json.dumps(nodes, default=lambda o: o._try(o)),
        status=200,
        mimetype="application/json"
    )


@app.route("/scheduler/nodes/<node_name>", methods=["POST", "PUT", "DELETE"])
def scheduler_node(node_name):
    token = request.args.get("token")
    scheduler = _get_scheduler(config, token)
    
    if request.method == "DELETE":
        nodes = scheduler.delete_node(node_name)
    else:
        node = request.get_json()
        node['name'] = node_name
        
        if request.method == "POST":
            nodes = scheduler.add_node(node)
        else:
            nodes = scheduler.update_node(node)

    return app.response_class(
        response=json.dumps(nodes, default=lambda o: o._try(o)),
        status=200,
        mimetype="application/json"
    )


def _run_job(scheduler, job_name, job_dir, script_name, output_log, error_log, settings, hold, commands):
    with SchedulerTransaction(scheduler, scheduler.impersonator.token):
        script = scheduler.create_job_script(
            job_name, 
            job_dir, 
            script_name, 
            output_log, 
            error_log, 
            settings, 
            hold, 
            commands
        )
        job_id = scheduler.execute_job_script(script)
    return jsonify({"job_id": job_id})


def _get_scheduler(config, token):
    impersonator = Impersonator(config["impersonator"]["host"], config["impersonator"]["port"])

    scheduler_name = config["scheduler"]["name"]
    Scheduler = schedulers[scheduler_name]["scheduler"]

    scheduler = Scheduler(config, impersonator)
    scheduler.set_credentials(token)

    return scheduler