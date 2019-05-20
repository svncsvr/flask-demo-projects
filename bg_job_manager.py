from flask import Flask, jsonify
from flask import request
import uuid
import time
import threading
from pprint import pprint as pp

app = Flask(__name__)

executed_jobs = {}


def short_job(job_id):
    time.sleep(3)
    executed_jobs['JobRunShort'] = {"id": job_id, "status": 'finished', 'result': 'succeeded'}


def long_job(job_id):
    time.sleep(20)
    executed_jobs['JobRunLong'] = {"id": job_id, "status": 'finished', 'result': 'succeeded'}


def run_long_job():
    already_started = executed_jobs.get('JobRunLong', None)
    pp(already_started)
    if already_started is None or executed_jobs['JobRunLong']['status'] == 'finished':
        job_id = uuid.uuid4().hex
        executed_jobs['JobRunLong'] = {"id": job_id, "status": 'in_progress', 'result': None}
        print(f"ID ===== {executed_jobs['JobRunLong']['id']}")
        thread = threading.Thread(target=long_job, args=[job_id])
        thread.start()
        return f"JobRunLong has started, {executed_jobs['JobRunLong']}"
    else:
        return f"JobRunLong has still in progress , {executed_jobs['JobRunLong']}"


def run_short_job():
    already_started = executed_jobs.get('JobRunShort', None)
    pp(already_started)
    if already_started is None or executed_jobs['JobRunLong']['status'] == 'finished':
        job_id = uuid.uuid4().hex
        executed_jobs['JobRunShort'] = {"id": job_id, "status": 'in_progress', 'result': None}
        thread = threading.Thread(target=short_job, args=[job_id])
        thread.start()
        return f"JobRunShort has started, {executed_jobs['JobRunShort']}"
    else:
        return f"JobRunShort has still in progress , {executed_jobs['JobRunShort']}"


switcher = {
    "JobRunLong": run_long_job,
    "JobRunShort": run_short_job
}


@app.route("/")
def home():
    return jsonify({})


@app.route("/job/<execution_id>", methods=['GET'])
def job_status(execution_id):
    if request.method == 'GET':
        result = {}
        print('Printing running job_tuples..')
        pp(executed_jobs)
        job_tuples = [(key, value) for key, value in executed_jobs.items() if value['id'] == execution_id]
        job_tuple = job_tuples[0] if len(job_tuples) > 0 else None

        if job_tuple is None:
            return jsonify(result)
        job_type = job_tuple[0]
        status = job_tuple[1]
        result = dict(status, **{'JobType': job_type})
        return jsonify(result)


@app.route("/job/<job>", methods=['POST'])
def start_job(job):
    if request.method == 'POST':
        func = switcher.get(job, lambda: f"Given Job ({job}) could not be found")
        result = func()
        return f"{result}"


if __name__ == "__main__":
    app.run(debug=True)
