import json
from flask import Flask, jsonify
from flask import request
import uuid
import time
import threading
from pprint import pprint as pp

app = Flask(__name__)
executed_jobs = {}
SHORT_TASK_DELAY_SECONDS = 15
LONG_TASK_DELAY_SECONDS = 60


def short_job(job_id):
    print(f"Short task has started and it will run for {SHORT_TASK_DELAY_SECONDS} seconds")
    time.sleep(SHORT_TASK_DELAY_SECONDS)
    executed_jobs['JobRunShort'] = {"code": 1000, "execution_id": job_id, "status": 'finished', 'result': 'success',
                                    'message': 'JobRunShort is completed.'}


def long_job(job_id):
    print(f"Long task has started and it will run for {LONG_TASK_DELAY_SECONDS} seconds")
    time.sleep(60)
    executed_jobs['JobRunLong'] = {"code": 1000, "execution_id": job_id, "status": 'finished', 'result': 'failure',
                                   'message': 'JobRunLong is completed.'}


def run_long_job():
    already_started = executed_jobs.get('JobRunLong', None)
    print(f'Has JobRunLong already started:  {already_started}')
    if already_started is None or executed_jobs['JobRunLong']['status'] == 'finished':
        job_id = uuid.uuid4().hex
        executed_jobs['JobRunLong'] = {"code": 1000, "execution_id": job_id, "status": 'in_progress', 'result': None,
                                       'message': f'JobRunLong execution({job_id}) has started'}
        print(f"Execution has started.ExecutionId :{executed_jobs['JobRunLong']['execution_id']}")
        thread = threading.Thread(target=long_job, args=[job_id])
        thread.start()
        return executed_jobs['JobRunLong']
    else:
        executed_jobs['JobRunLong']['message'] = 'JobRunLong is in progress !'
        executed_jobs['JobRunLong']['code'] = 2000
        return executed_jobs['JobRunLong']


def run_short_job():
    already_started = executed_jobs.get('JobRunShort', None)
    print(f'Has JobRunShort already started:  {already_started}')
    if already_started is None or executed_jobs['JobRunShort']['status'] == 'finished':
        job_id = uuid.uuid4().hex
        executed_jobs['JobRunShort'] = {"code": 1000, "execution_id": job_id, "status": 'in_progress', 'result': None,
                                        'message': f'JobRunShort execution({job_id}) has started'}
        print(f"Execution has started.ExecutionId :{executed_jobs['JobRunShort']['execution_id']}")
        thread = threading.Thread(target=short_job, args=[job_id])
        thread.start()
        return executed_jobs['JobRunShort']
    else:
        executed_jobs['JobRunShort']['message'] = 'JobRunShort is in progress!'
        executed_jobs['JobRunShort']['code'] = 2000

        return executed_jobs['JobRunShort']


switcher = {
    "JobRunLong": run_long_job,
    "JobRunShort": run_short_job
}


@app.route("/")
def home():
    return jsonify({})


@app.route("/jobs/<execution_id>", methods=['GET'])
def job_status(execution_id):
    if request.method == 'GET':
        print(f'Returning execution status for  {execution_id}')
        print('Printing all executed jobs..')
        pp(executed_jobs)
        job_executions = [(key, value) for key, value in executed_jobs.items() if value['execution_id'] == execution_id]
        job_execution = job_executions[0] if len(job_executions) > 0 else None

        if job_execution is None:
            result = {'code': 3000, "execution_id": execution_id, "status": None, "result": None,
                      "message": "Given Job could not be found!"}
            return jsonify(result)
        job_type = job_execution[0]
        job_execution_status = job_execution[1]
        print('job_execution_status')
        pp(job_execution_status)
        result = dict(job_execution_status, **{'JobType': job_type})
        return jsonify(result)


@app.route("/jobs", methods=['GET'])
def jobs():
    if request.method == 'GET':
        results = []
        print('Printing all executed jobs..')
        pp(executed_jobs)
        job_tuples = [(key, value) for key, value in executed_jobs.items()]

        for job_tuple in job_tuples:
            job_type = job_tuple[0]

            status = job_tuple[1]
            result = dict(status, **{'JobType': job_type})
            results.append(result)

        return jsonify(results)


@app.route("/jobs/<job>", methods=['POST'])
def start_job(job):
    if request.method == 'POST':
        func = switcher.get(job, lambda: f"Given Job ({job}) could not be found")
        result = func()
        return json.dumps(result)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=88, debug=True)
