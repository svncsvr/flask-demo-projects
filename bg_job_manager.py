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
    executed_jobs['JobRunShort'] = {"Code": 1000, "JobId": job_id, "Status": 'finished', 'Result': 'success',
                                    'Message': 'JobRunShort is completed.'}


def long_job(job_id):
    print(f"Long task has started and it will run for {LONG_TASK_DELAY_SECONDS} seconds")
    time.sleep(60)
    executed_jobs['JobRunLong'] = {"Code": 1000, "JobId": job_id, "Status": 'finished', 'Result': 'failure',
                                   'Message': 'JobRunLong is completed.'}


def run_long_job(job_name):
    already_started = executed_jobs.get(job_name, None)
    print(f'Has {job_name} already started:  {already_started}')
    if already_started is None or executed_jobs['JobRunLong']['Status'] == 'finished':
        job_id = uuid.uuid4().hex
        executed_jobs['JobRunLong'] = {"Code": 1000, "JobId": job_id, "Status": 'in_progress', 'Result': None,
                                       'Message': f'JobRunLong execution({job_id}) has started'}
        print(f"Execution has started.ExecutionId :{executed_jobs['JobRunLong']['JobId']}")
        thread = threading.Thread(target=long_job, args=[job_id])
        thread.start()
        return executed_jobs['JobRunLong']
    else:
        executed_jobs['JobRunLong']['Message'] = 'JobRunLong is in progress !'
        executed_jobs['JobRunLong']['Code'] = 2000
        return executed_jobs['JobRunLong']


def run_short_job(job_name):
    already_started = executed_jobs.get(job_name, None)
    print(f'Has {job_name} already started:  {already_started}')
    if already_started is None or executed_jobs['JobRunShort']['Status'] == 'finished':
        job_id = uuid.uuid4().hex
        executed_jobs['JobRunShort'] = {"Code": 1000, "JobId": job_id, "Status": 'in_progress', 'Result': None,
                                        'Message': f'JobRunShort execution({job_id}) has started'}
        print(f"Execution has started.ExecutionId :{executed_jobs['JobRunShort']['JobId']}")
        thread = threading.Thread(target=short_job, args=[job_id])
        thread.start()
        return executed_jobs['JobRunShort']
    else:
        executed_jobs['JobRunShort']['Message'] = 'JobRunShort is in progress!'
        executed_jobs['JobRunShort']['Code'] = 2000

        return executed_jobs['JobRunShort']


switcher = {
    "JobRunLong": run_long_job,
    "JobRunShort": run_short_job
}


@app.route("/")
def home():
    return jsonify({})


@app.route("/jobs/<job_id>", methods=['GET'])
def job_status(job_id):
    if request.method == 'GET':
        print(f'Returning execution status for  {job_id}')
        print('Printing all executed jobs..')
        pp(executed_jobs)
        job_executions = [(key, value) for key, value in executed_jobs.items() if value['JobId'] == job_id]
        job_execution = job_executions[0] if len(job_executions) > 0 else None

        if job_execution is None:
            result = {'Code': 3000, "JobId": job_id, "Status": None, "Result": None,
                      "Message": "Given Job execution could not be found!"}
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


def handle_undefined_job_request(job_name):
    return {'Code': 3000, "JobId": None, "Status": None, "Result": None,
            "Message": f'Given job definition({job_name}) could not be found.'}


@app.route("/jobs/<job>", methods=['POST'])
def start_job(job):
    if request.method == 'POST':
        func = switcher.get(job, lambda job_name: handle_undefined_job_request(job_name))
        result = func(job)
        return json.dumps(result)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=88, debug=True)
