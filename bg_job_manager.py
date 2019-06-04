import json
from flask import Flask, jsonify, Response
from flask import request
import uuid
import time
import threading
from pprint import pprint as pp
from pendulum import datetime

app = Flask(__name__)
executed_jobs = {}
SHORT_TASK_DELAY_SECONDS = 10
LONG_TASK_DELAY_SECONDS = 20

lock = threading.Lock()


def medium_job():
    print(f"Short task has started and it will run for {SHORT_TASK_DELAY_SECONDS} seconds")
    time.sleep(SHORT_TASK_DELAY_SECONDS)
    executed_jobs['TestJobMedium']['IsFinished'] = True
    executed_jobs['TestJobMedium']['IsSuccess'] = True


def long_job():
    print(f"Long task has started and it will run for {LONG_TASK_DELAY_SECONDS} seconds")
    time.sleep(60)
    executed_jobs['TestJobLong']['IsFinished'] = True
    executed_jobs['TestJobLong']['IsSuccess'] = False


def run_long_job(job_name):
    already_started = executed_jobs.get(job_name, None)
    print(f'Has {job_name} already started:  {already_started}')
    if already_started is None or executed_jobs['TestJobLong']['IsFinished']:
        job_id = uuid.uuid4().hex
        executed_jobs['TestJobLong'] = {
            "IsFinished": False,
            "IsSuccess": False,
            "JobDefinition": "TestJobLong",
            "JobId": job_id,
            "JobType": "TestJob",
            "StartedAt": str(datetime.utcnow().isoformat())
        }

        print(f"Execution has started.ExecutionId :{executed_jobs['TestJobLong']['JobId']}")
        thread = threading.Thread(target=medium_job)
        thread.start()
        return executed_jobs['TestJobLong']
    else:
        raise Exception(json.dumps({'Message': 'OlympJob \'TestJobLong\' already executing!'}))


def run_medium_job(job_name):
    already_started = executed_jobs.get(job_name, None)
    print(f'Has {job_name} already started:  {already_started}')
    if already_started is None or executed_jobs['TestJobMedium']['IsFinished'] == True:
        job_id = uuid.uuid4().hex
        executed_jobs['TestJobMedium'] = {
            "IsFinished": False,
            "IsSuccess": False,
            "JobDefinition": "TestJobMedium",
            "JobId": job_id,
            "JobType": "TestJob",
            "StartedAt": str(datetime.utcnow().isoformat())
        }

        print(f"Execution has started.ExecutionId :{executed_jobs['TestJobMedium']['JobId']}")
        thread = threading.Thread(target=medium_job)
        thread.start()
        return executed_jobs['TestJobMedium']
    else:
        raise Exception(json.dumps({'Message': 'OlympJob \'TestJobMedium\' already executing!'}))


switcher = {
    "TestJobLong": run_long_job,
    "TestJobMedium": run_medium_job
}


@app.route("/")
def home():
    return jsonify({})


@app.route("/jobs/<job_id>", methods=['GET'])
def job_status(job_id):
    if request.method == 'GET':

        try:
            print(f'Returning execution status for  {job_id}')
            print('Printing all executed jobs..')
            pp(executed_jobs)
            job_executions = [(key, value) for key, value in executed_jobs.items() if value['JobId'] == job_id]
            job_execution = job_executions[0] if len(job_executions) > 0 else None

            if job_execution is None:
                result = {"Message": f"Could not find JobStatus for JobId '{job_id}'"}
                return jsonify(result)

            job_execution_status = job_execution[1]
            print('job_execution_status')
            pp(job_execution_status)
            result = job_execution_status
            return jsonify(result)

        except Exception as ex:
            raise Exception(json.dumps({'Message': str(ex)}))


@app.route("/jobs", methods=['GET'])
def jobs():
    if request.method == 'GET':
        print('Printing all executed jobs..')
        pp(executed_jobs)

        active_jobs = [(key, value) for key, value in executed_jobs.items() if not value['IsFinished']]
        result = []
        for job in active_jobs:
            result.append(job[1])

        return jsonify(result)


def handle_undefined_job_request(job_name):
    return Response(
        f'{"Message": "Error executing OlympJob \'{job_name}\'! Msg: OlympConfigManager-Error: JobDefinition \'{job_name}\' not found!"}',
        status=400, mimetype='application/json')


@app.route("/jobs/<job>", methods=['POST'])
def start_job(job):
    if request.method == 'POST':

        try:
            func = switcher.get(job, lambda job_name: handle_undefined_job_request(job_name))
            result = func(job)
            return json.dumps(result)
        except Exception as ex:
            return Response(str(ex), status=400, mimetype='application/json')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=88, debug=True)
