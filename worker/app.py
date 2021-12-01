import os
import subprocess
import sys
import uuid

import psutil as psutil
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename

from model import *

app = Flask(__name__)

app.config['MAX_CONTENT_PATH'] = 16 * 1024 * 1024  # bytes
app.config['UPLOAD_FOLDER'] = './worker/files'
ALLOWED_EXTENSIONS = {'py'}
DEFAULT_TIMEOUT = 5
JOB_COUNT = 0
LOAD_BALANCER_URL = 'localhost:8082'


@app.route('/status', methods=['GET'], endpoint='status')
def status():
    """
    Provide current CPU utilization.
    :return: int (CPU Usage)
    """
    return jsonify({"cpuUsage": psutil.cpu_percent(), "jobCount": JOB_COUNT}, 200)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/execute', methods=['POST'])
def execute_file():
    """
    Executes given python file with specified args, env and timeout.
    :return: GeneralRes
    """
    # Validate json request
    response = GeneralRes(400, "", None, None)
    # Validate file that needs to be executed
    if 'file' not in request.files:
        response.message = 'No file part in the request'
    file = request.files['file']
    if file.filename == '':
        response.message = 'No file selected for execution in the request'
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filename = app.config['UPLOAD_FOLDER'] + "/" + str(uuid.uuid4()) + "_" + filename
        file.save(os.path.join(filename))
        args = get_args(request.form.get('args'))
        env = get_env(request.form.get('env'))
        timeout = get_timeout(request.form.get('timeout'))
        response = execute(filename, args, env, timeout)
        os.remove(os.path.join(filename))
        global JOB_COUNT
        JOB_COUNT += 1
    else:
        response.message = 'Allowed file type is py'
    # Notify load balancer about work completion
    # requests.get(LOAD_BALANCER_URL + '/execute/update_node/<>')
    return response.to_json()


def execute(filename, args, env, timeout):
    env = dict(os.environ, **env)
    try:
        output = subprocess.check_output(["python", os.path.join(filename), args], timeout=timeout, env=env,
                                         universal_newlines=True, stderr=subprocess.STDOUT)
        error = ''
    except subprocess.CalledProcessError as e:
        output = e.output.split('Traceback (most recent call last):')[0]
        error = 'Traceback (most recent call last):' + '\n'.join(
            e.output.split('Traceback (most recent call last):')[1:])
    if error != '':
        return GeneralRes(200, "Code executed with some error", output, error)
    else:
        return GeneralRes(200, "Code executed successfully", output, None)


def get_args(args: str):
    if args is not None:
        return " ".join(args.split(","))
    else:
        return ""


def get_env(env: {}):
    if env is not None:
        return json.loads(env)
    else:
        return {}


def get_timeout(timeout: int):
    if timeout is not None:
        return timeout
    else:
        return DEFAULT_TIMEOUT


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(sys.argv[1]))
