from flask import render_template
import os

from flask import Flask, request, redirect, jsonify
from werkzeug.utils import secure_filename
import requests
import config

ALLOWED_EXTENSIONS = set(['py'])
LOAD_BALANCER_URL = "http://10.0.0.14:8081/execute"


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)


@app.route('/', methods=["GET", "POST"])
def Eval():
    return render_template('eval_data_1.html')


@app.route('/stats')
def stats():
    response = requests.get("http://localhost:8081/stats")
    return render_template('stats.html', data=response.json().items(), algo=config.ALGORITHM)


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        try:
            print(request.form.get('args'))
            print(request.form.get('timeout'))
            print(request.form.get('env'))
            f = request.files['file']
            f.save(secure_filename(f.filename))
            payload = {
                'args': request.form.get('args'),
                'timeout': int(request.form.get('timeout'))
            }
            file = os.getcwd() + "/" + f.filename
            files = [
                ('file', ('test.py', open(file, 'rb'), 'application/octet-stream'))
            ]
            headers = {}
            response = requests.request("POST", LOAD_BALANCER_URL, headers=headers, data=payload, files=files)
            return jsonify(response.json())
        except Exception as e:
            return jsonify(response.content)


if __name__ == "__main__":
    app.run(debug=True, port="7000")
