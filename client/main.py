from flask import Flask, request, render_template
import random
import os
import urllib.request
from app import app
from flask import Flask, request, redirect, jsonify
from werkzeug.utils import secure_filename
import requests

ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)


@app.route('/')
def Eval():
    return render_template('eval_data_1.html')


@app.route('/stats')
def stats():
    response = requests.get("http://localhost:8081/stats")
    return jsonify(response.json())


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        print(request.files)
        f = request.files['file']
        f.save(secure_filename(f.filename))
        with open(f.filename, 'rb') as f:
            r = requests.post("http://172.20.20.20:8081/execute", files={'file': f})
        if r.status_code == 200:
            res_payload_dict = r.json()
            print(type(res_payload_dict))
        return jsonify(res_payload_dict)


if __name__ == "__main__":
    app.run(debug=True, port="7000")
