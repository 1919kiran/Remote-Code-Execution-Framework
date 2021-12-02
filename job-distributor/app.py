from flask import Flask, jsonify, request, redirect
from waitress import serve
from distributor import Distributor

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/execute", methods=['POST', 'GET'])
def execute_task():
    node = Distributor.get_worker_node()
    if node is None:
        return jsonify('Limit reached')
    redirect_url = "http://{}/execute".format(node.host)
    Distributor.update_node_timestamp([node], status=1)
    print('Redirecting to host: ', redirect_url)
    return redirect(location=redirect_url, code=307)


@app.route("/execute/update_node", methods=['GET'])
def update_node():
    node = request.args.get('node')
    Distributor.update_node_status([node])
    return jsonify()


@app.route("/stats", methods=['GET'])
def get_stats():
    r = Distributor.get_stats()
    return r


if __name__ == 'app':
    Distributor.init_workers()
    serve(app, host='0.0.0.0', port=8081)
