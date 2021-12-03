from subprocess import Popen

from flask import Flask
from psutil import process_iter
from signal import SIGTERM  # or SIGKILL
import config

app = Flask(__name__)


def spawn_worker(port):
    try:
        print("Spawning worker on port: ", port)
        p = Popen(['python worker/app.py ' + str(port)],
                  shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)
        print("****", p.pid)
    except Exception as e:
        print("***", e)
    return None


def close_workers(ports):
    for proc in process_iter():
        for conns in proc.connections(kind='inet'):
            if conns.laddr[1] in ports:
                proc.send_signal(SIGTERM)  # or SIGKILL
                print("Closing worker on port: ", conns.laddr[1])
    return None


def spawn_base_workers():
    try:
        close_workers(config.NODE_PORTS)
        for i in range(config.DEFAULT_NODES):
            Popen(['python ../worker/app.py ' + str(config.NODE_PORTS[i])],
                  shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)

    except Exception as e:
        print(e)

    return None


def main():
    close_workers(config.NODE_PORTS)
    spawn_base_workers()


if __name__ == "__main__":
    main()
