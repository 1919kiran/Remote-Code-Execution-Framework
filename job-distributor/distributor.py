import pickle
import time
import traceback

import redis
import redis_lock
import requests

import config
# import spawner
from Node import Node
from Singleton import Singleton
import spawner

CONN = redis.Redis(host=config.REDIS_SERVER_HOSTNAME, port=config.REDIS_SERVER_PORT)


class Distributor(metaclass=Singleton):

    def __init__(self):
        self.pool = redis.ConnectionPool(host=config.REDIS_SERVER_HOSTNAME, port=config.REDIS_SERVER_PORT)
        self.conn = redis.Redis(connection_pool=self.pool, charset="utf-8", decode_responses=True)

    @staticmethod
    def init_workers():
        """
        Initializes worker nodes in the distributed cache
        :return: None
        """
        # spawner.spawn_base_workers()
        lock = redis_lock.Lock(CONN, "write-lock")
        try:
            if lock.acquire(blocking=False):
                CONN.hmset(name="nodes", mapping={
                    "localhost:" + str(config.NODE_PORTS[i]): pickle.dumps(
                        Node(host="localhost:" + str(config.NODE_PORTS[i]), status=0, last_executed=time.time() * 1000))
                    for i in range(config.DEFAULT_NODES)})
                print('Initialized {} worker nodes'.format(config.DEFAULT_NODES))
            else:
                print("Someone else has the lock.")
            lock.release()
        except Exception as e:
            print(e)
            lock.release()
        finally:
            if lock.locked():
                lock.release()
        return None

    @staticmethod
    def get_lru_node():
        """
        Returns the least recently used node in the node cluster
        :rtype: Node
        """
        lock = redis_lock.Lock(CONN, "write-lock")
        CONN.hgetall(name="nodes")
        nodes = CONN.hgetall(name="nodes").values()
        available_nodes = sorted([pickle.loads(n) for n in nodes if pickle.loads(n).status == 0],
                                 key=lambda x: x.last_executed, reverse=False)
        busy_nodes = sorted([pickle.loads(n) for n in nodes if pickle.loads(n).status == 1],
                            key=lambda x: x.last_executed, reverse=False)
        all_nodes = config.NODE_PORTS
        cpu_util = Distributor.get_cpu_util(available_nodes)

        print("*****")
        print("all_nodes", len(all_nodes))
        print("available_nodes", len(available_nodes))
        print("busy_nodes", len(busy_nodes))
        print("NODE_PORTS", len(config.NODE_PORTS))
        print("cpu_util", cpu_util)
        print("DEFAULT_NODES", config.DEFAULT_NODES)

        if (len(all_nodes) <= len(busy_nodes)) \
                and (len(available_nodes) == 0):
            return None
        elif len(available_nodes) == 0 or cpu_util > 50:
            # up scale
            print("Increasing the number of workers")
            return Distributor.scale_nodes(all_nodes, busy_nodes, operation="CREATE")
        elif cpu_util < 50 and len(available_nodes) > config.DEFAULT_NODES:
            # down scale
            print("Decreasing the number of workers")
            return Distributor.scale_nodes(all_nodes, available_nodes, operation="CLOSE")
            # return available_nodes[0]
        else:
            return available_nodes[0]

        return None

 

    @staticmethod
    def get_stats():
        nodes = CONN.hgetall(name="nodes").values()
        available_nodes = sorted([pickle.loads(n) for n in nodes],
                                 key=lambda x: x.last_executed, reverse=False)

        r = {}
        for node in available_nodes:
            response = requests.get("http://" + node.host + "/status")
            r[node.host] = response.json()
        return r
