import pickle
import time
import random

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
    """
    Implementation of job distributor
    """
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
        CONN.flushall()
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
    def get_worker_node():
        if config.ALGORITHM == "LRU":
            return Distributor.get_lru_node()
        elif config.ALGORITHM == "RANDOM":
            return Distributor.get_random_node()
        elif config.ALGORITHM == "MODULO":
            return Distributor.get_modulo_node()
        else:
            return Distributor.get_lru_node()
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
            print(available_nodes[0])
            return available_nodes[0]

        return None

    @staticmethod
    def get_random_node():
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
            print(available_nodes[0])
            return available_nodes[random.randint(0, len(available_nodes) - 1)]

        return None

    @staticmethod
    def get_modulo_node():
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

        if (len(all_nodes) <= len(busy_nodes)) \
                and (len(available_nodes) == 0):
            return None
        elif len(available_nodes) == 0 or cpu_util > 50:
            # up scale
            print("Increasing the number of workers")
            return Distributor.scale_nodes(busy_nodes, operation="CREATE")
        elif cpu_util < 50 and len(available_nodes) > config.DEFAULT_NODES:
            # down scale
            print("Decreasing the number of workers")
            return Distributor.scale_nodes(available_nodes, operation="CLOSE")
            # return available_nodes[0]
        else:
            print(available_nodes[0])
            n = len(available_nodes) - 1
            if n != 0:
                return available_nodes[int(time.time() * 1000) % (len(available_nodes) - 1)]
            else:
                return available_nodes[0]

        return None

    @staticmethod
    def scale_nodes(busy_nodes: [Node], operation: str):
        """
        Scale nodes according to cpu usage.
        :return: None
        """
        if operation == "CREATE":
            port = config.NODE_PORTS[0]
            print([x.host[-4:] for x in busy_nodes if x.host[-4:] != 'None'])
            busy_ports = [int(x.host[-4:]) for x in busy_nodes]
            print(busy_ports)
            for p in config.NODE_PORTS:
                if p in busy_nodes:
                    continue
                else:
                    port = p
            print("&&&&&&&&port=", port)
            spawner.spawn_worker(port)
            node = Node(host="localhost:" + str(port), status=0, last_executed=time.time() * 1000)
            CONN.hmset(name="nodes", mapping={
                "localhost:" + str(node): pickle.dumps(node) for node in [port]
            })
            return node
        elif operation == "CLOSE":
            spawner.close_workers([int(busy_nodes[-1].host[-4:])])
            return busy_nodes[0]

    @staticmethod
    def get_cpu_util(nodes: [Node]):
        cpu_utilization = 0
        for node in nodes:
            response = requests.get("http://" + node.host + "/status")
            cpu_utilization += int(response.json()["cpuUsage"])
        if len(nodes) != 0:
            cpu_utilization = cpu_utilization / len(nodes)
        return cpu_utilization

    @staticmethod
    def update_node_timestamp(nodes: [Node], status=1):
        """
        Updates the timestamp of the given nodes to current time
        :return: None
        """
        lock = redis_lock.Lock(CONN, "write-lock")
        if lock.acquire(blocking=False):
            CONN.hmset(name="nodes", mapping={
                node.host: pickle.dumps(
                    Node(host=node.host, status=status, last_executed=time.time() * 1000))
                for node in nodes})
            [print('Updated timestamp of node: {}'.format(node.host)) for node in nodes]
        else:
            print("Someone else has the lock.")
        lock.release()

    @staticmethod
    def update_node_status(nodes, status=0):
        """
        Updates the status of the given nodes
        :return: None
        """
        lock = redis_lock.Lock(CONN, "write-lock")
        if lock.acquire(blocking=False):
            CONN.hmset(name="nodes", mapping={
                "localhost:" + node: pickle.dumps(
                    Node(host="localhost:" + node, status=0, last_executed=time.time() * 1000))
                for node in nodes})
            [print('Updated status of node: {}'.format(node)) for node in nodes]
            lock.release()
        else:
            print("Someone else has the lock.")
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
