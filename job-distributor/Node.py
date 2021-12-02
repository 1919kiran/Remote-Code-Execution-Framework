class Node:
    def __init__(self, host=None, port=None, status=None, last_executed=None):
        self.host = host
        self.status = status
        self.last_executed = last_executed

    def __str__(self):
        return '(host={}, status={}, last_executed={})'. \
            format(self.host, self.status, self.last_executed)