import json


class WorkStatus:
    status: str
    output: str
    file_name: str
    args: []
    env: {}

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

    def __init__(self, status: str,
                 output: str,
                 file_name: str,
                 args: [],
                 env: {}) -> None:
        self.status = status
        self.output = output
        self.file_name = file_name
        self.args = args
        self.env = env


class GeneralRes:
    status_code: int
    message: str
    output: str
    error: str

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

    def __init__(self, status_code: int, message: str, output: str, error: str) -> None:
        self.status_code = status_code
        self.message = message
        self.output = output
        self.error = error
