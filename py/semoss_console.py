class SemossConsole(object):
    def __init__(self, socket_handler=None, payload=None):
        self.socket_handler = socket_handler
        self.payload = payload
        self.output = []

    def write(self, console_line):
        if self.socket_handler is not None:
            self.socket_handler.send_output(console_line, self.payload, response=False)
        else:
            self.output.append(console_line)

    def set_payload(self, payload):
        self.payload = payload

    def reset_outputs(self):
        self.output = []

    def flush(self):
        pass

    def close(self):
        pass
