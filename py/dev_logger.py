import os


class DevLogger:
    '''
    Log messages to the log file in this directory when working on the local server.
    This determines the local server when you pass the server URL to the constructor.
    If you provide an alternate log path, it will use that instead. This should be an absolute path to your txt file.
    '''

    def __init__(self, url=None, log_path="C:/workspace/Semoss/py/log.txt", alt_log_path=None, override_url=False):
        self.log_file = None
        self.log_path = log_path if alt_log_path is None else alt_log_path
        self.url = url
        self.override_url = override_url
        self.setup_logger()

    def setup_logger(self):
        if self.url == 'http://localhost:9090/Monolith/api' or self.override_url is True:
            log_dir = os.path.dirname(self.log_path)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            try:
                self.log_file = open(self.log_path, "a", encoding="utf-8")
                print("Log file opened successfully.")
            except IOError as e:
                print(f"Error opening log file {self.log_path}: {e}")
                self.log_file = None
        else:
            pass

    def log(self, message):
        if self.log_file is not None:
            self.log_file.write("\n")
            self.log_file.write(str(message))
            self.log_file.flush()
