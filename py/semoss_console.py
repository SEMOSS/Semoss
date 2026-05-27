from typing import Any, TYPE_CHECKING
import sys

if TYPE_CHECKING:
    from gaas_tcp_server_handler import TCPServerHandler


class SemossConsole(object):

    def __init__(self, socket_handler: "TCPServerHandler" = None):
        self.socket_handler = socket_handler

    def write(self, console_line):
        # also print to regular console in addition to socker handler
        # better debugging when running FORCE_PORT
        if console_line:
            print(console_line, file=sys.__stdout__)

        if self.socket_handler is not None and console_line and console_line.strip():
            self.socket_handler.send_output(console_line, response=False)

    def flush(self):
        pass

    def close(self):
        pass
