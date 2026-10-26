import argparse
import logging
import sys
import socket
import socketserver
import threading
import asyncio
import os

# Must be imported before anything that can open a TLS connection. Importing it
# leaves a single truststore owning ssl.SSLContext, without which the openai and
# anthropic clients drive ssl.SSLContext.verify_mode into unbounded recursion on
# every handshake. See smss_system_certs for the full explanation.
import smss_system_certs

# Must be imported before anything that can pull in matplotlib. Importing it
# pins the headless Agg backend, without which matplotlib autoselects a GUI
# backend and aborts the process the moment user code plots from a worker
# thread. See smss_inline_display for the full explanation.
import smss_inline_display

from gaas_tcp_server_handler import TCPServerHandler

# logging.basicConfig(level=logging.DEBUG,
#                    format='%(name)s: %(message)s',
#                    )

# this thread will stop after 15 min of wake time if no other sockets are there


class Server(socketserver.ThreadingTCPServer):

    def __init__(
        self,
        server_address=None,
        handler_class=TCPServerHandler,
        port=81,
        max_count=1,
        py_folder=".",
        insight_folder=".",
        prefix="",
        timeout=15,
        start=True,
        blocking=False,
        logger_level: str = "INFO",
        uds_path=None,
        engine_owned=False,
    ):
        self.logger = logging.getLogger("SocketServer")
        self.logger.debug("__init__")
        self.stop = False
        self.port = port
        self.uds_path = uds_path
        self.max_count = max_count
        self.cur_count = 0
        self.user_mode = self.max_count == 1
        self.insight_folder = insight_folder
        self.prefix = prefix
        # engine owned processes run platform code, not a user's python code
        self.engine_owned = engine_owned

        self.monitor = threading.Condition()
        self.timed_out = False
        self.blocking = blocking

        # see if the port was passed through argv
        if self.port is None and len(sys.argv) > 0:
            self.port = sys.argv[0]

        if self.port is None and len(sys.argv) > 1:
            self.start = sys.argv[1] == 1

        # set the current folder to pick up scripts from
        sys.path.append(py_folder)

        if self.uds_path:
            self.address_family = socket.AF_UNIX
            self.server_address = self.uds_path
            try:
                if os.path.exists(self.uds_path):
                    os.unlink(self.uds_path)
            except OSError:
                pass
        else:
            self.server_address = ("localhost", self.port)
        socketserver.ThreadingTCPServer.__init__(
            self, self.server_address, handler_class
        )
        # Set up a TCP/IP server
        self.logger.info("Ready to start server")
        if timeout > 0:
            timeout = timeout * 60
            print(f"Setting timeout to .. {timeout}")
            self.timeout = timeout
            # self.socket.settimeout(timeout*60)
        else:
            print("Setting timeout to None")
            self.socket.settimeout(None)

        # The timeout_val is inherited from the parent and needs to be set
        # This value (in seconds) is used by the TCPServerHandler to set the timeout on the client connection socket
        self.timeout_val = timeout

        if start:
            self.serve_forever()

    def handle_timeout(self):
        # no clients.. kill this server, no point keeping it
        # give back the GPU
        self.timed_out = True
        if self.cur_count == 0:
            self.logger.info(
                f"Server idle for {self.timeout / 60} minutes. No client connected. Shutting down."
            )
            self.stop_it()

    def server_activate(self):
        self.logger.debug("server_activate")
        socketserver.TCPServer.server_activate(self)
        return

    def serve_forever(self):
        self.logger.info(f"waiting for request on port {self.port}")
        self.logger.info("Handling requests, press <Ctrl-C> to quit")
        try:
            while not self.stop:
                # guard the capacity check with the same condition lock we use when notify is called in remove_handler
                with self.monitor:
                    while not self.stop and self.cur_count >= self.max_count:
                        print("Max connections reached. Waiting for a slot to be free.")
                        self.monitor.wait()

                    if self.stop:
                        break

                print("Listening on port " + str(self.port))
                self.handle_request()

                # also keep count updates synchronized with remove_handler
                with self.monitor:
                    self.timed_out = False
                    self.cur_count += 1
        except Exception as e:
            self.logger.error(f"Error: {e}", exc_info=True)
            self.stop_it()
        return

    def remove_handler(self):
        with self.monitor:
            self.cur_count = self.cur_count - 1
            self.monitor.notify()

    def stop_it(self):
        print(
            f"Max connections = {self.max_count}, Current connections = {self.cur_count}"
        )
        if self.user_mode:
            print("Closing server")
            self.stop = True
            socketserver.TCPServer.server_close(self)


def parse_args():
    parser = argparse.ArgumentParser(description="Server configuration")
    parser.add_argument(
        "--port",
        type=int,
        default=9999,
        help="TCP port to listen on, bound to localhost. Java picks a free port "
        "and connects to it. Ignored when --uds-path is given.",
    )
    parser.add_argument(
        "--max_count",
        type=int,
        default=1,
        help="How many clients may be connected at once. Further connections "
        "wait until one frees up. The default of 1 also puts the server in "
        "user mode, where it closes itself once its single client goes away "
        "rather than waiting for another.",
    )
    parser.add_argument(
        "--py_folder",
        type=str,
        default=".",
        help="The SEMOSS py directory, appended to sys.path so this server can "
        "import its own modules (semoss, gaas_*, smss_*) and so executed code "
        "can import them by name.",
    )
    parser.add_argument(
        "--insight_folder",
        type=str,
        default=".",
        help="Working directory for this process, one per insight or engine. "
        "Its log.txt receives the server's logging, and Java reads console.txt "
        "from the same place.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="",
        help="Marker that tags a line of output as partial results streaming "
        "back mid execution: output starting with it is stripped of the prefix "
        "and sent as interim STDOUT rather than as the final response. Java "
        "generates a random one per process and can reset it with the 'prefix' "
        "command.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="Minutes the server may sit with no client connected before it "
        "shuts itself down and releases its resources, the GPU above all. "
        "Zero or less means it waits forever.",
    )
    parser.add_argument(
        "--start",
        type=bool,
        default=True,
        help="Start serving immediately. False constructs the server without "
        "entering its accept loop, which is only useful when embedding it.",
    )
    parser.add_argument(
        "--logger_level",
        type=str,
        default="INFO",
        help="Logging level for this process: CRITICAL, WARNING, INFO or "
        "DEBUG. Anything unrecognized is treated as DEBUG.",
    )
    parser.add_argument(
        "--userChrootFolder",
        type=str,
        help="Chroot into this directory before serving, confining executed "
        "code to it. The environment is cleared as part of the switch, so pass "
        "anything the process needs as an argument rather than an env var.",
    )
    parser.add_argument(
        "--engine_owned",
        action="store_true",
        help="This process runs an engine's own python (a model, vector, "
        "function or guardrail engine) rather than a user's python code. "
        "Adapters that only make sense for user code are skipped.",
    )
    parser.add_argument(
        "--uds-path",
        type=str,
        default=None,
        help="Listen on this AF_UNIX socket path instead of a TCP port "
        "(used by the namespace sandbox, whose empty netns makes TCP "
        "loopback unreachable)",
    )
    return parser.parse_args()


# python.exe C:/workspace/Semoss_Dev/py/gaas_tcp_socket_server.py --port 5359 --max_count 1 --py_folder C:/workspace/Semoss_Dev/py --insight_folder C:/workspace/Semoss_Dev/InsightCache/MODEL_agrukpJ --prefix p_aIBr2j --timeout 15
if __name__ == "__main__":
    args = parse_args()

    # Set the logging level based on command line argument
    logger_level_input = args.logger_level.strip().upper()
    if logger_level_input == "CRITICAL":
        logging_level = logging.CRITICAL
    elif logger_level_input == "WARNING":
        logging_level = logging.WARNING
    elif logger_level_input == "INFO":
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG

    logging.basicConfig(level=logging_level)

    # Perform chroot if userChrootFolder is specified
    if args.userChrootFolder:
        try:
            os.chroot(args.userChrootFolder)
            os.chdir("/")  # Change to root directory within chroot
            logging.info(
                f"Chrooted to {args.userChrootFolder} and changed directory to /"
            )
            sandbox_path = os.environ.get("PATH")
            os.environ.clear()
            # Keep only the explicitly configured executable path in the
            # chrooted worker environment; the rest of the host environment is
            # intentionally discarded.
            if sandbox_path:
                os.environ["PATH"] = sandbox_path
            smss_inline_display.pin_headless_backend()
        except PermissionError:
            logging.error("Permission denied: You need to run this script as root.")
            sys.exit(1)
        except FileNotFoundError:
            logging.error(
                f"The specified chroot path {args.userChrootFolder} does not exist."
            )
            sys.exit(1)
        except Exception as e:
            logging.error(f"An error occurred during chroot: {e}")
            sys.exit(1)

    Server(
        port=args.port,
        max_count=args.max_count,
        py_folder=args.py_folder,
        insight_folder=args.insight_folder,
        prefix=args.prefix,
        timeout=args.timeout,
        start=args.start,
        uds_path=args.uds_path,
        engine_owned=args.engine_owned,
    )
