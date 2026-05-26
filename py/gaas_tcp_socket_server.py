import argparse
import logging
import sys
import socketserver
import threading
import asyncio
import os
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
    ):
        self.logger = logging.getLogger("SocketServer")
        self.logger.debug("__init__")
        self.stop = False
        self.port = port
        self.max_count = max_count
        self.cur_count = 0
        self.user_mode = self.max_count == 1
        self.insight_folder = insight_folder
        self.prefix = prefix

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
    parser.add_argument("--port", type=int, default=9999, help="Port number")
    parser.add_argument("--max_count", type=int, default=1, help="Max count")
    parser.add_argument("--py_folder", type=str, default=".", help="Python Folder")
    parser.add_argument(
        "--insight_folder", type=str, default=".", help="Insight Folder"
    )
    parser.add_argument("--prefix", type=str, default="", help="Prefix")
    parser.add_argument("--timeout", type=int, default=15, help="Timeout")
    parser.add_argument("--start", type=bool, default=True, help="Start")
    parser.add_argument(
        "--logger_level", type=str, default="INFO", help="The level of the logger"
    )
    parser.add_argument("--userChrootFolder", type=str, help="Directory to chroot into")
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
            os.environ.clear()
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
    )
