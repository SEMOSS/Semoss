from typing import Dict, List, Any, Optional, Union, Tuple
import sys
import socketserver
import traceback as tb
import threading
import importlib
import importlib.util
import builtins as _builtins_mod
import os
import gc as gc
import sys
import re
import ast
import textwrap

# IMPORTANT
# Your python support extention might tell you that these packages arent being used
# That is incorrect. They get by some of the base code that exists.
# An example is importing a py frame where it needs PyFrame
import socket
import string
import random
import datetime
import logging
import smssutil
import json as json
import math
import contextlib
import semoss_console as console


def _lazy_import(name):
    """Bind the module now but defer its import cost to first attribute access."""
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.find_spec(name)
    spec.loader = importlib.util.LazyLoader(spec.loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pd = _lazy_import("pandas")
np = _lazy_import("numpy")
jp = _lazy_import("jsonpickle")

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gaas_tcp_socket_server import Server


def custom_nan_handler(nan_value: Any) -> Union[Any, str]:
    """Custom handler for NaN values"""
    if math.isnan(nan_value):
        return "NaN"

    return nan_value


def custom_tostr_handler(value: Any) -> str:
    """Custom handler to convert any value to string"""
    return str(value)


def custom_pandas_handler(dataframe: Any) -> Union[Any, Dict]:
    """Custom handler to stringify values in pandas DataFrame"""
    import pandas as pd

    if isinstance(dataframe, pd.DataFrame):
        data_dict = dataframe.to_dict(orient="split")
        for col_name, col_data in data_dict["data"].items():
            data_dict["data"][col_name] = [
                str(value) if pd.notna(value) else "NaN" for value in col_data
            ]
        return data_dict

    return dataframe


from gaas_tcp_server_thread_local import (
    _asset_thread_local,
    _asset_ns_key,
    smss_clear_app_imports as _smss_clear_app_imports,
    smss_get_runtime_var as _smss_get_runtime_var,
)

# Capture the real __import__ before we replace it.
_orig_import = _builtins_mod.__import__


def _asset_aware_import(name, _globals=None, _locals=None, fromlist=(), level=0):
    """
    Wraps builtins.__import__ to isolate project-local modules per asset path.

    When handle_python sets _asset_thread_local.active_paths, any simple
    (non-dotted, absolute) import whose .py file exists inside one of those
    paths is loaded from the explicit file path and cached under a namespaced
    sys.modules key (_smss_{hash}_{name}).  Concurrent threads with different
    asset paths never see each other's cached copy.

    All other imports (e.g. torch, numpy, stdlib) fall through to the real
    __import__ unchanged — sys.modules is never replaced.
    """
    if level == 0 and "." not in name:
        active_paths = getattr(_asset_thread_local, "active_paths", None)
        if active_paths:
            for path in active_paths:
                ns_key = _asset_ns_key(path) + "_" + name
                cached = sys.modules.get(ns_key)
                if cached is not None:
                    return cached
                candidate = os.path.join(path, name + ".py")
                if os.path.exists(candidate):
                    spec = importlib.util.spec_from_file_location(ns_key, candidate)
                    mod = importlib.util.module_from_spec(spec)
                    # Register before exec so circular imports resolve correctly
                    sys.modules[ns_key] = mod
                    spec.loader.exec_module(mod)
                    return mod
    return _orig_import(name, _globals, _locals, fromlist, level)


_builtins_mod.__import__ = _asset_aware_import


class ExecutionCancelled(Exception):
    """Raised when a running python execution is cancelled by a remote request."""


class TCPServerHandler(socketserver.BaseRequestHandler):
    """
    This class is the request handler for the Native Python Server.

    This class is instantiated for each request to be handled.  The
    constructor sets the instance variables request, client_address
    and server, and then calls the handle() method.  To implement a
    specific service, all you need to do is to derive a class which
    defines a handle() method.

    The handle() method can find the request as self.request, the
    client address as self.client_address, and the server (in case it
    needs access to per-server information) as self.server.  Since a
    separate instance is created for each request, the handle() method
    can define other arbitrary instance variables.
    """

    if TYPE_CHECKING:
        server: Server
        request: socket.socket

    # Class attribute to hold a singleton instance
    da_server = None
    CANCELLED_MESSAGE = "The request was cancelled by the user"

    def log_level_mapper(self, log_level_name: str) -> int:
        """
        Maps a log level name to its corresponding logging constant.

        Args:
            log_level_name (str): The name of the log level (e.g., "DEBUG", "INFO").

        Returns:
            int: The logging constant corresponding to the log level name.
        """
        log_mapper = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        return log_mapper.get(log_level_name)

    def logging_setup(self):
        """Configures logging with environment-based log levels."""
        try:
            log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()  # (default: INFO)

            logging.basicConfig(
                filename=f"{self.insight_folder}/log.txt",
                level=self.log_level_mapper(log_level_name),
                format="%(asctime)s - %(levelname)s - %(message)s",
                filemode="a",
                force=True,
            )

            # Create a logger and apply a filter to log only the exact level
            self.logger = logging.getLogger("TCPServerHandler")
            log_level = getattr(
                logging, log_level_name, self.log_level_mapper(log_level_name)
            )
            self.logger.addFilter(
                lambda record: record.levelno == log_level
            )  # Logs only the selected level
            self.logger.info("Logging Setup Completed")
        except Exception as e:
            self.log_file.write("\n ERROR - Unexpected Error While Logging Setup.")
            self.log_file.flush()

    def setup(self):
        """
        This method is responsible for initializing the server before it starts to serve client requests.

        The method is called automatically when the Server is instantiated,
        typically during the creation of the socketserver.ThreadingTCPServer instance, before the server starts listening for client connections.
        """
        self.stop = False

        # TODO: These are currently not in use. Check with PK whether or not the are needed
        self.message = None
        self.size = 0
        self.msg_index = 0
        self.residue = None

        TCPServerHandler.da_server = self

        # a lock to serialise all socket writes. HuggingFace (and similar libraries)
        # download files in parallel worker threads that all write tqdm progress to
        # stderr → SemossConsole → send_output() → sendall(). Without this lock the
        # concurrent sendall() calls interleave bytes on the wire, which corrupts the
        # 4-byte size header that Java uses to frame messages. Java then tries to read
        # an astronomically large payload, its receive buffer fills up, sendall()
        # blocks on the Python side, and both ends deadlock.
        self.send_lock = threading.Lock()

        # cache where the link between payload id and monitor is kept
        self.monitors = {}
        # cache insight cancellation flags so a secondary request can interrupt
        # currently running execution while keeping the process alive
        self.insight_cancel_events = {}
        self.insight_cancel_events_lock = threading.Lock()

        # add the storage
        # LLM
        # DB Proxy here
        self.prefix = self.server.prefix
        self.insight_folder = self.server.insight_folder
        self.log_file = None
        self.logger = None

        # self.server.timeout_val holds the timeout in seconds for the client connection socket.
        if self.server.timeout_val > 0:
            self.request.settimeout(self.server.timeout_val)
        else:
            # this may not be needed but
            self.request.settimeout(None)

        if self.insight_folder is not None:
            # print(f"starting to log in location {self.insight_folder}/log.txt")
            self.log_file = open(
                f"{self.insight_folder}/log.txt", "a", encoding="utf-8"
            )

        print("Ready to start server")
        print(f"Server is {self.server}")
        self.orig_mount_points = {}
        self.cur_mount_points = {}
        self.cmd_monitor = threading.Condition()

        self.try_jp = False

        # experimental
        if self.try_jp:
            import numpy as np
            import pandas as pd

            jp.handlers.register(float, custom_nan_handler)
            jp.handlers.register(np.datetime64, custom_tostr_handler)
            jp.handlers.register(pd.DataFrame, custom_pandas_handler)
            self.serializier = jp
        else:
            self.serializier = json

        self.console = console.SemossConsole(
            socket_handler=self,
        )

        # set the thread local
        TCPServerHandler.thread_local = threading.local()

        # Sometimes the debugger is not effective or cannot handle certain troubleshooting scenarios.
        # This is where you can use the custom_log() method. It writes to the log txt file, ensuring the file exists, creates a new line, adds the message, and flushes the log.
        # The logs can become very heavy during streamed responses so for some log statements we want to only write them when we are developing locally
        # I don't have a way of knowing what env we are in so adding a manual dev switch here.
        # If you use this, be sure to turn it off before committing your code.
        env_value = os.getenv("PYTHON_DEV_LOGS")

        # Convert the environment variable to a boolean
        if env_value is not None:
            # Convert to boolean by checking against common true values
            self.dev_log_switch = env_value.lower() in ["true", "1", "yes", "on"]
        else:
            # Default to False if the environment variable is not set
            self.dev_log_switch = False

        # define_root_logger_script = "import sys\nroot_logger = logging.getLogger()\nroot_logger.setLevel(logging.WARNING)\nhandler = logging.StreamHandler(sys.stdout)\nformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')\nhandler.setFormatter(formatter)\nroot_logger.addHandler(handler)"
        # with contextlib.redirect_stdout(self.console), contextlib.redirect_stderr(self.console):
        #     exec(define_root_logger_script, globals())
        self.logging_setup()  # setting up to log

    def custom_dev_logger(self, message: str):
        """
        ONLY WRITES TO LOGS WHEN self.dev_log_switch IS TRUE
        Write to the log txt file. Ensures file exists, creates new line, adds message and flushes log.
        This is very useful when the python debugger cannot handle troubleshooting a threading issue.

        Args:
            message (str): The message to log.
        """
        if self.logger and self.dev_log_switch:
            self.logger.info(message)

    def prod_logger(self, message: str):
        """
        These messages will be logged to the log file in container environments.
        Write to the log txt file. Ensures file exists, creates new line, adds message and flushes log.

        Args:
            message (str): The message to log.
        """
        if self.logger:
            self.logger.info(message)

    def handle(self):
        """
        Handles incoming requests from the client.
        This method is called for each request to be handled.
        """
        while not self.stop:
            # print("listening")
            try:
                # Receive the first 4 bytes to get the size
                data = self.request.recv(4)
                if not data:
                    raise RuntimeError("No data received or connection closed.")

                size = int.from_bytes(data, "big")

                epoc_size = 20
                epoc = b""

                # Loop until we receive the expected epoc_size bytes
                while len(epoc) < epoc_size:
                    chunk = self.request.recv(epoc_size - len(epoc))
                    if not chunk:
                        raise RuntimeError("No data received or connection closed.")
                    epoc += chunk

                # Decode the epoc data as UTF-8
                epoc = epoc.decode("utf-8")

                # Receive the remaining data with the specified size
                data = b""
                # Loop until we receive the expected size bytes
                while len(data) < size:
                    chunk = self.request.recv(size - len(data))
                    if not chunk:
                        raise RuntimeError("No data received or connection closed.")
                    data += chunk

                # print(f"process the data ---- {data.decode('utf-8')}")
                # payload = data.decode('utf-8')
                if self.server.blocking:
                    self.custom_dev_logger("Server is BLOCKING: Getting final output.")
                    self.get_final_output(data, epoc)
                else:
                    self.custom_dev_logger(
                        "Server is NOT BLOCKING: Starting new thread."
                    )
                    runner = threading.Thread(
                        target=self.get_final_output,
                        kwargs=({"data": data, "epoc": epoc}),
                    )
                    runner.start()
                # self.get_final_output(data)
                if not data:
                    break
            except socket.timeout:
                self.logger.warning("Client connection timed out. Closing this socket.")
                self.stop_request()
            except Exception as e:
                self.logger.warning(f"An unexpected error occurred: {e}")
                self.logger.warning("Closing this socket due to an unexpected error.")
                self.stop_request()

    def log_data(self, data: Union[bytes, dict, None]):
        """
        Log the data to the log file. This is useful for debugging purposes.
        Trimming the payload to 50 characters to avoid log file bloat.

        Args:
            data (Union[bytes, dict, None]): The data to log.
        """
        try:
            if isinstance(data, bytes):
                data = json.loads(data.decode("utf-8"))
            elif data is None:
                data = {}

            payload = data.get("payload", [])

            log = {
                "epoc": data.get("epoc", "N/A"),
                "payload": payload,
                "operation": data.get("operation", "N/A"),
            }

            log_message = (
                f"Final Output: {json.dumps(log, ensure_ascii=False, indent=4)}"
            )
            self.prod_logger("------------- OUTPUT LOG - START ----------------\n")
            self.prod_logger(log_message)
            self.prod_logger("------------- OUTPUT LOG - END ----------------\n")
        except Exception as e:
            self.logger.warning(f"Error in get_final_output: {str(e)}")

    def get_final_output(
        self, data: Optional[bytes] = None, epoc: Optional[str] = None
    ):
        """
        Processes the final output of a request.

        Args:
            data (Optional[bytes]): The data received from the client. Defaults to None.
            epoc (Optional[str]): The epoc associated with the request. Defaults to None.
        """
        self.log_data(data)

        payload = ""
        # payload = data
        # if this fails.. there is nothing you can do..
        # you just have to send the response back as error
        try:
            # if this fails.. no go
            # but the receiver still needs to be informed so it doesnt stall
            payload = data
            if not self.try_jp:
                payload = data.decode("utf-8")
            payload = self.serializier.loads(payload)

            # print(f"PAYLOAD.. {payload}")
            # do payload manipulation here
            # payload = json.loads(payload)

            # SETTING THE PAYLOAD HERE... NO NEED TO PASS IT AROUND WITH PARAMS
            self.thread_local.payload = payload

            payload_set_log = {
                threading.current_thread().name: self.thread_local.payload
            }
            self.custom_dev_logger("---------- PAYLOAD SET LOG - START -----------\n")
            self.custom_dev_logger(
                f"Payload Set For Thread - {json.dumps(payload_set_log, ensure_ascii=False, indent=4)}"
            )
            self.custom_dev_logger("---------- PAYLOAD SET LOG - END -------------\n")

            command_list = payload["payload"]
            command = ""
            output_file = ""
            err_file = ""

            # command, output_file, error_file
            if len(command_list) > 0:
                command = command_list[0]
            if len(command_list) > 1:
                output_file = command_list[1]
            if len(command_list) > 2:
                err_file = command_list[2]

            # print("command set to " + command)
            # print(command_list)

            if command == "stop" and payload["operation"] == "CMD":
                self.stop_request()
            # handle setting prefix
            elif command == "prefix" and payload["operation"] == "CMD":
                self.prefix = output_file
                if self.prefix is None:
                    print("The prefix is None")
                else:
                    print("The prefix is set to value = " + self.prefix)
                self.send_output("prefix set", operation="PYTHON", response=True)
            # handle log out
            elif command == "CLOSE_ALL_LOGOUT<o>" and payload["operation"] == "CMD":
                # shut down the server
                self.stop_request()
            # handle clear of insight globals
            elif (
                command == "CLEAR_NON_MODULE_GLOBALS"
                and payload["operation"] == "INSIGHT"
            ):
                insight_id = payload.get("insightId")
                store = InsightGlobalStore()
                store.clear_non_module_globals(insight_id)
                self.send_output(
                    "Successfully cleared non-module globals",
                    operation=payload["operation"],
                    response=True,
                )
            # handle delete of insight globals
            elif (
                command == "REMOVE_INSIGHT_GLOBALS"
                and payload["operation"] == "INSIGHT"
            ):
                insight_id = payload.get("insightId")
                store = InsightGlobalStore()
                store.remove_insight_globals(insight_id)
                self.send_output(
                    "Successfully removed insight globals",
                    operation=payload["operation"],
                    response=True,
                )
            elif command == "INTERRUPT_INSIGHT" and payload["operation"] == "INSIGHT":
                interrupted = self.interrupt_insight_execution(payload)
                message = (
                    "Interrupt signal received"
                    if interrupted
                    else "No insight id provided for interrupt request"
                )
                self.send_output(
                    message,
                    operation=payload["operation"],
                    response=True,
                )
            # If this is a python payload
            elif payload["operation"] == "PYTHON":
                insight_id = payload.get("insightId")
                self.handle_python(command, insight_id)
            # this is when it is a response
            elif payload["response"]:
                self.handle_response()
            # this is when we are doing shell
            elif payload["operation"] == "CMD":
                self.handle_shell()
            else:
                output = f"This is a python only instance. Command {str(command).encode('utf-8')} is not supported"
                print(f"{str(command).encode('utf-8')} = {output}")
                # output = "Response.. " + data.decode("utf-8")
                self.send_output(
                    output,
                    operation=payload["operation"],
                    response=True,
                    exception=True,
                )
        except (SystemExit, KeyboardInterrupt, GeneratorExit):
            raise
        except BaseException as e:
            print(f"in the exception block  {epoc}")
            output = "".join(tb.format_exception(None, e, e.__traceback__))
            error_payload = {"epoc": epoc, "ex": [output]}
            # there is a possibility this is a response from the previous
            condition = self.monitors.get(epoc)
            if isinstance(condition, threading.Condition):
                with condition:
                    if self.monitors.get(epoc) is condition:
                        self.monitors[epoc] = error_payload
                        condition.notify_all()
            else:
                # This is really the only instance where we need to set the payload outside of the normal flow
                self.thread_local.payload = error_payload
                self.send_output(
                    output, operation="PYTHON", response=True, exception=True
                )

    def log_payload_details(
        self, payload: dict, operation: str, response: bool, interim: bool
    ):
        """
        Logs payload details for debugging purposes.
        This helps prevent excessive logging when streaming responses.

        Args:
            payload (dict): The payload to log.
            operation (str): The operation being performed.
            response (bool): Whether this is a response.
            interim (bool): Whether this is an interim response.
        """
        # When streaming responses, this will cause the log files to become very heavy, so we only want to do this during development. Switch dev_log_switch to True to enable this.
        if not self.logger or not self.dev_log_switch:
            return

        try:
            orig_payload_value = self.thread_local.payload.get(
                "payload", ["There is no original payload."]
            )[0]
            new_payload = payload.get("payload", ["There is no new payload."])[0]
            orig_payload_insight_id = self.thread_local.payload.get(
                "insightId", "There is no insightId in the original payload."
            )
            new_payload_insight_id = payload.get("insightId", "There is no insightId.")
            prefix_mssg = (
                self.prefix if self.prefix is not None else "There is no prefix."
            )

            log_data = {
                "Prefix": prefix_mssg,
                "Operation": operation,
                "Original Payload": orig_payload_value,
                "Original Payload Insight ID": orig_payload_insight_id,
                "New Payload": new_payload,
                "New Payload Insight ID": new_payload_insight_id,
            }

            formatted_log = json.dumps(
                log_data, ensure_ascii=False, indent=4
            )  # formatting log data to pretty format

            self.logger.info(
                "--------------------- SENDING OUTPUT -------------------------\n"
            )
            self.logger.info(formatted_log)
            self.logger.info("--------------------- END ---------------------------\n")

        except Exception:
            # Ensure logging errors do not crash the application
            self.logger.warning("There was an error during logging")

    def send_output(
        self,
        output: Any,
        operation: str = "STDOUT",
        response: bool = False,
        interim: bool = False,
        exception: bool = False,
    ):
        """
        Sends output back to the client.

        Args:
            output (Any): The output to send.
            operation (str, optional): The operation being performed. Defaults to "STDOUT".
            response (bool, optional): Whether this is a response. Defaults to False.
            interim (bool, optional): Whether this is an interim response. Defaults to False.
            exception (bool, optional): Whether an exception occurred. Defaults to False.
        """
        # Do not write any prints here
        # since the console is captured it will go into recursion

        # Stdout = true, response = true = partial
        # interim = true are the parts

        # stdout = false, response = true <-- actual response

        # print("sending output " + output)
        # make it back into payload just for epoch
        # if this comes with prefix. it is part of the response
        if (
            self.prefix is not None
            and self.prefix != ""
            and str(output).startswith(self.prefix)
        ):
            output = output.replace(self.prefix, "")
            operation = "STDOUT"
            response = True
            interim = True

        if str(output).endswith("D.O.N.E"):
            # print("Finishing execution")
            output = str(output).replace("D.O.N.E", "")
            interim = False

        # After switching to thread_local, the operation param won't be None, so we need to check if its a dict and if it is we default to STDOUT
        operation = (
            "STDOUT"
            if isinstance(operation, dict) and "operation" in operation
            else operation
        )

        orig_payload = getattr(self.thread_local, "payload", None)
        payload = {
            "epoc": (orig_payload.get("epoc") if orig_payload else None),
            "response": response,
            "interim": interim,
            "payload": [output],
            "operation": operation,
            "insightId": (orig_payload.get("insightId") if orig_payload else None),
            "asset_paths": (orig_payload.get("asset_paths") if orig_payload else None),
            "executionInsightId": (
                orig_payload.get("executionInsightId") if orig_payload else None
            ),
            "jobId": (orig_payload.get("jobId") if orig_payload else None),
            "sessionId": (orig_payload.get("sessionId") if orig_payload else None),
            "mdc": (orig_payload.get("mdc") if orig_payload else None),
        }

        if exception:
            payload.update({"ex": output})

        output = None
        if self.try_jp:
            output = self.serializier.encode(
                payload, unpicklable=False, make_refs=False
            )
        else:
            output = self.serializier.dumps(
                payload, default=lambda obj: str(obj), allow_nan=True
            )

        # write response back
        size = len(output)
        size_byte = size.to_bytes(4, "big")
        ret_array = bytearray(size)
        # pack the size upfront
        ret_array[0:4] = size_byte
        # pack the message next
        ret_array[4:] = output.encode("utf-8")

        self.log_payload_details(payload, operation, response, interim)

        # send it out — acquire the lock so concurrent calls from parallel
        # download worker threads cannot interleave bytes and corrupt the protocol
        with self.send_lock:
            self.request.sendall(ret_array)

    def send_request(self, payload: dict):
        """
        Sends a request to the client.

        Args:
            payload (dict): The payload to send.
        """
        # Do not write any prints here
        # since the console is captured it will go into recursion

        # print("sending output " + output)
        # make it back into payload just for epoch
        # if this comes with prefix. it is part of the response
        # local = threading.local()
        # orig_payload = local.payload

        # print(f"Original Payload {orig_payload}")
        # print(locals().keys())
        # print(globals().keys())

        output = self.serializier.dumps(payload)
        # write response back
        size = len(output)
        size_byte = size.to_bytes(4, "big")
        ret_array = bytearray(size)
        # pack the size upfront
        ret_array[0:4] = size_byte
        # pack the message next
        ret_array[4:] = output.encode("utf-8")

        self.custom_dev_logger("---------- SEND REQUEST LOG - START ---------\n")
        self.custom_dev_logger(
            f"send_request(): REQUEST === {json.dumps(payload, ensure_ascii=False, indent=4)}"
        )
        self.custom_dev_logger("---------- SEND REQUEST LOG - END -----------\n")

        # send it out — use the same lock as send_output() to prevent interleaving
        with self.send_lock:
            self.request.sendall(ret_array)

    def stop_request(self):
        """Stops the request and closes the connection."""
        if not self.stop:
            self.server.remove_handler()
            self.server.stop_it()
            self.request.close()
            import sys

            self.custom_dev_logger("---------- STOP REQUEST LOG - START ---------\n")
            self.custom_dev_logger("Connection has been closed")
            self.custom_dev_logger("---------- STOP REQUEST LOG - END -----------\n")

            sys.exit("Connection has been closed")

    def close_request(self):
        """Closes the request."""
        print("close request called")

    def release_all(self):
        """Releases all conditions so no threads are breaking."""
        # pushes out all the conditions
        # so no threads are breaking
        # technically this is not a good way.. but
        epocs_to_release = list(self.monitors.keys())
        payload = {"ex": "Failed to perform operation, forcing release"}
        for epoc in epocs_to_release:
            condition = self.monitors[epoc]
            payload.update({"epoc": epoc})
            self.monitors.update(epoc, payload)
            condition.acquire()
            condition.notifyAll()
            condition.release()

    def _resolve_execution_cancel_keys(
        self, payload: dict, insight_id: Optional[str] = None
    ):
        keys = []
        if payload is not None:
            payload_job_id = payload.get("jobId")
            execution_insight_id = payload.get("executionInsightId")
            payload_insight_id = payload.get("insightId")
            if payload_job_id:
                keys.append(f"job::{payload_job_id}")
            if execution_insight_id:
                keys.append(f"insight::{execution_insight_id}")
            if payload_insight_id:
                insight_key = f"insight::{payload_insight_id}"
                if insight_key not in keys:
                    keys.append(insight_key)

        if insight_id:
            insight_key = f"insight::{insight_id}"
            if insight_key not in keys:
                keys.append(insight_key)

        return keys

    def _resolve_interrupt_target_keys(self, payload: dict):
        keys = []
        if payload is None:
            return keys

        payload_job_id = payload.get("jobId")
        if payload_job_id:
            return [f"job::{payload_job_id}"]

        execution_insight_id = payload.get("executionInsightId")
        payload_insight_id = payload.get("insightId")
        if execution_insight_id:
            keys.append(f"insight::{execution_insight_id}")
        if payload_insight_id:
            insight_key = f"insight::{payload_insight_id}"
            if insight_key not in keys:
                keys.append(insight_key)
        return keys

    def _get_or_create_cancel_event(self, insight_id: str) -> threading.Event:
        with self.insight_cancel_events_lock:
            event = self.insight_cancel_events.get(insight_id)
            if event is None:
                event = threading.Event()
                self.insight_cancel_events[insight_id] = event
            return event

    def interrupt_insight_execution(self, payload: dict) -> bool:
        keys = self._resolve_interrupt_target_keys(payload)
        if not keys:
            return False

        for key in keys:
            self._get_or_create_cancel_event(key).set()
        return True

    def _prepare_execution_cancel_events(
        self, payload: dict, insight_id: Optional[str]
    ) -> List[threading.Event]:
        keys = self._resolve_execution_cancel_keys(payload, insight_id)
        events = []
        for key in keys:
            event = self._get_or_create_cancel_event(key)
            # reset any stale interrupt so new work starts cleanly
            event.clear()
            events.append(event)
        return events

    def _build_cancel_trace(self, cancel_events: List[threading.Event]):
        def _cancel_trace(frame, event, arg):
            for cancel_event in cancel_events:
                if cancel_event.is_set():
                    raise ExecutionCancelled(self.CANCELLED_MESSAGE)
            return _cancel_trace

        return _cancel_trace

    def handle_python(self, command: str, insight_id: str):
        """
        Execute python code within the proper globals object

        Args:
            command (`str`): The python code to execute
            insight_id (`str`): The insight id / global store to execute with
        """
        # Import the thread-local setters and getters
        from smss_thread_local import set_smss_stream, clear_smss_stream

        payload = self.thread_local.payload
        asset_paths = payload.get("asset_paths")
        runtime_vars = payload.get("runtime_vars") or {}
        cancel_events = self._prepare_execution_cancel_events(payload, insight_id)
        cancel_trace = self._build_cancel_trace(cancel_events)

        # If asset paths are provided, prepend the logic to set the sys.path
        if asset_paths:
            # Ensure asset_paths is a list for consistent processing
            if isinstance(asset_paths, str):
                asset_paths = [asset_paths]

            if isinstance(asset_paths, list) and asset_paths:
                path_script = ""
                # Add each path to sys.path
                # Reverse to maintain order after inserting at 0
                for asset_path in reversed(asset_paths):
                    if not asset_path:
                        continue
                    path_script += textwrap.dedent(f"""
                        import sys
                        asset_path = r'{asset_path}'
                        if asset_path in sys.path:
                            sys.path.remove(asset_path)
                        sys.path.insert(0, asset_path)
                        del asset_path

                        """)
                command = path_script + command

        # If legacy append_vars are provided, prepend these vars at the start of the script
        append_vars = payload.get("append_vars") or {}
        if append_vars:
            append_vars_script = ""
            # Add each variable
            for key in append_vars:
                append_vars_script += textwrap.dedent(f"""
                        {key} = r'{append_vars.get(key)}'
                        """)
            append_vars_script += "\n"
            command = append_vars_script + command

        store = InsightGlobalStore()
        insight_globals = store.get_insight_globals(insight_id)

        # Collect the normalised asset paths for per-project module isolation.
        # _asset_aware_import reads _asset_thread_local.active_paths to namespace
        # any project-local .py file under a per-path key in sys.modules so
        # concurrent threads for different projects never share helper modules.
        _active_paths = (
            [p for p in (asset_paths or []) if p]
            if isinstance(asset_paths, list)
            else ([asset_paths] if asset_paths else [])
        )

        # Store insight_globals in thread-local so smss_clear_app_imports()
        # (gaas_tcp_server_thread_local) can evict __smss_mcp_* aliases at call
        # time without needing to import this module. Injecting the same module-
        # level function object every time means there is no closure and no race
        # on the 'smss_clear_app_imports' key in insight_globals.
        _asset_thread_local.insight_globals = insight_globals
        insight_globals["smss_clear_app_imports"] = _smss_clear_app_imports

        # Store runtime vars in thread-local so they can be accessed by the smss_get_runtime_var function that is injected into the insight's globals. This is so variables can be passed from Java to Python without running into a race condition on the insight_globals when multiple threads are running for the same insight.
        _asset_thread_local.runtime_vars = runtime_vars

        insight_globals["smss_get_runtime_var"] = _smss_get_runtime_var

        # Define and inject the smss_stream function
        def smss_stream_func(
            data: Any, stream_type: str = "content", interim: bool = True
        ):
            structured_output = {"stream_type": stream_type, "data": data}
            self.send_output(
                structured_output,
                operation="STRUCTURED_STREAM",
                response=True,
                interim=interim,
            )

        # Get the original process CWD to ensure we change back to it
        process_cwd = None
        try:
            process_cwd = os.getcwd()
        except FileNotFoundError:
            process_cwd = None

        try:
            # Set the function for the current thread
            set_smss_stream(smss_stream_func)
            # Activate per-project module isolation for this thread
            _asset_thread_local.active_paths = _active_paths if _active_paths else None

            # Determine the target CWD for this specific insight from its globals
            target_cwd = insight_globals.get("__smss_cwd__")
            if not target_cwd:
                # If no CWD is stored for the insight, default to the first asset path
                if asset_paths and isinstance(asset_paths, list) and asset_paths:
                    target_cwd = asset_paths[0]

            if target_cwd:
                try:
                    os.chdir(target_cwd)
                except Exception:
                    # If we can't change to the dir, just proceed from the current dir
                    pass

            try:
                is_exception = False
                user_cancelled = False
                output = None
                with contextlib.redirect_stdout(
                    self.console
                ), contextlib.redirect_stderr(self.console):
                    insight_globals["core_server"] = self
                    # Engine-owned python processes (model / vector / etc.) set
                    # disableCancelTrace because their executions can never be
                    # cancelled by a user. Skipping sys.settrace avoids its large
                    # overhead on import-heavy init / ask scripts.
                    disable_cancel_trace = bool(payload.get("disableCancelTrace"))
                    previous_trace = sys.gettrace()
                    try:
                        if not disable_cancel_trace:
                            sys.settrace(cancel_trace)
                        output, is_exception, user_cancelled = self.execute_and_capture(
                            command, insight_globals
                        )
                    finally:
                        if not disable_cancel_trace:
                            sys.settrace(previous_trace)

                    self.send_output(
                        output if type(output) is not type(None) else '""',
                        operation=(
                            payload["operation"] if not user_cancelled else "CANCELLED"
                        ),
                        response=True,
                        exception=is_exception,
                    )
            finally:
                # After execution, save the potentially new CWD back to the insight's globals
                insight_globals["__smss_cwd__"] = os.getcwd()
        finally:
            # Always clear the function for the current thread
            clear_smss_stream()
            _asset_thread_local.active_paths = None
            _asset_thread_local.insight_globals = None
            _asset_thread_local.runtime_vars = None
            # Always change back to the original process CWD
            if process_cwd is not None:
                os.chdir(process_cwd)

    def execute_and_capture(
        self, code: str, insight_globals: dict
    ) -> Tuple[str, bool, bool]:
        """
        Mimics a Python Jupyter kernel for executing a code block. The intended purpose of this method is to try capture the final line output

        If an exception occures then it will return and exception flag and the traceback string.

        Args:
            code (`str`): The Python code to be executed.
            insight_globals (`dict`): The globals dict to execute with.

        Returns:
            `Tuple[str, bool]`: A tuple containing the output of the last expression in the code input and a boolean if it was successfully able to execute the code.
                                The first element is the eval output of the last expression. If last expression is not evaluable, then it will exec and return an empty string.
                                The second element is a boolean indicating if the code was executed successfully (False) or if an exception occurred (True).
                                The third element is a boolean indicatiing if the code was cancelled by the user (True) or not (False).
        """
        try:
            # Handle empty code
            if not code.strip():
                return '""', False, False

            parsed_code = ast.parse(code)

            # Execute all statements before the last one
            if len(parsed_code.body) > 1:
                preceding_code = ast.unparse(parsed_code.body[:-1])
                exec(preceding_code, insight_globals)

            last_node = parsed_code.body[-1]

            # Special handling for print()
            if (
                isinstance(last_node, ast.Expr)
                and isinstance(last_node.value, ast.Call)
                and isinstance(last_node.value.func, ast.Name)
                and last_node.value.func.id == "print"
            ):
                # Evaluate print arguments to be the return value of this function
                print_args = last_node.value.args
                if len(print_args) == 1:
                    return (
                        eval(ast.unparse(print_args[0]), insight_globals),
                        False,
                        False,
                    )
                else:
                    # Return a tuple of evaluated arguments
                    return (
                        tuple(
                            eval(ast.unparse(arg), insight_globals)
                            for arg in print_args
                        ),
                        False,
                        False,
                    )

            # Check if the last node is an evaluatable expression using an explicit list
            can_eval = isinstance(last_node, ast.Expr) and isinstance(
                last_node.value,
                (
                    ast.Attribute,
                    ast.BinOp,
                    ast.BoolOp,
                    ast.Call,
                    ast.Compare,
                    ast.Constant,
                    ast.Dict,
                    ast.DictComp,
                    ast.FormattedValue,
                    ast.GeneratorExp,
                    ast.IfExp,
                    ast.JoinedStr,
                    ast.Lambda,
                    ast.List,
                    ast.ListComp,
                    ast.Name,
                    ast.NamedExpr,
                    ast.Set,
                    ast.SetComp,
                    ast.Subscript,
                    ast.Tuple,
                    ast.UnaryOp,
                ),
            )

            if can_eval:
                return (eval(ast.unparse(last_node), insight_globals), False, False)
            else:
                # It's a statement or a non-evaluatable expression, so just execute it
                exec(ast.unparse(last_node), insight_globals)
                return ('""', False, False)

        except ExecutionCancelled as e:
            return (str(e), True, True)
        except (SystemExit, KeyboardInterrupt, GeneratorExit):
            raise
        except BaseException as e:
            # Catch BaseException (not just Exception) so that C-extension panics
            # such as pyo3_runtime.PanicException are captured and returned to the
            # caller instead of crashing the thread silently.
            traceback = sys.exc_info()[2]
            full_trace = ["Traceback (most recent call last):\n"]
            full_trace = (
                full_trace
                + tb.format_tb(traceback)[1:]
                + tb.format_exception_only(type(e), e)
            )

            return ("".join(full_trace), True, False)

    def handle_response(self):
        """
        Handle a Java response for a previously issued Python request.

        The request thread stores `epoc -> Condition` in `self.monitors` and waits
        on that condition in `gaas_server_proxy.comm()`. When the matching response
        arrives, this method:
        1. Finds the waiting monitor entry by `epoc`.
        2. Replaces `epoc -> Condition` with `epoc -> payload`.
        3. Calls `notify_all()` to wake the waiting request thread.

        Identity/type checks guard against stale or already-cleaned monitor entries.
        """
        payload = self.thread_local.payload
        epoc = payload.get("epoc", "EPOC NOT FOUND")

        # this is a response coming back from a request from the java container
        self.custom_dev_logger("---------- HANDLE RESPONSE LOG - START ---------\n")
        self.custom_dev_logger(
            f"handle_response() -- Handling response which is going to check the monitors for epoc {epoc}. Here are the monitors: {self.monitors}"
        )
        if epoc in self.monitors:
            self.prod_logger(
                f"\nhandle_response() -- Payload Response: {json.dumps(payload, ensure_ascii=False, indent=4)}"
            )
            condition = self.monitors.get(epoc)
            if isinstance(condition, threading.Condition):
                with condition:
                    if self.monitors.get(epoc) is condition:
                        self.monitors[epoc] = payload
                        condition.notify_all()
        self.custom_dev_logger("---------- HANDLE RESPONSE LOG - END ---------\n")

    def handle_shell(self):
        """Handles a shell command."""
        payload = self.thread_local.payload
        # get the method name
        try:
            # we can look at changing it to a lower point
            self.cmd_monitor.acquire()
            method_name = payload["methodName"]
            mount_name, mount_dir = payload["insightId"].split("__", 1)
            if method_name == "constructor":
                # set the mount point
                # execute this once so that you know it even exists
                cmd_payload = ["cd", mount_dir]
                if mount_name not in self.orig_mount_points:
                    mount_dir = self.exec_cd(
                        mount_name=mount_name, payload=cmd_payload, check=False
                    )
                    self.orig_mount_points.update({mount_name: mount_dir})
                    self.cur_mount_points.update({mount_name: mount_dir})
                self.send_output(
                    mount_dir, operation=payload["operation"], response=True
                )

            if method_name == "removeMount":
                # set the mount point
                # execute this once so that you know it even exists
                if mount_name not in self.orig_mount_points:
                    self.orig_mount_points.pop(mount_name)
                    self.cur_mount_points.pop(mount_name)
                self.send_output(
                    "Mount point removed",
                    operation=payload["operation"],
                    response=True,
                )

                # return "completed constructor"
            if method_name == "executeCommand":
                # get the insight id
                # get the mount dir
                # see what the command is and execute accordingly
                # need to see the process of cd etc.
                cur_dir = self.get_cd(mount_name)
                commands = payload["payload"][0].split(" ")
                commands = [command for command in commands if len(command) > 0]
                command = commands[0]
                output = "Command not allowed"
                # mounts =
                if command == "cd" or command.startswith("cd"):
                    output = self.exec_cd(mount_name=mount_name, payload=commands)
                elif command == "dir" or command == "ls":
                    output = self.exec_dir(mount_name=mount_name, payload=commands)
                elif command == "cp" or command == "copy":
                    output = self.exec_cp(mount_name=mount_name, payload=commands)
                elif command == "mv" or command == "move":
                    output = self.exec_cp(mount_name=mount_name, payload=commands)
                elif command == "git":
                    output = self.exec_generic(mount_name=mount_name, payload=commands)
                elif command == "mvn":
                    output = self.exec_generic(mount_name=mount_name, payload=commands)
                elif command == "rm" or command == "del":
                    # if commands has -r
                    # get the third argument and try to see it can resolve to a directory
                    # if so remove that
                    dir_name = commands[1]
                    # dir_name = self.exec_cd(mount_name = mount_name, payload=["cd", dir_name])
                    # if not dir_name.startswith("Sorry"):
                    output = self.exec_generic(mount_name=mount_name, payload=commands)
                    # else:
                    #  output = dir_name
                elif command == "pwd":
                    output = self.exec_generic(mount_name=mount_name, payload=commands)
                elif command == "deltree":
                    output = self.exec_generic(mount_name=mount_name, payload=commands)
                elif command == "mkdir":
                    dir_name = commands[1]
                    dir_name = self.exec_cd(
                        mount_name=mount_name, payload=["cd", dir_name]
                    )
                    if not dir_name.startswith("Sorry"):
                        output = self.exec_generic(
                            mount_name=mount_name, payload=commands
                        )
                    else:
                        output = dir_name
                elif command == "pnpm":
                    output = self.exec_generic(mount_name=mount_name, payload=commands)
                else:
                    output = "Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, deltree, pwd, git, mvn (Experimental), mkdir, pnpm(Experimental)"

                # replace the mount point / hide it
                output = output.replace("\\", "/")
                orig_dir = self.orig_mount_points[mount_name]
                orig_dir_opt1 = orig_dir.replace("\\", "/")
                insensitive_orig_dir = re.compile(re.escape(orig_dir), re.IGNORECASE)
                output = insensitive_orig_dir.sub("_", output)
                insensitive_orig_dir = re.compile(
                    re.escape(orig_dir_opt1), re.IGNORECASE
                )
                output = insensitive_orig_dir.sub("_", output)

                # send the output
                self.send_output(output, operation=payload["operation"], response=True)
                # if command == 'ls' or command == 'dir':
                #  exec_cd(mount_name=mount_name, payload=payload['payload'])
            self.cmd_monitor.release()
        except Exception:
            self.cmd_monitor.release()
            raise

    def get_cd(self, mount_name: str) -> str:
        """
        Gets the current directory for a given mount name.

        Args:
            mount_name (str): The name of the mount.

        Returns:
            str: The current directory.
        """
        cur_dir = ""
        if mount_name in self.cur_mount_points:
            cur_dir = self.cur_mount_points[mount_name]
            return cur_dir
        else:
            # raise exception
            self.logger.warning(f"There is no mount point for {mount_name}")
            raise Exception(f"There is no mount point for {mount_name}")

    def exec_cd(
        self,
        mount_name: Optional[str] = None,
        payload: Optional[list] = None,
        check: bool = True,
    ) -> str:
        """
        Executes a cd command.

        Args:
            mount_name (Optional[str]): The name of the mount. Defaults to None.
            payload (Optional[list]): The payload for the command. Defaults to None.
            check (bool): Whether to check if the directory is within the mount sandbox. Defaults to True.

        Returns:
            str: The new directory.
        """
        import subprocess

        # there is only 2 arguments I need to accomodate for
        # cd <space>
        # ideally we should just append it to the cd call it a day
        orig_mount_dir = ""
        if len(payload) == 1:  # this is the case of cd.. or something
            payload.append(payload[0].replace("cd", ""))

        if check:
            cur_mount_dir = self.get_cd(mount_name)
            orig_mount_dir = self.orig_mount_points[mount_name]
            appender = payload[1]
            cur_mount_dir = cur_mount_dir + "/" + appender
        else:  # do it for the first time
            cur_mount_dir = payload[1]
        import subprocess

        # throw the exception
        # try:
        proc = subprocess.Popen(
            ["pwd"], cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE
        )
        new_dir = (
            proc.stdout.read().decode("utf-8").replace("\r\n", "").replace("\n", "")
        )
        if check and new_dir.startswith(
            orig_mount_dir
        ):  # we are in the scope all is set
            print("updating mount points")
            self.cur_mount_points.update({mount_name: new_dir})
        elif not new_dir.startswith(orig_mount_dir):
            new_dir = "Sorry, you are trying to cd outside of the mount sandbox which is not allowed"
        return new_dir
        # except NotADirectoryError:
        #  raise Exception

    def exec_dir(
        self, mount_name: Optional[str] = None, payload: Optional[list] = None
    ) -> str:
        """
        Executes a dir or ls command.

        Args:
            mount_name (Optional[str]): The name of the mount. Defaults to None.
            payload (Optional[list]): The payload for the command. Defaults to None.

        Returns:
            str: The output of the command.
        """
        import subprocess

        # there is only 2 arguments I need to accomodate for
        # cd <space>
        # ideally we should just append it to the cd call it a day
        cur_mount_dir = self.get_cd(mount_name)

        # throw the exception
        # need to accomodate for secondary arguments like ls - ls etc. - done
        proc = subprocess.Popen(
            payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE
        )
        output = proc.stdout.read().decode("utf-8")
        return output

    def exec_cp(
        self, mount_name: Optional[str] = None, payload: Optional[list] = None
    ) -> str:
        """
        Executes a cp or copy command.

        Args:
            mount_name (Optional[str]): The name of the mount. Defaults to None.
            payload (Optional[list]): The payload for the command. Defaults to None.

        Returns:
            str: The output of the command.
        """
        import subprocess

        cur_mount_dir = self.get_cd(mount_name)
        orig_mount_dir = self.orig_mount_points[mount_name]
        # copy from and to

        from_file = f"{cur_mount_dir}/{payload[1]}"
        to_file = f"{cur_mount_dir}/{payload[2]}"
        # check to see if this is from the mount space
        # the possibility here is the user does a file space of ../.. etc.. we need to catch eventually
        # execute copy
        proc = subprocess.Popen(
            payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE
        )
        output = (
            proc.stdout.read().decode("utf-8").replace("\r\n", "").replace("\n", "")
        )
        return output

    def exec_generic(
        self, mount_name: Optional[str] = None, payload: Optional[list] = None
    ) -> str:
        """
        Executes a generic command.

        Args:
            mount_name (Optional[str]): The name of the mount. Defaults to None.
            payload (Optional[list]): The payload for the command. Defaults to None.

        Returns:
            str: The output of the command.
        """
        import subprocess

        cur_mount_dir = self.get_cd(mount_name)
        orig_mount_dir = self.orig_mount_points[mount_name]
        # execute git
        proc = subprocess.Popen(
            payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE
        )
        output = proc.stdout.read().decode("utf-8")
        return output


class InsightGlobalStore:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.insight_globals = {}
        return cls._instance

    def get_insight_globals(self, insight_id: str) -> dict:
        if not insight_id:
            return {}

        if insight_id not in self.insight_globals:
            original_import = __import__
            forbidden_imports = {"socket", "subprocess"}
            forbidden_attributes = {
                "os": {
                    "execle",
                    "execl",
                    "execlp",
                    "execlpe",
                    "execv",
                    "execve",
                    "execvp",
                    "execvpe",
                    "fork",
                    "forkpty",
                    "kill",
                    "killpg",
                    "plock",
                    "popen",
                    "spawnl",
                    "spawnle",
                    "spawnlp",
                    "spawnlpe",
                    "spawnv",
                    "spawnve",
                    "spawnvp",
                    "spawnvpe",
                    "system",
                }
            }

            # define custom import
            def secure_import(name, globals=None, locals=None, fromlist=(), level=0):
                # module not allowed
                if name in forbidden_imports:
                    raise ImportError(f"Import of module '{name}' is not allowed")
                module = original_import(name, globals, locals, fromlist, level)
                # attribute in module not allowed
                if name in forbidden_attributes:
                    for attr_name in forbidden_attributes[name]:
                        if hasattr(module, attr_name):
                            try:
                                delattr(module, attr_name)
                            except (AttributeError, TypeError):
                                # Failsafe in case the attribute is not removable
                                pass

                return module

            # First-time initialization: build the globals dict
            globals_dict = {
                "__builtins__": {
                    # all standard builtins
                    **__builtins__,
                    "__import__": secure_import,
                },
                "string": string,
                "np": np,
                "pd": pd,
                "random": random,
                "datetime": datetime,
                "json": json,
                "jsonpickle": jp,
                "math": math,
                "smssutil": smssutil,
            }

            self.insight_globals[insight_id] = globals_dict

        return self.insight_globals[insight_id]

    def clear_non_module_globals(self, insight_id: str):
        """
        Clears all non-module variables from the global dictionary for a given insight.

        Args:
            insight_id (`str`): The insight id to clear the globals for.
        """
        if insight_id in self.insight_globals:
            self.insight_globals[insight_id] = {
                k: v
                for k, v in self.insight_globals[insight_id].items()
                if isinstance(v, type(sys))
                or k
                in [
                    "__smss_cwd__",
                    "string",
                    "np",
                    "pd",
                    "random",
                    "datetime",
                    "json",
                    "jsonpickle",
                    "math",
                    "smssutil",
                ]
            }

    def remove_insight_globals(self, insight_id: str):
        """
        Removes the entire global dictionary for a given insight and suggests garbage collection.

        Args:
            insight_id (`str`): The insight id to remove the globals for.
        """
        if insight_id in self.insight_globals:
            del self.insight_globals[insight_id]
            gc.collect()


if __name__ == "__main__":
    from gaas_tcp_socket_server import Server

    Server(port=9999, start=True)
