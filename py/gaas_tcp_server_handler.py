from typing import Dict, List, Any, Optional, Union, Tuple

import sys
import socketserver

import traceback as tb
import threading

import gc as gc
import sys
import re

# IMPORTANT
# Your python support extention might tell you that these packages arent being used
# That is incorrect. They get by some of the base code that exists.
# An example is importing a py frame where it needs PyFrame
import socket
import string
import random
import datetime
from clean import PyFrame
import gaas_server_proxy as gsp
import logging
import smssutil

import jsonpickle as jp
import json as json
import math
import numpy as np
import pandas as pd

import contextlib
import semoss_console as console


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
    if isinstance(dataframe, pd.DataFrame):
        data_dict = dataframe.to_dict(orient="split")
        for col_name, col_data in data_dict["data"].items():
            data_dict["data"][col_name] = [
                str(value) if pd.notna(value) else "NaN" for value in col_data
            ]
        return data_dict

    return dataframe


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

    # Class attribute to hold a singleton instance
    da_server = None

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

        self.monitor = threading.Condition()

        TCPServerHandler.da_server = self

        # cache where the link between payload id and monitor is kept
        self.monitors = {}

        # add the storage
        # LLM
        # DB Proxy here
        self.prefix = self.server.prefix
        self.insight_folder = self.server.insight_folder
        self.log_file = None

        # need to set timeout here also
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
        self.dev_log_switch = False

        # define_root_logger_script = "import sys\nroot_logger = logging.getLogger()\nroot_logger.setLevel(logging.WARNING)\nhandler = logging.StreamHandler(sys.stdout)\nformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')\nhandler.setFormatter(formatter)\nroot_logger.addHandler(handler)"
        # with contextlib.redirect_stdout(self.console), contextlib.redirect_stderr(self.console):
        #     exec(define_root_logger_script, globals())

    def custom_dev_logger(self, message):
        """
        ONLY WRITES TO LOGS WHEN self.dev_log_switch IS TRUE
        Write to the log txt file. Ensures file exists, creates new line, adds message and flushes log.
        This is very useful when the python debugger cannot handle troubleshooting a threading issue.
        """
        if self.log_file is not None and self.dev_log_switch:
            self.log_file.write("\n")
            self.log_file.write(message)
            self.log_file.flush()

    def prod_logger(self, message):
        """
        These messages will be logged to the log file in container environments.
        Write to the log txt file. Ensures file exists, creates new line, adds message and flushes log.
        """
        if self.log_file is not None:
            self.log_file.write("\n")
            self.log_file.write(message)
            self.log_file.flush()

    def handle(self):
        while not self.stop:
            # print("listening")
            try:
                # Receive the first 4 bytes to get the size
                data = self.request.recv(4)
                if not data:
                    raise RuntimeError(
                        "No data received or connection closed.")

                size = int.from_bytes(data, "big")

                epoc_size = 20
                epoc = b""

                # Loop until we receive the expected epoc_size bytes
                while len(epoc) < epoc_size:
                    chunk = self.request.recv(epoc_size - len(epoc))
                    if not chunk:
                        raise RuntimeError(
                            "No data received or connection closed.")
                    epoc += chunk

                # Decode the epoc data as UTF-8
                epoc = epoc.decode("utf-8")

                # Receive the remaining data with the specified size
                data = b""
                # Loop until we receive the expected size bytes
                while len(data) < size:
                    chunk = self.request.recv(size - len(data))
                    if not chunk:
                        raise RuntimeError(
                            "No data received or connection closed.")
                    data += chunk

                # print(f"process the data ---- {data.decode('utf-8')}")
                # payload = data.decode('utf-8')
                if self.server.blocking:
                    self.custom_dev_logger(
                        "Server is BLOCKING: Getting final output.")
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
                    self.request.sendall(data)
            except Exception as e:
                print(e)
                print("connection closed.. closing this socket")
                self.stop_request()
                # self.server.stop_it()
            # print("all processing has finished !!")
        # self.get_final_output(data)

    def get_final_output(self, data=None, epoc=None):
        self.prod_logger(f"Getting Final Output for Data === {data}")

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

            self.custom_dev_logger(
                f"Payload set for thread {threading.current_thread().name}: {self.thread_local.payload}"
            )

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
                self.send_output(
                    "prefix set", operation="PYTHON", response=True)

            # handle log out
            elif command == "CLOSE_ALL_LOGOUT<o>" and payload["operation"] == "CMD":
                # shut down the server
                self.stop_request()

            # elif command == 'core':
            #  exec('core_server=s
            #  print("set the core " + self.prefix)
            #  self.send_output("prefix set", payload, response=True)

            # need a way to handle stop message here

            # Need a way to push stdout as print here

            # If this is a python payload
            elif payload["operation"] == "PYTHON":
                self.handle_python(command)
            # this is when it is a response
            elif payload["response"]:
                self.handle_response()
            # nothing to do here. Unfortunately this is a py instance so we cannot anything
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
        except Exception as e:
            print(f"in the exception block  {epoc}")
            output = "".join(tb.format_exception(None, e, e.__traceback__))
            payload = {"epoc": str(epoc), "ex": [output]}
            # there is a possibility this is a response from the previous
            if epoc in self.monitors:
                condition = self.monitors[epoc]
                self.monitors.update({epoc: payload})
                condition.acquire()
                condition.notifyAll()
                condition.release()
            else:
                # This is really the only instance where we need to set the payload outside of the normal flow
                self.thread_local.payload = payload
                self.send_output(
                    output, operation="PYTHON", response=True, exception=True
                )

    def send_output(
        self,
        output,
        operation="STDOUT",
        response=False,
        interim=False,
        exception=False,
    ):
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
            operation = "STDOUT"  # orig_payload["operation"]
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

        payload = {
            "epoc": self.thread_local.payload["epoc"],
            "payload": [output],
            "response": response,
            "operation": operation,
            "interim": interim,
        }

        if "insightId" in self.thread_local.payload:
            payload.update(
                {"insightId": self.thread_local.payload["insightId"]})

        if exception:
            payload.update(
                {
                    "ex": output
                    # "payload":[None]
                }
            )

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

        # When streaming responses, this will cause the log files to become very heavy, so we only want to do this during development. Switch dev_log_switch to True to enable this.
        if self.log_file is not None and self.dev_log_switch:
            try:
                orig_payload_value = (
                    self.thread_local.payload["payload"][0]
                    if "payload" in self.thread_local.payload
                    else "There is no original payload."
                )
                new_payload = (
                    payload["payload"][0]
                    if "payload" in payload
                    else "There is no new payload."
                )
                orig_payload_insight_id = (
                    self.thread_local.payload["insightId"]
                    if "insightId" in self.thread_local.payload
                    else "There is no insightId in the original payload."
                )
                new_payload_insight_id = (
                    payload["insightId"]
                    if "insightId" in payload
                    else "There is no insightId."
                )
                prefix_mssg = (
                    self.prefix if self.prefix is not None else "There is no prefix."
                )

                self.log_file.write("\n")
                self.log_file.write(
                    "---------------------000000-------------------------"
                )
                self.log_file.write("\n")
                self.log_file.write(f"The Prefix is: {prefix_mssg}")
                self.log_file.write("\n")
                self.log_file.write(f"The Operation is {operation}")
                self.log_file.write("\n")
                self.log_file.write(f"Original Payload: {orig_payload_value}")
                self.log_file.write("\n")
                self.log_file.write(
                    f"Original Payload Insight ID: {orig_payload_insight_id}"
                )
                self.log_file.write("\n")
                self.log_file.write(f"New Payload: {new_payload}")
                self.log_file.write("\n")
                self.log_file.write(
                    f"New Payload Insight ID: {new_payload_insight_id}")
                self.log_file.write("\n")
                self.log_file.write("\n")
                self.log_file.write(
                    "----------------------END_OF_MESSAGE-------------------------"
                )
                self.log_file.flush()
                if response and not interim:
                    self.log_file.write("\n")
            except:
                # I don't want to ever stop if there is an error in this block
                self.custom_dev_logger("There was an error during logging")

        # send it out
        self.request.sendall(ret_array)

    def send_request(self, payload):
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

        self.custom_dev_logger(f"send_request(): REQUEST === {payload}")

        # send it out
        self.request.sendall(ret_array)

    def stop_request(self):
        if not self.stop:
            self.server.remove_handler()
            self.server.stop_it()
            self.request.close()
            import sys

            sys.exit("Connection has been closed")
            self.stop = True

    def close_request(self):
        print("close request called")

    def handle_timeout(self):
        print("handler timeout.. ")

    def release_all(self):
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

    def handle_python(self, command):
        is_exception = False
        print(f"Executing command {command.encode('utf-8')}")

        payload = self.thread_local.payload
        # set the payload coming in
        self.console.set_payload(payload=payload)

        output = None
        with contextlib.redirect_stdout(self.console), contextlib.redirect_stderr(
            self.console
        ):
            if command.endswith(".py") or command.startswith("smssutil"):
                try:
                    output = eval(command, globals())
                except Exception as e:
                    try:
                        output = exec(command, globals())
                    except Exception as e:
                        is_exception = True
                        output = str(e)
                print(f"executing file.. {command.encode('utf-8')}")
            # all new
            else:
                # same trick - try to eval if it fails run as exec
                globals()["core_server"] = self

                output, is_exception = self.execute_and_capture(command)

            self.send_output(
                output if type(output) is not type(None) else '""',
                operation=payload["operation"],
                response=True,
                exception=is_exception,
            )

    def execute_and_capture(self, code) -> Tuple[str, bool]:
        """
        Mimics a Python Jupyter kernel for executing a code block. The intended purpose of this method is to try capture the final line output

        If an exception occures then it will return and exception flag and the traceback string.

        Args:
            code (`str`): The Python code to be executed.

        Returns:
            `Tuple[str, Any]`: A tuple containing the captured print statements and the result of the last expression. The first element is a string capturing any output from print statements, and the second element can be of any type, representing the result of the last expression executed. If the last line is not an expression or doesn't produce a result, the second element will be None.
        """

        try:
            try:
                # Split the code into lines
                lines = code.strip().split("\n")
                last_line = lines[-1] if lines else ""
                preceding_lines = lines[:-1]

                # Create a string to hold preceding lines of code
                preceding_code = "\n".join(preceding_lines)

                # Execute preceding lines
                if preceding_code:
                    exec(preceding_code, globals())

                # Evaluate last line (if not empty) and capture the output
                last_line_output = '""'
                if last_line:
                    try:
                        last_line_output = eval(last_line, globals())
                    except:
                        # Fallback to exec if eval fails, indicating it's not an expression
                        exec(last_line, globals())

                return last_line_output, False
            except Exception:
                # we failed so try run all the code as is
                try:
                    return eval(code, globals()), False
                except:
                    exec(code, globals())
                    return '""', False
        except Exception as e:
            # if we fail all attempts then send back the traceback
            traceback = sys.exc_info()[2]
            full_trace = ["Traceback (most recent call last):\n"]
            full_trace = (
                full_trace
                + tb.format_tb(traceback)[1:]
                + tb.format_exception_only(type(e), e)
            )

            return "".join(full_trace), True

    def handle_response(self):
        payload = self.thread_local.payload
        # print("In the response block")
        # this is a response coming back from a request from the java container
        self.custom_dev_logger(
            f"handle_response() -- Handling response which is going to check the monitors for epoc {payload.get('epoc', 'EPOC NOT FOUND')}. Here are the monitors: {self.monitors}"
        )
        if payload["epoc"] in self.monitors:
            self.prod_logger(
                f"handle_response() -- Payload Response: {payload}")

            condition = self.monitors[payload["epoc"]]
            self.monitors.update({payload["epoc"]: payload})
            condition.acquire()
            condition.notifyAll()
            condition.release()

    def handle_shell(self):
        payload = self.thread_local.payload
        # get the method name
        try:
            # we can look at changing it to a lower point
            self.cmd_monitor.acquire()
            method_name = payload["methodName"]
            print(f"insight id is {payload['insightId']}")
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
                commands = [
                    command for command in commands if len(command) > 0]
                command = commands[0]
                output = "Command not allowed"
                # mounts =
                if command == "cd" or command.startswith("cd"):
                    output = self.exec_cd(
                        mount_name=mount_name, payload=commands)
                elif command == "dir" or command == "ls":
                    output = self.exec_dir(
                        mount_name=mount_name, payload=commands)
                elif command == "cp" or command == "copy":
                    output = self.exec_cp(
                        mount_name=mount_name, payload=commands)
                elif command == "mv" or command == "move":
                    output = self.exec_cp(
                        mount_name=mount_name, payload=commands)
                elif command == "git":
                    output = self.exec_generic(
                        mount_name=mount_name, payload=commands)
                elif command == "mvn":
                    output = self.exec_generic(
                        mount_name=mount_name, payload=commands)
                elif command == "rm" or command == "del":
                    # if commands has -r
                    # get the third argument and try to see it can resolve to a directory
                    # if so remove that
                    dir_name = commands[1]
                    # dir_name = self.exec_cd(mount_name = mount_name, payload=["cd", dir_name])
                    # if not dir_name.startswith("Sorry"):
                    output = self.exec_generic(
                        mount_name=mount_name, payload=commands)
                    # else:
                    #  output = dir_name
                elif command == "pwd":
                    output = self.exec_generic(
                        mount_name=mount_name, payload=commands)
                elif command == "deltree":
                    output = self.exec_generic(
                        mount_name=mount_name, payload=commands)
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
                    output = self.exec_generic(
                        mount_name=mount_name, payload=commands)
                else:
                    output = "Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, deltree, pwd, git, mvn (Experimental), mkdir, pnpm(Experimental)"

                # replace the mount point / hide it
                output = output.replace("\\", "/")
                orig_dir = self.orig_mount_points[mount_name]
                orig_dir_opt1 = orig_dir.replace("\\", "/")
                insensitive_orig_dir = re.compile(
                    re.escape(orig_dir), re.IGNORECASE)
                output = insensitive_orig_dir.sub("_", output)
                insensitive_orig_dir = re.compile(
                    re.escape(orig_dir_opt1), re.IGNORECASE
                )
                output = insensitive_orig_dir.sub("_", output)

                # send the output
                self.send_output(
                    output, operation=payload["operation"], response=True)
                # if command == 'ls' or command == 'dir':
                #  exec_cd(mount_name=mount_name, payload=payload['payload'])
            self.cmd_monitor.release()
        except Exception:
            self.cmd_monitor.release()
            raise

    def get_cd(self, mount_name):
        cur_dir = ""
        if mount_name in self.cur_mount_points:
            cur_dir = self.cur_mount_points[mount_name]
            return cur_dir
        else:
            # raise exception
            raise Exception(f"There is no mount point for {mount_name}")

    def exec_cd(self, mount_name=None, payload=None, check=True):
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

    def exec_dir(self, mount_name=None, payload=None):
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

    def exec_cp(self, mount_name=None, payload=None):
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

    def exec_generic(self, mount_name=None, payload=None):
        import subprocess

        cur_mount_dir = self.get_cd(mount_name)
        orig_mount_dir = self.orig_mount_points[mount_name]
        # execute git
        proc = subprocess.Popen(
            payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE
        )
        output = proc.stdout.read().decode("utf-8")
        return output


if __name__ == "__main__":
    from gaas_tcp_socket_server import Server
    Server(port=9999, start=True)
