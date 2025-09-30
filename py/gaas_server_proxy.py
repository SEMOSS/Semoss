from typing import List, Any, Optional
import threading
import random
import string


class ServerProxy:
    """This class is used to send requests from a python process back to the Tomcat Server"""

    def __init__(self):
        """
        Initialize the ServerProxy instance.
        """
        self.condition = threading.Condition()

        from gaas_tcp_server_handler import TCPServerHandler

        self.server = TCPServerHandler.da_server

    def get_next_epoc(self) -> str:
        """This method atomically returns a random value that is unique across all thread operations and instances."""
        return "py_" + "".join(random.choice(string.digits) for _ in range(17))

    def comm(
        self,
        epoc: str,
        engine_type: str,
        engine_id: str,
        method_name: str,
        method_args: Optional[List[Any]] = [],
        method_arg_types: Optional[List[str]] = [],
        insight_id: Optional[str] = None,
        operation: str = "REACTOR",
    ):
        """
        This method in responsible for:
            - converting the args into a PayloadStruct
            - adds itself to the monitor block
            - calls the server to deliver the message
            - acquires and goes into wait
            - once it gets the response removes it from the monitors
            - returns the response back

        Args:
            epoc (`str`): The epoc ID for the payload struct
            engine_type (`Optional[str]`): The engine type that will be called from the tomcat server. Options are model, storage, database or vector and are set in NativePyEngineWorker.java
            engine_id (`Optional[str]`): The unique identifier of the engine being called. This passed so the tomcat server can call Utility.java to find the engine
            method_name (`Optional[str]`): The IEngine method name that is available in the engine_type
            method_args (`Optional[List[Any]]`): A list of object to be sent to the IEngine method as inputs
            method_arg_types (`Optional[List[str]]`): A list of Java class names that represent the method args types
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            `List[Dict]`: A list that contains the response from the tomcat server engine.
        """
        # get the original payload from the current thread so that we can get the insight id
        # orig_payload = getattr(current_thread(), "payload", None)
        orig_payload = getattr(self.server.thread_local, "payload", None)

        if insight_id is None:
            assert (
                orig_payload is not None
            ), "Unable to determine insight id from the original payload"

            insight_id = orig_payload.get("insightId")

        # construct the PayloadStruct
        payload = {
            "epoc": epoc,
            "response": False,
            "engineType": engine_type,
            "interim": False,
            # all the method stuff will come here
            "objId": engine_id,
            "methodName": method_name,
            "payload": method_args,
            "payloadClassNames": method_arg_types,
            "operation": operation,  # should be REACTOR or ENGINE ... mostly REACTOR
            # these values are send back and forth for debug/logging purposes
            "insightId": insight_id,
            "executionInsightId": (
                orig_payload.get("executionInsightId") if orig_payload else None
            ),
            "jobId": (orig_payload.get("jobId") if orig_payload else None),
            "sessionId": (orig_payload.get("sessionId") if orig_payload else None),
            "mdc": (orig_payload.get("mdc") if orig_payload else None),
        }

        # adds itself to the monitor block
        self.server.monitors.update({epoc: self.condition})
        # acquires and goes into wait
        self.condition.acquire()
        self.server.send_request(payload)  # send the request
        self.condition.wait()
        # once it gets the response removes it from the monitors
        self.condition.release()

    def callReactor(self, epoc: str, pixel: str, insight_id: Optional[str] = None):
        """
        This method is responsible for initiating a pixel call communication with the server using a separate thread, which calls the `comm` method.

        Args:
            epoc (`str`): The epoc ID for the payload struct.
            pixel (`str`): The pixel being executed to tomcat
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            `List[Dict]`: A list that contains the response from the Tomcat server engine.
        """
        orig_payload = getattr(self.server.thread_local, "payload", None)

        # Setting the thread-local storage for the new thread
        def set_thread_local_payload():
            self.server.thread_local.payload = orig_payload
            self.comm(
                epoc=epoc,
                engine_type=None,
                engine_id=None,
                method_name=None,
                method_args=[pixel],
                method_arg_types=None,
                insight_id=insight_id,
                operation="REACTOR",
            )

        thread = threading.Thread(target=set_thread_local_payload)
        thread.start()  # start the thread
        thread.join()  # wait for it to finish

        new_payload_struct = self.server.monitors.pop(epoc)

        if "ex" in new_payload_struct:
            # if exception, convert exception and give back
            raise Exception(new_payload_struct["ex"])
        else:
            return new_payload_struct["payload"]

    def callEngine(
        self,
        epoc: str,
        engine_type: str,
        engine_id: str,
        method_name: str = "None",
        method_args: Optional[List[Any]] = [],
        method_arg_types: Optional[List[str]] = [],
        insight_id: Optional[str] = None,
    ):
        """
        This method is responsible for initiating a remote engine communication with the server using a separate thread, which calls the `comm` method.

        Args:
            epoc (`str`): The epoc ID for the payload struct.
            engine_type (`str`): The engine type that will be called from the Tomcat server. Options are model, storage, database, or vector and are set in NativePyEngineWorker.java.
            engine_id (`str`): The unique identifier of the engine being called. This is passed so the Tomcat server can call Utility.java to find the engine.
            method_name (`Optional[str`): The IEngine method name that is available in the `engine_type`.
            method_args (`Optional[List[Any]]`): A list of objects to be sent to the IEngine method as inputs.
            method_arg_types (`Optional[List[str]]`): A list of Java class names that represent the method argument types.
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            `List[Dict]`: A list that contains the response from the Tomcat server engine.
        """
        orig_payload = getattr(self.server.thread_local, "payload", None)

        # Setting the thread-local storage for the new thread
        def set_thread_local_payload():
            self.server.thread_local.payload = orig_payload
            self.comm(
                epoc=epoc,
                engine_type=engine_type,
                engine_id=engine_id,
                method_name=method_name,
                method_args=method_args,
                method_arg_types=method_arg_types,
                insight_id=insight_id,
                operation="ENGINE",
            )

        thread = threading.Thread(target=set_thread_local_payload)
        thread.start()  # start the thread
        thread.join()  # wait for it to finish

        new_payload_struct = self.server.monitors.pop(epoc)

        if "ex" in new_payload_struct:
            # if exception, convert exception and give back
            raise Exception(new_payload_struct["ex"])
        else:
            return new_payload_struct["payload"]

    def get_thread_insight_id(self):
        """Helper function to get insight_id from the current thread's payload"""
        try:
            # get the original payload from the current thread so that we can get the insight id
            orig_payload = getattr(self.server.thread_local, "payload", None)

            if orig_payload:
                return orig_payload.get("executionInsightId") or orig_payload.get(
                    "insightId"
                )
            else:
                return None
        except AttributeError:
            return None


if __name__ == "__main__":
    from gaas_tcp_socket_server import Server

    Server(port=9999, start=True)
