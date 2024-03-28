from typing import List, Any, Optional, Dict

import threading

from threading import current_thread
# we may need to make this singleton
# or create similar proxies.. but same issue no way to pass it

class ServerProxy:
    '''This class is used to send requests from a python process back to the Tomcat Server'''
    
    def __init__(self):
        '''
        Initialize the ServerProxy instance.
        '''
        self.epoc = 0
        self.condition = threading.Condition()

        from gaas_tcp_server_handler import TCPServerHandler
        self.server = TCPServerHandler.da_server    

    def get_next_epoc(self) -> str:
        '''This method atomically increments the epoc count by one plus the current value.'''
        self.epoc = self.epoc + 1
        return f"py_{self.epoc}"

    def comm(
        self,
        epoc:str,
        engine_type:str,
        engine_id:str,
        method_name:str,
        method_args:Optional[List[Any]] = [],
        method_arg_types:Optional[List[str]] = [],
        insight_id:Optional[str] = None
    ):
        '''
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
        '''
        if insight_id is None:
            # get the original payload from the current thread so that we can get the insight id
            orig_payload = getattr(current_thread(), "payload", None)
            
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
            "insightId": insight_id,
            "operation": "ENGINE",
        }
        
        self.server.monitors.update({epoc: self.condition}) # adds itself to the monitor block
        self.condition.acquire()                            # acquires and goes into wait
        self.server.send_request(payload)                   # send the request
        self.condition.wait()
        self.condition.release()                            # once it gets the response removes it from the monitors

    def call(
        self,
        epoc:str,
        engine_type:str,
        engine_id:str,
        method_name:str = "None",
        method_args:Optional[List[Any]] = [],
        method_arg_types:Optional[List[str]] = [],
        insight_id:Optional[str] = None
    ):
        '''
        This method is responsible for initiating a communication with the server using a separate thread, which calls the `comm` method.
        
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
        '''
        # epoc = self.get_next_epoc()
        # if insight_id is None:
        #   return "Insight Id cannot be none"
        thread = threading.Thread(
            target=self.comm,
            kwargs={
                "epoc": epoc,
                "engine_type": engine_type,
                "engine_id": engine_id,
                "method_name": method_name,
                "method_args": method_args,
                "method_arg_types": method_arg_types,
                "insight_id": insight_id,
            },
        )
        orig_payload = getattr(current_thread(), "payload", None)
        thread.payload = orig_payload
        
        thread.start()           # start the thread
        thread.join()            # wait for it to finish
        
        new_payload_struct = self.server.monitors.pop(epoc)

        if "ex" in new_payload_struct:
            # if exception, convert exception and give back
            raise Exception(new_payload_struct["ex"])
        else:
            return new_payload_struct["payload"]

    def test(self):
        epoc = self.get_next_epoc()
        thread = threading.Thread(
            target=self.comm,
            kwargs={
                "epoc": epoc,
                "engine_type": "ENGINE",
                "engine_id": "hello",
                "method_name": "method1",
                "method_args": ["cat", "bat", None],
                "method_arg_types": [
                    "java.lang.String",
                    "java.lang.String",
                    "java.lang.String",
                ],
            },
        )
        
        thread.start()
        thread.join()
        new_payload_struct = self.server.monitors.pop(epoc)
        # print(new_payload_struct)
        # if exception
        # convert exception and give back
        if "ex" in new_payload_struct:
            return new_payload_struct["ex"]
        else:
            # if we get to the point for json pickle we can do that
            # new_payload_struct = process_payload(new_payload_struct)
            return new_payload_struct["payload"]

    def process_payload(self, payload_struct):
        # try to see if the types are pickle
        # if so unpickle it
        import jsonpickle as jp

        payload_data = None
        if "payload" in payload_struct:
            payload_data = payload_struct["payload"]
        if payload_data is not None and isinstance(payload_data, list):
            for data in payload_data:
                index = payload_data.index(data)
                try:
                    orig_obj = data
                    obj = jp.loads(orig_obj)
                    payload_struct["payload"][index] = obj
                except Exception as e:
                    pass
        return payload_struct
