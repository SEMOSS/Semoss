# TODO -- move to Java
# TODO -- move to Java
# TODO -- move to Java
# TODO -- move to Java
# TODO -- move to Java
# TODO -- move to Java
# TODO -- move to Java
import logging
import multiprocessing

import json
import socket
import struct
import unittest
import time

from typing import List, Union, Any
import dataclasses

import os

# Get the current directory of the script
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Go two directories above
SEMOSS_DEV_PY = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))

import sys
sys.path.append(SEMOSS_DEV_PY)

from gaas_tcp_socket_server import Server, parse_args

@dataclasses.dataclass
class PayloadStruct:
    """A PayloadStruct class in python to test tcp requests/responses
    """
    
    epoc:str = None
    operation:str = "PYTHON"
    methodName:str = "method"
    payload:List[Any] = None
    payloadClasses:List[str] = None

    payloadClassNames:List[str] = None
    engineType:str = None

    ex:str = None
    processed:bool = False
    longRunning:bool = False
    env:str = None
    interim:bool = False

    inputAlias:List[str] = None
    aliasReturn:str = None

    hasReturn:bool = True

    parentEpoc:str = None
    response:bool = False

    objId:str = None

    projectId:str = None

    projectName:str = None

    portalId:str = None

    insightId:str = None
    
def get_next_epoc() -> str:
    '''This method atomically increments the epoc count by one plus the current value.'''
    epoc = 0 + 1
    return f"ps{epoc}"
    
def write_payload(ps, socket_client):
    # Nulling the classes so they don't affect JSON serialization
    ps['epoc'] = get_next_epoc()
    json_ps = json.dumps(ps)
    ps_bytes = pack(json_ps, ps['epoc'])
    
    socket_client.send(ps_bytes)
    
def get_final_response(socket_client):
    while True:
        response_str = receive_response(socket_client)
        print("Received response:", response_str)

        # Convert JSON string to dictionary
        payload_struct_dict = json.loads(response_str)

        # Create a PayloadStruct instance from dictionary
        payload_struct = PayloadStruct(**payload_struct_dict)

        if payload_struct.response:
            return payload_struct

def pack(message, epoc):
    # Encode message and epoch into bytes
    ps_byets = bytearray(message.encode("utf-8"))
    
    # Get size of message
    length:int = len(ps_byets)

    # Convert size to bytes (big-endian)
    len_bytes = struct.pack('>I', length)
    
    # Converting string to bytes and allocating 20 bytes
    epoc_bytes = epoc.encode('utf-8')
    epoc_bytes_padded = epoc_bytes.ljust(20, b'\x00')

    # Concatenate all byte arrays
    final_bytes = len_bytes + epoc_bytes_padded + ps_byets

    return final_bytes

def receive_response(s):
    try:
        # Receive the length of the response
        len_bytes = s.recv(4)
        response_len = struct.unpack('>I', len_bytes)[0]

        # Receive the response data
        response_data = b''
        while len(response_data) < response_len:
            chunk = s.recv(response_len - len(response_data))
            if not chunk:
                break
            response_data += chunk

        return response_data.decode('utf-8')
    except Exception as ex:
        print(ex)
        
def start_server():
    args = parse_args()
  
    # Set the logging level based on command line argument
    logger_level_input = args.logger_level.strip().upper()
    if (logger_level_input == "CRITICAL"):
        logging_level = logging.CRITICAL
    elif (logger_level_input == "WARNING"):
        logging_level = logging.WARNING
    elif (logger_level_input == "INFO"):
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    
    logging.basicConfig(
        level=logging_level,
    )

    Server(
        port=args.port, 
        max_count=args.max_count, 
        py_folder=args.py_folder, 
        insight_folder=args.insight_folder, 
        prefix=args.prefix, 
        timeout=args.timeout, 
        start=args.start
    )
        
class TestServerClient(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        "Hook method for setting up class fixture before running tests in the class."
        
        # start the server in a separate process
        server_process = multiprocessing.Process(target=start_server)
        server_process.start()

        time.sleep(2)
    
        # connect the client        
        cls.socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cls.socket_client.connect(('localhost', 9999))
        
        cls.ps = PayloadStruct(
            methodName="runScript",
            longRunning=True,
            payload=[
                '''2'''
            ],
            insightId="c14ca9b8-0df4-4f6f-bfa1-cd0e35fa0563",
        )

    @classmethod
    def tearDownClass(cls):
        "Hook method for deconstructing the class fixture after running all tests in the class."
        cls.socket_client.close()
        
    def test_sum(self):
        command = '''2+2'''
        
        self.ps.payload = [command]
        
        write_payload(dataclasses.asdict(self.ps), self.__class__.socket_client)

        response:PayloadStruct = get_final_response(self.__class__.socket_client)

        self.assertEqual(response.payload[0], 4)

    def test_function_with_wait(self):
        command = '''
        def test():
            import time
            counter = 0
            while counter < 5:
                print(counter)
                time.sleep(1)
                counter+=1
            return 22
            
        test()
        '''
        
        self.ps.payload = [command]
        
        write_payload(dataclasses.asdict(self.ps), self.__class__.socket_client)

        response:PayloadStruct = get_final_response(self.__class__.socket_client)

        self.assertEqual(response.payload[0], 22)


if __name__ == '__main__':
    unittest.main()
