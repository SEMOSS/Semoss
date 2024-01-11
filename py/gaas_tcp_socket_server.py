import argparse
import logging
import sys
import socketserver
import threading
import asyncio

from gaas_tcp_server_handler import TCPServerHandler

#logging.basicConfig(level=logging.DEBUG,
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
    start=False,
    blocking=False,
    logger_level: str = "INFO"
  ):
    self.logger = logging.getLogger('SocketServer')
    self.logger.debug('__init__')
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
    if(self.port is None and len(sys.argv) > 0):
      self.port = sys.argv[0]

    if(self.port is None and len(sys.argv) > 1):
      self.start = sys.argv[1] == 1

    # set the current folder to pick up scripts from
    import sys
    sys.path.append(py_folder)

    self.server_address = ('localhost', self.port)
    socketserver.ThreadingTCPServer.__init__(self, self.server_address, handler_class)
    # Set up a TCP/IP server
    self.logger.info("Ready to start server")
    #self.socket.timeout = 10
    # default time out is 15 min. set up if you want more
    if timeout > 0:
      timeout = timeout * 60
      print(f"Setting timeout to .. {timeout}")
      self.timeout = timeout
      #self.socket.settimeout(timeout*60)
    else:
      print(f"Setting timeout to .. {timeout}")
      self.socket.settimeout(None)
    
    self.timeout_val = timeout
  
    if start:
      self.serve_forever()
  
  def handle_timeout(self):
    # no clients.. kill this server, no point keeping it
    # give back the GPU
    self.timed_out=True
    if self.cur_count == 0:
      self.stop_it()
    
  
  def server_activate(self):
    self.logger.debug('server_activate')
    socketserver.TCPServer.server_activate(self)
    return

  def serve_forever(self):
    self.logger.debug(f'waiting for request on port {self.port}')
    self.logger.info('Handling requests, press <Ctrl-C> to quit')
    try:
      while not self.stop: 
          if self.max_count > self.cur_count:
            print("listening")
            self.handle_request()
            self.timed_out = False
            self.cur_count = self.cur_count + 1
          else:
            with self.monitor:
              # go into wait so this thread doesnt get killed otherwise leads to thread issues
              print("going into wait mode")
              self.monitor.wait()
      #self.stop_it()
    except Exception as e:
      print("Stopping all ")
      print(e)
      self.stop_it()
    return
    
  def remove_handler(self):
    self.cur_count = self.cur_count - 1
    with self.monitor:
      # Wake up thread to move forward
      #print("waking up.. ")
      self.monitor.notify()


  def stop_it(self):
    print(f"{self.max_count} <> {self.cur_count}")
    if self.user_mode:
    #if self.max_count <= self.cur_count:
      self.stop = True
      socketserver.TCPServer.server_close(self)

def parse_args():
  parser = argparse.ArgumentParser(description="Server configuration")
  parser.add_argument("--port", type=int, default=9999, help="Port number")
  parser.add_argument("--max_count", type=int,  default=1, help="Max count")
  parser.add_argument("--py_folder", type=str, default=".", help="Python Folder")
  parser.add_argument("--insight_folder", type=str, default=".", help="Insight Folder")
  parser.add_argument("--prefix", type=str, default="", help="Prefix")
  parser.add_argument("--timeout", type=int, default=15, help="Timeout")
  parser.add_argument("--start", type=bool, default=True, help="Start")
  parser.add_argument("--logger_level", type=str, default="INFO", help="The level of the logger")
  return parser.parse_args()

# python gaas_tcp_socket_server.py --port 8080 --max_count 5 --py_folder /path/to/folder --insight_folder /path/to/insight --prefix some_prefix --timeout 10 --start --debug

# C:/Users/ttrankle/AppData/Local/Programs/Python/Python310/python.exe C:/workspace/Semoss_Dev/py/gaas_tcp_socket_server.py --port 5359 --max_count 1 --py_folder C:/workspace/Semoss_Dev/py --insight_folder C:/workspace/Semoss_Dev/InsightCache/MODEL_agrukpJ --prefix p_aIBr2j --timeout 15
if __name__ == '__main__':
  
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