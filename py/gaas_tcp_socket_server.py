import socketserver
import logging
import sys
import threading
from gaas_tcp_server_handler import TCPServerHandler

logging.basicConfig(level=logging.DEBUG,
                    format='%(name)s: %(message)s',
                    )

class Server(socketserver.ThreadingTCPServer):
  
  def __init__(self, server_address=None, handler_class=TCPServerHandler, port=81, max_count=1, py_folder=".", insight_folder=".", prefix="", start=False):
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
    if start:
      self.serve_forever()
    
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
      print("waking up.. ")
      self.monitor.notify()

  def stop_it(self):
    print(f"{self.max_count} <> {self.cur_count}")
    if self.user_mode:
    #if self.max_count <= self.cur_count:
      self.stop = True
      socketserver.TCPServer.server_close(self)
   
if __name__ == '__main__':
  if len(sys.argv) == 2:
    Server(port=int(sys.argv[1]), start=True)
  if len(sys.argv) == 3:
    Server(port=int(sys.argv[1]), max_count=int(sys.argv[2]), start=True)
  if len(sys.argv) == 4:
    Server(port=int(sys.argv[1]), max_count=int(sys.argv[2]), py_folder=sys.argv[3], start=True)
  if len(sys.argv) == 5:
    Server(port=int(sys.argv[1]), max_count=int(sys.argv[2]), py_folder=sys.argv[3], insight_folder=sys.argv[4], start=True)
  if len(sys.argv) == 6:
    Server(port=int(sys.argv[1]), max_count=int(sys.argv[2]), py_folder=sys.argv[3], insight_folder=sys.argv[4], prefix=sys.argv[5], start=True)
    
 