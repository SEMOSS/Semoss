import threading

# we may need to make this singleton
# or create similar proxies.. but same issue no way to pass it
class ServerProxy():

  def __init__(self):
    self.epoc = 0
    self.condition = threading.Condition()
    from gaas_tcp_server_handler import TCPServerHandler
    self.server = TCPServerHandler.da_server

  def get_next_epoc(self):
    self.epoc = self.epoc + 1
    return f"py_{self.epoc}"

  # method_args is typically a list
  def comm(self, epoc=None, engine_type="storage", engine_id = None, method_name=None, method_args=[]):
  # converts this into a payload
  # adds itself to the monitor block
  # calls the server to deliver the message
  # acquires and goes into wait
  # once it gets the response removes it from the monitors
  # returns the response back

    # converts this into a payload
    payload = {
      "epoc": epoc,
      "response": False,
      "operation": engine_type,
      "interim": False,
      # all the method stuff will come here
      "objId": engine_id,
      "methodName": method_name,
      "payload": method_args
    }
    # adds itself to the monitor block
    self.server.monitors.update({epoc:self.condition})
    
    
    # acquires and goes into wait
    self.condition.acquire()
    # send the request
    self.server.send_request(payload)
    self.condition.wait()
    self.condition.release()
    # once it gets the response removes it from the monitors

  def test(self):
    epoc = self.get_next_epoc()
    thread = threading.Thread(target=self.comm, kwargs={'epoc':epoc, 'engine_type':"ENGINE", 'engine_id': 'hello', 'method_name':'method1', 'method_args':['cat', 'bat', 'rat']})
    thread.start()
    thread.join()
    new_payload_struct = self.server.monitors.pop(epoc)
    print(new_payload_struct)
    # if exception
    # convert exception and give back
    if 'ex' in new_payload_struct:
      return new_payload_struct['ex']
    else:
      return new_payload_struct['payload']
    
    #ps = self.comm(engine_type="ENGINE", engine_id="hello", method_name="method1", method_args=['cat', 'bat', 'rat'])
    #print(ps['payload'])


