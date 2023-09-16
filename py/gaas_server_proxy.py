import threading

# we may need to make this singleton
# or create similar proxies.. but same issue no way to pass it
class ServerProxy():

  def __init__(self, g={}, l={}):
    self.epoc = 0
    self.condition = threading.Condition()
    from gaas_tcp_server_handler import TCPServerHandler
    self.server = TCPServerHandler.da_server
    self.rest = False
    # try to see if there is a rest api exposed
    if self.server is None:
      from gaas_rest_server import RESTServer
      self.server = RESTServer.da_server
      self.rest = True
    print(f"set the server to .. {self.server}")
    self.globals = g
    self.locals = l

  def get_next_epoc(self):
    self.epoc = self.epoc + 1
    return f"py_{self.epoc}"

  # method_args is typically a list
  def comm(self, epoc=None, engine_type="storage", engine_id = None, method_name=None, method_args=[], method_arg_types=[], insight_id=None):
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
      "engineType": engine_type,
      "interim": False,
      # all the method stuff will come here
      "objId": engine_id,
      "methodName": method_name,
      "payload": method_args,
      "payloadClassNames":method_arg_types,
      "insightId": insight_id,
      "operation":"ENGINE"
    }
    # adds itself to the monitor block
    self.server.monitors.update({epoc:self.condition})
    if not self.rest:
      # acquires and goes into wait
      self.condition.acquire()
      # send the request
      self.server.send_request(payload)
      self.condition.wait()
      self.condition.release()
      # once it gets the response removes it from the monitors
    else:
      self.server.send_request(payload)
  

  def call(self, epoc=None, engine_type='Storage', engine_id=None, method_name='None', method_args=None, method_arg_types=None, insight_id=None):
    #print("doing call.. ")
    if self.rest:
      return self.call_sync(epoc=epoc, engine_type=engine_type, engine_id=engine_id, method_name=method_name, method_args=method_args, method_arg_types=method_arg_types, insight_id=insight_id)
    else:
      return self.call_async(epoc=epoc, engine_type=engine_type, engine_id=engine_id, method_name=method_name, method_args=method_args, method_arg_types=method_arg_types, insight_id=insight_id)


  def call_async(self, epoc=None, engine_type='Storage', engine_id=None, method_name='None', method_args=None, method_arg_types=None, insight_id=None):
    #epoc = self.get_next_epoc()
    if insight_id is None:
      return "Insight Id cannot be none"
    thread = threading.Thread(target=self.comm, kwargs={
              'epoc':epoc, 
              'engine_type':engine_type, 
              'engine_id': engine_id, 
              'method_name':method_name, 
              'method_args':method_args,
              'method_arg_types': method_arg_types,
              'insight_id': insight_id
              })
    thread.start()
    thread.join()
    #else
    #  thread.join
    new_payload_struct = self.server.monitors.pop(epoc)
    print(new_payload_struct)
    # if exception
    # convert exception and give back
    
    if 'ex' in new_payload_struct:
      raise Exception(new_payload_struct['ex']) 
      #return new_payload_struct['ex']
    else:
      #new_payload_struct = process_payload(new_payload_struct['payload'])
      return new_payload_struct['payload']

  def call_sync(self, epoc=None, engine_type='Storage', engine_id=None, method_name='None', method_args=None, method_arg_types=None, insight_id=None):
    #epoc = self.get_next_epoc()
    #print("running sync.. ")
    if insight_id is None:
      return "Insight Id cannot be none"
    self.comm(epoc=epoc, engine_type=engine_type, engine_id=engine_id, method_name=method_name, method_args = method_args, method_arg_types=method_arg_types, insight_id=insight_id)
    #else
    #  thread.join
    new_payload_struct = self.server.monitors.pop(epoc)
    print(new_payload_struct)
    # if exception
    # convert exception and give back
    
    if 'ex' in new_payload_struct:
      raise Exception(new_payload_struct['ex']) 
      #return new_payload_struct['ex']
    else:
      #new_payload_struct = process_payload(new_payload_struct['payload'])
      print(f"answer is .. {new_payload_struct['payload']}")
      return new_payload_struct['payload']



  def test(self):
    epoc = self.get_next_epoc()
    thread = threading.Thread(target=self.comm, kwargs={'epoc':epoc, 
                              'engine_type':"ENGINE", 
                              'engine_id': 'hello', 
                              'method_name':'method1', 
                              'method_args':['cat', 'bat', None],
                              'method_arg_types':['java.lang.String', 'java.lang.String', 'java.lang.String']
                              })
    thread.start()
    thread.join()
    new_payload_struct = self.server.monitors.pop(epoc)
    #print(new_payload_struct)
    # if exception
    # convert exception and give back
    if 'ex' in new_payload_struct:
      return new_payload_struct['ex']
    else:
      # if we get to the point for json pickle we can do that
      #new_payload_struct = process_payload(new_payload_struct)
      return new_payload_struct['payload']
      
  def process_payload(self, payload_struct):
    # try to see if the types are pickle
    # if so unpickle it
    import jsonpickle as jp
    payload_data = None
    if 'payload' in payload_struct:
      payload_data = payload_struct['payload']
    if payload_data is not None and isinstance(payload_data, list):
      for data in payload_data:
        index = payload_data.index(data)
        try:
          orig_obj = data
          obj = jp.loads(orig_obj)
          payload_struct['payload'][index] = obj
        except Exception as e:
          pass
    return payload_struct
    
