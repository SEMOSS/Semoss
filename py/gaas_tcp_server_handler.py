import socket
import sys
import socketserver
import json
import logging
import smssutil
import traceback as tb
import threading
from clean import PyFrame
import gaas_server_proxy as gsp

import numpy as np
import pandas as pd
import gc as gc
import sys

import string
import random
import datetime


class TCPServerHandler(socketserver.BaseRequestHandler):
  
  da_server = None
  
  def setup(self):
    self.message = None
    self.residue = None
    self.msg_index = 0
    self.stop = False
    self.size = 0
    self.my_var = {}
    self.monitor = threading.Condition()
    self.my_var.update({"core_server": self})
    TCPServerHandler.da_server = self
    #TCPServerHandler.myvar = self
    
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
    else: # this may not be needed but
      self.request.settimeout(None)
    
    if self.insight_folder is not None:
      #print(f"starting to log in location {self.insight_folder}/log.txt")
      self.log_file = open(f"{self.insight_folder}/log.txt", "a", encoding='utf-8')
    print("Ready to start server")  
    print(f"Server is {self.server}")
  
  def handle(self):
    while not self.stop:
      #print("listening")
      try:
        data = self.request.recv(4)
        size = int.from_bytes(data, "big")
        epoc_size = 20
        epoc = self.request.recv(epoc_size)
        #print(f"epoc {epoc} {epoc.decode('utf-8')}")
        data = self.request.recv(size)
        #print(f"process the data ---- {data.decode('utf-8')}")
        #payload = data.decode('utf-8')   
        if self.server.blocking:
          self.get_final_output(data)
        else:
          runner = threading.Thread(target=self.get_final_output, kwargs=({'data':data}))
          runner.start()
        #self.get_final_output(data)
        if not data: 
          break
          self.request.sendall(data)
      except Exception as e:
        print(e)
        print("connection closed.. closing this socket")
        self.stop_request()
        #self.server.stop_it()
      #print("all processing has finished !!")
    #self.get_final_output(data)
  
  def get_final_output(self, data=None, epoc=None):
    payload = ""
    #payload = data
    # if this fails.. there is nothing you can do.. 
    # you just have to send the response back as error
    try:
      # if this fails.. no go 
      # but the receiver still needs to be informed so it doesnt stall
      payload = data.decode('utf-8')      
      #print(f"PAYLOAD.. {payload}")
      # do payload manipulation here 
      payload = json.loads(payload)
      local = threading.local()
      local.payload = payload

      command_list = payload['payload']
      command = ''
      output_file = ''
      err_file = ''
      
      # command, output_file, error_file
      if(len(command_list) > 0):
        command = command_list[0]
      if(len(command_list) > 1):
        output_file = command_list[1]
      if(len(command_list) > 2):
        err_file = command_list[2]

      #print("command set to " + command)
      #print(command_list)

      if command == 'stop':
        self.stop_request()

      elif command == 'prefix':
        self.prefix = output_file
        print("set the prefix to .. " + self.prefix)
        self.send_output("prefix set", payload, operation="PYTHON", response=True)

      elif command == 'CLOSE_ALL_LOGOUT<o>':
        # shut down the server
        self.stop_request()

      #elif command == 'core':
      #  exec('core_server=s
      #  print("set the core " + self.prefix)
      #  self.send_output("prefix set", payload, response=True)
      
      # need a way to handle stop message here
   
      # Need a way to push stdout as print here
      
   
      # If this is a python payload 
      elif payload['operation'] == 'PYTHON':
        is_exception = False
        print(f"Executing command {command.encode('utf-8')}")
        # old smss calls
        if command.endswith(".py") or command.startswith('smssutil'):
          try:
            output = eval(command, globals(), self.my_var)
          except Exception as e:
            try:
              exec(command, globals(), self.my_var)
              output = f"executed command {command.encode('utf-8')}"
            except Exception as e:
              print(e)
              output = str(e)
              is_exception = True
          print(f"executing file.. {command.encode('utf-8')}")
          output = str(output)
          self.send_output(output, payload, operation=payload["operation"], response=True, exception=is_exception)

        # all new
        else:
          # same trick - try to eval if it fails run as exec
          import contextlib
          globals()['core_server'] = self
          import semoss_console as console
          c = console.SemossConsole(socket_handler=self, payload=payload)
          with contextlib.redirect_stdout(c), contextlib.redirect_stderr(c):
            try:
              output = eval(command, globals(), self.my_var)
            except Exception as e:
              try:
                exec(command, globals(), self.my_var)
                output = f"executed command {command.encode('utf-8')}"
              except Exception as e:
                # user is probably trying to call a 'global' variable inside a function call
                # add all user defined variables to globals
                globals().update(self.my_var)

                # store the removal keys in case of assignment
                removal_keys = list(self.my_var.keys())
                try:
                  output = eval(command, globals(), self.my_var)
                except Exception as e:
                  try:
                    exec(command, globals(), self.my_var)
                    output = f"executed command {command.encode('utf-8')}"
                  except Exception as last_exec_error:
                    #output =  ''.join(tb.format_exception(None, e, e.__traceback__))
                    #error_message = tb.format_exception_only(type(last_exec_error), last_exec_error)
                    #output = "".join(error_message)
                    traceback = sys.exc_info()[2]                    
                    full_trace = ['Traceback (most recent call last):\n']
                    full_trace = full_trace + tb.format_tb(traceback)[1:] + tb.format_exception_only(type(last_exec_error), last_exec_error)
                    output = ''.join(full_trace)
                    is_exception = True
                    
                # remove all user defined variables to globals
                for key in removal_keys:
                  del globals()[key]
                
          output = str(output)
          self.send_output(output, payload, operation=payload["operation"], response=True, exception=is_exception)
      
      # this is when it is a response 
      elif payload['response']:
        print("In the response block")
        # this is a response coming back from a request from the java container
        if payload['epoc'] in self.monitors:
          # log this payload
          if self.log_file is not None:
            self.log_file.write(f"Payload Response {payload}")
            self.log_file.write("\n")
            self.log_file.flush()

          condition = self.monitors[payload['epoc']]
          self.monitors.update({payload['epoc']: payload})
          condition.acquire()
          condition.notifyAll()
          condition.release()
      
      else:
        output = f"This is a python only instance. Command {str(command).encode('utf-8')} is not supported"
        output = str(output)
        print(f"{str(command).encode('utf-8')} = {output}")
        #output = "Response.. " + data.decode("utf-8")
        self.send_output(output, payload, operation=payload["operation"], response=True, exception=True)
    except Exception as e:
      output = ''.join(tb.format_exception(None, e, e.__traceback__))
      payload = {
      "epoc": epoc,
      "ex": [output]
      }
      # there is a possibility this is a response from the previous  
      if epoc in self.monitors:
        condition = self.monitors[epoc]
        self.monitors.update({epoc: payload})
        condition.acquire()
        condition.notifyAll()
        condition.release()
      else:
        self.send_output(output, payload, response=True, exception=True)
      
  def send_output(self, output, orig_payload, operation = "STDOUT", response=False, interim=False, exception=False):
    # Do not write any prints here
    # since the console is captured it will go into recursion

    # Stdout = true, response = true = partial
    # interim = true are the parts

    # stdout = false, response = true <-- actual response

        
    #print("sending output " + output)
    # make it back into payload just for epoch
    # if this comes with prefix. it is part of the response
    if self.prefix != "" and str(output).startswith(self.prefix):
      output = output.replace(self.prefix, "")
      operation="STDOUT" #orig_payload["operation"]
      response=True
      interim = True

    if(str(output).endswith("D.O.N.E")):
      #print("Finishing execution")
      output = str(output).replace("D.O.N.E", "")
      interim = False

    payload = {
      "epoc": orig_payload['epoc'],
      "payload": [output],
      "response": response,
      "operation": operation,
      "interim": interim
    }
  
    if 'insightId' in orig_payload:
      payload.update({'insightId': orig_payload['insightId']})


    if exception:
      payload.update({"ex":output
                      #"payload":[None]
                      })

    output = json.dumps(payload)
    # write response back
    size = len(output)
    size_byte = size.to_bytes(4, 'big')
    ret_array = bytearray(size)
    # pack the size upfront 
    ret_array[0:4] = size_byte
    # pack the message next
    ret_array[4:] = output.encode('utf-8')
    
    if self.log_file is not None:
      self.log_file.write(f"{orig_payload} === {size}")
      self.log_file.write("\n")
      self.log_file.write(f"OUTPUT === {payload}")
      self.log_file.write(f"{list(orig_payload.keys())}")
      self.log_file.flush()
      if response and not interim:
        self.log_file.write("\n")

    
    # send it out
    self.request.sendall(ret_array)

  def send_request(self, payload):
    # Do not write any prints here
    # since the console is captured it will go into recursion
        
    #print("sending output " + output)
    # make it back into payload just for epoch
    # if this comes with prefix. it is part of the response 
    #local = threading.local()
    #orig_payload = local.payload
    
    #print(f"Original Payload {orig_payload}")
    #print(locals().keys())
    #print(globals().keys())
    
    output = json.dumps(payload)
    # write response back
    size = len(output)
    size_byte = size.to_bytes(4, 'big')
    ret_array = bytearray(size)
    # pack the size upfront 
    ret_array[0:4] = size_byte
    # pack the message next
    ret_array[4:] = output.encode('utf-8')
    
    if self.log_file is not None:
      self.log_file.write(f"REQUEST === {payload}")
      self.log_file.flush()
      self.log_file.write("\n")
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
      payload.update({'epoc':epoc})
      self.monitors.update(epoc, payload)
      condition.acquire()
      condition.notifyAll()
      condition.release()


      