import socket
import sys
import socketserver
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
import re

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
    self.monitor = threading.Condition()
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
    self.orig_mount_points = {}
    self.cur_mount_points = {}
    self.cmd_monitor = threading.Condition()
    
    self.try_jp = False
    # experimental
    if self.try_jp:
      import jsonpickle as jp
      self.json = jp
    else:
      import json as json
      self.json = json
  
  def handle(self):
    while not self.stop:
      #print("listening")
      try:
        data = self.request.recv(4)
        size = int.from_bytes(data, "big")
        epoc_size = 20
        epoc = self.request.recv(epoc_size)
        epoc = epoc.decode('utf-8')
        #print(f"epoc {epoc} {epoc.decode('utf-8')}")
        data = self.request.recv(size)
        #print(f"process the data ---- {data.decode('utf-8')}")
        #payload = data.decode('utf-8')   
        if self.server.blocking:
          self.get_final_output(data, epoc)
        else:
          runner = threading.Thread(target=self.get_final_output, kwargs=({'data':data, 'epoc':epoc}))
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
    if self.log_file is not None:
      self.log_file.write(f"{data}")
    #print(data)
    payload = ""
    #payload = data
    # if this fails.. there is nothing you can do.. 
    # you just have to send the response back as error
    try:
      # if this fails.. no go 
      # but the receiver still needs to be informed so it doesnt stall
      payload = data
      if not self.try_jp:
        payload = data.decode('utf-8')
      payload = self.json.loads(payload)
      
      #print(f"PAYLOAD.. {payload}")
      # do payload manipulation here 
      #payload = json.loads(payload)
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

      if command == 'stop' and payload['operation'] == 'CMD':
        self.stop_request()
      
      # handle setting prefix
      elif command == 'prefix' and payload['operation'] == 'CMD':
        self.prefix = output_file
        print("set the prefix to .. " + self.prefix)
        self.send_output("prefix set", payload, operation="PYTHON", response=True)
      
      # handle log out
      elif command == 'CLOSE_ALL_LOGOUT<o>' and payload['operation'] == 'CMD':
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
        self.handle_python(payload, command)
      # this is when it is a response 
      elif payload['response']:
        self.handle_response(payload)
      # nothing to do here. Unfortunately this is a py instance so we cannot anything
      elif payload['operation'] == 'CMD':
        self.handle_shell(payload)
      else:
        output = f"This is a python only instance. Command {str(command).encode('utf-8')} is not supported"
        print(f"{str(command).encode('utf-8')} = {output}")
        #output = "Response.. " + data.decode("utf-8")
        self.send_output(output, payload, operation=payload["operation"], response=True, exception=True)
    except Exception as e:
      print(f"in the exception block  {epoc}")
      output = ''.join(tb.format_exception(None, e, e.__traceback__))
      payload = {
      "epoc": str(epoc),
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
        self.send_output(output, payload, operation="PYTHON", response=True, exception=True)
      
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
      "payload": [self.json.dumps(output, default=lambda obj:str(obj), allow_nan=True)],
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

    #print("sending payload back.. ")
    output = self.json.dumps(payload)
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
    
    output = self.json.dumps(payload)
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
      
      
  def handle_python(self, payload, command):
    is_exception = False
    print(f"Executing command {command.encode('utf-8')}")
    # old smss calls
    if command.endswith(".py") or command.startswith('smssutil'):
      try:
        output = eval(command, globals())
      except Exception as e:
        try:
          exec(command, globals())
          output = f"executed command {command.encode('utf-8')}"
        except Exception as e:
          print(e)
          output = str(e)
          is_exception = True
      print(f"executing file.. {command.encode('utf-8')}")
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
          output = eval(command, globals())
        except Exception as e:
          try:
            exec(command, globals())
            output = f"executed command {command.encode('utf-8')}"
          except Exception as e:
            try:
              output = eval(command, globals())
            except Exception as e:
              try:
                exec(command, globals())
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
            
      self.send_output(output, payload, operation=payload["operation"], response=True, exception=is_exception)
  
  def handle_response(self, payload):
    #print("In the response block")
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
      
  def handle_shell(self, payload):
    # get the method name
    try:
      # we can look at changing it to a lower point
      self.cmd_monitor.acquire()
      method_name = payload['methodName']
      print(f"insight id is {payload['insightId']}")
      mount_name, mount_dir = payload['insightId'].split("__", 1)
      if method_name == 'constructor':
        # set the mount point
        # execute this once so that you know it even exists
        cmd_payload = ["cd", mount_dir]
        if mount_name not in self.orig_mount_points:
          mount_dir = self.exec_cd(mount_name=mount_name, payload=cmd_payload, check=False)
          self.orig_mount_points.update({mount_name:mount_dir})
          self.cur_mount_points.update({mount_name:mount_dir})
        self.send_output(mount_dir, payload, operation=payload["operation"], response=True)

      if method_name == 'removeMount':
        # set the mount point
        # execute this once so that you know it even exists
        if mount_name not in self.orig_mount_points:
          self.orig_mount_points.pop(mount_name)
          self.cur_mount_points.pop(mount_name)
        self.send_output("Mount point removed", payload, operation=payload["operation"], response=True)

        #return "completed constructor"
      if method_name == 'executeCommand':
        # get the insight id
        # get the mount dir 
        # see what the command is and execute accordingly
        # need to see the process of cd etc. 
        cur_dir = self.get_cd(mount_name)
        commands = payload['payload'][0].split(" ")
        commands = [command for command in commands if len(command) > 0]
        command = commands[0]
        output = "Command not allowed"
        #mounts = 
        if command == 'cd' or command.startswith("cd"):
          output = self.exec_cd(mount_name=mount_name, payload=commands)
        elif command == 'dir' or command == 'ls':
          output = self.exec_dir(mount_name=mount_name, payload=commands)
        elif command == 'cp' or command == 'copy':
          output = self.exec_cp(mount_name=mount_name, payload=commands)
        elif command == 'mv' or command == 'move':
          output = self.exec_cp(mount_name=mount_name, payload=commands)
        elif command == 'git':
          output = self.exec_generic(mount_name=mount_name, payload=commands)
        elif command == 'mvn':
          output = self.exec_generic(mount_name=mount_name, payload=commands)
        elif command == 'rm' or command == 'del':
          # if commands has -r
          # get the third argument and try to see it can resolve to a directory
          # if so remove that
          dir_name = commands[1]
          #dir_name = self.exec_cd(mount_name = mount_name, payload=["cd", dir_name])
          #if not dir_name.startswith("Sorry"):
          output = self.exec_generic(mount_name=mount_name, payload=commands)
          #else:
          #  output = dir_name
        elif command == 'pwd':
          output = self.exec_generic(mount_name=mount_name, payload=commands)
        elif command == 'deltree':
          output = self.exec_generic(mount_name=mount_name, payload=commands)
        elif command == 'mkdir':
          dir_name = commands[1]
          dir_name = self.exec_cd(mount_name = mount_name, payload=["cd", dir_name])
          if not dir_name.startswith("Sorry"):
            output = self.exec_generic(mount_name=mount_name, payload=commands)
          else:
            output = dir_name
        elif command == 'pnpm':
          output = self.exec_generic(mount_name=mount_name, payload=commands)
        else:
          output = "Commands allowed cd, dir, ls, copy, cp, mv, move, del <specific file>, rm <specific file>, deltree, pwd, git, mvn (Experimental), mkdir, pnpm(Experimental)"

        # replace the mount point / hide it
        output = output.replace("\\", "/")
        orig_dir = self.orig_mount_points[mount_name]
        orig_dir_opt1 = orig_dir.replace("\\","/")
        insensitive_orig_dir = re.compile(re.escape(orig_dir), re.IGNORECASE)
        output = insensitive_orig_dir.sub('_', output)
        insensitive_orig_dir = re.compile(re.escape(orig_dir_opt1), re.IGNORECASE)
        output = insensitive_orig_dir.sub('_', output)

        # send the output
        self.send_output(output, payload, operation=payload["operation"], response=True)
        #if command == 'ls' or command == 'dir':
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
    if len(payload) == 1: # this is the case of cd.. or something
      payload.append(payload[0].replace("cd",""))

    if check:
      cur_mount_dir = self.get_cd(mount_name)
      orig_mount_dir = self.orig_mount_points[mount_name] 
      appender = payload[1]
      cur_mount_dir = cur_mount_dir + "/" + appender
    else: # do it for the first time
      cur_mount_dir = payload[1]
    import subprocess
    # throw the exception
    #try:
    proc = subprocess.Popen(['pwd'], cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE)
    new_dir = proc.stdout.read().decode('utf-8').replace("\r\n", "").replace("\n", "")
    if check and new_dir.startswith(orig_mount_dir): # we are in the scope all is set
      print("updating mount points")
      self.cur_mount_points.update({mount_name:new_dir})
    elif not new_dir.startswith(orig_mount_dir):
      new_dir = "Sorry, you are trying to cd outside of the mount sandbox which is not allowed"
    return new_dir
    #except NotADirectoryError:
    #  raise Exception

  def exec_dir(self, mount_name=None, payload=None):
    import subprocess
    # there is only 2 arguments I need to accomodate for
    # cd <space>
    # ideally we should just append it to the cd call it a day
    cur_mount_dir = self.get_cd(mount_name)

    # throw the exception
    # need to accomodate for secondary arguments like ls - ls etc. - done
    proc = subprocess.Popen(payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE)
    output = proc.stdout.read().decode('utf-8')
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
    proc = subprocess.Popen(payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE)
    output = proc.stdout.read().decode('utf-8').replace("\r\n", "").replace("\n", "")
    return output
    
  def exec_generic(self, mount_name=None, payload=None):
    import subprocess
    cur_mount_dir = self.get_cd(mount_name)
    orig_mount_dir = self.orig_mount_points[mount_name]
    # execute git
    proc = subprocess.Popen(payload, cwd=cur_mount_dir, shell=True, stdout=subprocess.PIPE)
    output = proc.stdout.read().decode('utf-8')
    return output


      