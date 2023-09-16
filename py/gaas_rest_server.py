import requests
import json

class RESTServer():
  
  da_server = None

  def __init__(self, access_key=None, secret_key=None, base=None):
    self.access_key = access_key
    self.secret_key = secret_key
    self.main_url = base
    self.login()
    self.cur_insight = None
    self.connected = True
    RESTServer.da_server = self
    self.monitors = {}
    
  def login(self):
    import base64
    combined = self.access_key + ":" + self.secret_key
    combined_enc = base64.b64encode(combined.encode('ascii'))
    headers = {'authorization': f"Basic {combined_enc.decode('ascii')}"}
    headers.update({'disableRedirect':'true'})
    #login url 
    api_url = "/auth/whoami"
    url = self.main_url + api_url
    self.r = requests.get(url, headers=headers)
    self.cookies = self.r.cookies
    
  
  def make_new_insight(self):
    if not self.connected:
      return "please login"
    pixel_payload = {}
    pixel_payload.update({'expression':'OpenEmptyInsight()'})
    api_url = "/engine/runPixel";
    json_output = requests.post(self.main_url + api_url, cookies = self.cookies, data=pixel_payload)
    json_output = json_output.content
    json_output = json.loads(json_output.decode('utf-8'))
    # need to catch exception
    insight_id = json_output['pixelReturn'][0]['output']['insightData']['insightID']
    return insight_id
  
  def run_pixel(self, payload=None, insight_id=None):
    # going to create an insight if insight not available
    if not self.connected:
      return "please login"
    if insight_id is None:
      insight_id = self.cur_insight
    
    # still null :(
    if insight_id is None:
      self.cur_insight = self.make_new_insight()
      insight_id = self.cur_insight

    print(f"Insight ID {insight_id}")
    pixel_payload = {}
    pixel_payload.update({'expression':payload})
    pixel_payload.update({'insightId':insight_id})

    api_url = "/engine/runPixel";
    json_output = requests.post(self.main_url + api_url, cookies = self.cookies, data=pixel_payload)
    json_output = json_output.text
    #print(f"JSON CONTENT Response {json_output}")    
    #json_output = json_output.decode('utf-8')
    json_output = json_output.replace("\\'","'") 
    #print(f"Str response... {json_output}")
    json_output = json.loads(json_output)

    #print(f"JSON Response {json_output}")
    #print("--")
    #print(f"{json_output['pixelReturn']}")
    #print("--")
    #print(f"{json_output['pixelReturn'][0]}")
    #print("--")
    #print(f"{json_output['pixelReturn'][0]['output']}")
    output = self.get_pixel_output(json_output)
    return output

    
  def get_pixel_output(self, payload=None):
    main_output = payload['pixelReturn'][0]['output']
    #print(type(main_output))
    if isinstance(main_output, list):
      #print("in the list function")
      output = main_output[0]['output']
    else:
      output = main_output
    return output
    
  def logout(self):
    api_url = "/logout/all"
    requests.get(self.main_url + api_url, cookies = self.cookies)
    cookies = None
    self.connected = False

  def send_request(self, input_payload):
    # this is a synchronous call
    # but I dont want to bother the server proxy and leave it as is
    epoc = input_payload['epoc']

    input_payload_message = json.dumps(input_payload)
    input_payload_message = input_payload_message.replace("\"", "'")
    # escape the quotes
    #input_payload_message = json.dumps(input_payload_message)
    # RemoteEngineRun
    func = "RemoteEngineRun(payload=\"" + input_payload_message + "\");"
    #print(f"Message sent.. {func}")
    output_payload_message = self.run_pixel(payload=func, insight_id=input_payload['insightId'])
    #print(output_payload_message)
    #output_payload_message = json.loads(output_payload_message)
    if epoc in self.monitors:
      condition = self.monitors[epoc]
      #payload.update({'epoc':epoc})
      self.monitors.update({epoc: output_payload_message})

    