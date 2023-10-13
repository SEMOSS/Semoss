package prerna.reactor.frame.gaas.chat;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class MooseChatReactor extends GaasBaseReactor {

	public MooseChatReactor()
	{
		this.keysToGet = new String[]{ReactorKeysEnum.COMMAND.getKey(), 
									  ReactorKeysEnum.PROJECT.getKey(), 
									  ReactorKeysEnum.MODEL.getKey(), 
									  Constants.MOOSE_ENDPOINT, 
									  ReactorKeysEnum.CONTEXT.getKey(), 
									  ReactorKeysEnum.CONTENT_LENGTH.getKey(),
									  ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1,0,0,0,0};
	}
	@Override
	public NounMetadata execute() 
	{
		// TODO Auto-generated method stub	
		organizeKeys();
		String model = DIHelper.getInstance().getProperty(Constants.MOOSE_MODEL);
		if(model == null || model.trim().isEmpty()) {
			model = "gpt_3";
		}
		
		// if you want to force a model you can
		if(keyValue.containsKey(keysToGet[2]))
			model = keyValue.get(keysToGet[2]);
		
		String context = "\"\"";
		if(keyValue.containsKey(keysToGet[4]))
			context = "\"" + keyValue.get(keysToGet[4]) + "\"";
			
		String projectId = getProjectId();
		Map output = new HashMap();
		
		String paramString = "";
		Map paramMap = processParamMap();
		if(paramMap != null)
			paramString = ", " + processMapToString(paramMap);
		
		int contentLength = 512;
		if(this.store.getNoun(keysToGet[5]) != null)
			contentLength = Integer.parseInt(this.store.getNoun(keysToGet[5]).get(0) + "");
		
		// commenting out for now
		projectId = null;

		if(projectId == null)
		{
			if(model.equalsIgnoreCase("alpaca"))
			{
				// create instruction and get response back
				String template = "Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request."
								   + "\\n\\n### Instruction:\\n"
								   + keyValue.get(keysToGet[0]) 
								   + "\\n\\n### Response:";
				
				String endpoint = DIHelper.getInstance().getProperty(Constants.MOOSE_ENDPOINT);
				if(endpoint == null || endpoint.trim().isEmpty()) {
					throw new IllegalArgumentException("Must define endpoint to run custom models");
				} 
				int maxTokens = template.length() + 80;
				String answer = (String)insight.getPyTranslator().runScript("smssutil.run_alpaca(\"" + template + "\", " + maxTokens + " ,\" "  + endpoint.trim() + "\")");
				
				output.put("answer", answer);
			}
			if(model.equalsIgnoreCase("guanaco"))
			{
				// create the client first
				
				// create instruction and get response back
				String template = keyValue.get(keysToGet[0]);				
				String endpoint = DIHelper.getInstance().getProperty(Constants.GUANACO_ENDPOINT);
				if(endpoint == null || endpoint.trim().isEmpty()) {
					throw new IllegalArgumentException("Must define endpoint to run custom models");
				} 
				insight.getPyTranslator().runScript("from text_generation import Client");
				String client_name = "client_" + Utility.getRandomString(6);
				insight.getPyTranslator().runScript(client_name + " = Client('" + endpoint + "', timeout=60)");
				
				int maxTokens = contentLength;
				String function = "smssutil.chat_guanaco";
				if(template.startsWith("code:"))
				{
					template = template.substring(template.indexOf("code:"));
					function = "smssutil.chat_guanaco_code";
				}
				String pyCommand = function + "(context= " + context + ", "
									+ "question=\"" + template + "\", "
									+ "max_new_tokens=" + maxTokens + ","
									+ "client=" + client_name
									+ paramString
									+ ")";
				//System.err.println("Command being Executed " + pyCommand);
				Object answer = insight.getPyTranslator().runScript(pyCommand);
				
				if(answer instanceof HashMap)
					answer = ((HashMap)answer).get("response");

				
				output.put("answer", answer);
			}
			else
			{
				output.put("answer", "GPT requires you to entire API Key");
			}
		}
		else
		{
			// do that faiss search on the project
			output.put("answer", "Project chat still needs to be implemented");
		}
		Map outputMap = new HashMap();
		outputMap.put("query",  keyValue.get(keysToGet[0]));
		outputMap.put("data", output);
		
		return new NounMetadata(outputMap, PixelDataType.MAP);
	}

}
