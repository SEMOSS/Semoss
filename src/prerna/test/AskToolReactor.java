package prerna.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.function.FunctionEngineToolShell;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.util.ArrayList;

public class AskToolReactor extends AbstractReactor {

    public AskToolReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
                ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey() ,"engine_tools"};
        this.keyRequired = new int[] { 1, 1, 0, 0 , 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
            throw new IllegalArgumentException(
                    "Model " + engineId + " does not exist or user does not have access to this model");
        }

        String question = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
        String context = this.keyValue.get(this.keysToGet[2]);
        if (context != null) {
            context = Utility.decodeURIComponent(context);
        }

        Map<String, Object> paramMap = getMap();
        IModelEngine modelEngine = Utility.getModel(engineId);
        if (paramMap == null) {
            paramMap = new HashMap<String, Object>();
        }

		List<String> engineIdForTools = getEngineIDs();
        if (!engineIdForTools.isEmpty()) {
        	
            // Check if the "tools_choice" key exists in the paramMap, else add it
            if (!paramMap.containsKey("tool_choice")) {
            	paramMap.put("tool_choice", "auto");                
            }
            
            // Check if the "tools" key exists in the paramMap
            List<Map<String, Object> > toolsList;
            if (paramMap.containsKey("tools")) {
                // Retrieve the existing list of tools
                toolsList = (List<Map<String, Object> >) paramMap.get("tools");
            } else {
                // Create a new list for tools
                toolsList = new ArrayList<Map<String, Object> >();
                paramMap.put("tools", toolsList);
            }

            // Iterate over each engine ID and add the function tool to the tools list
            
            //TODO this is hard checked for function engines - will need expand this out. 
            for (String toolEngineID : engineIdForTools) {
                IFunctionEngine function = Utility.getFunctionEngine(toolEngineID);
                Map<String, Object> functionTool = function.buildFunctionEngineToolMap();
                toolsList.add(functionTool);
            }
        }
		
        AskModelEngineResponse modelResponse = modelEngine.ask(question, context, this.insight, paramMap);

        Map<String, Object> output = modelResponse.toMap();

        if(modelResponse.getMessageType().equalsIgnoreCase(AskModelEngineResponse.TOOL)) {  	
            // the response is for a tool call
            // we need to call the actual tool now. 
            AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse) modelResponse;
            
            // tool result will be a custom element in the paramMap
            HashMap<String, String> toolExecutionMap = new HashMap<String, String>();
            toolExecutionMap.put(AbstractModelEngine.ROLE, "tool");
            toolExecutionMap.put("tool_call_id",toolResponse.getToolCallId());
            toolExecutionMap.put("name",toolResponse.getToolCallName());

            // {"function_id":"123-3345-567","map":{"lat":"123","lon":"321"}}
            String toolArguments = toolResponse.getToolCallArgumentsAsString();

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> functionParams = new HashMap<String, Object>();
            try {
                functionParams = mapper.readValue(toolArguments, Map.class);
            } catch (Exception e) {
                // Handle parsing error
                functionParams = null;
            }

            IFunctionEngine function = Utility.getFunctionEngine((String) functionParams.get("id"));
            
            // object for tool call information for the front end to execute the tool
            HashMap<String, Object> toolCallInfo = new HashMap<String, Object>();
            toolCallInfo.put("name", function.getFunctionName());
            toolCallInfo.put("type", function.getCatalogType());
            
            // object to store params needed to call the tool
            List<HashMap<String, Object>> toolCallInfoData = new ArrayList<HashMap<String, Object>>();
            for(Entry<String, Object> functionParam : ((Map<String, Object>)functionParams.get("map")).entrySet()){
                HashMap<String, Object> paramInfo = new HashMap<String, Object>();
                paramInfo.put("paramName", functionParam.getKey());
                paramInfo.put("paramType", functionParam.getValue().getClass().getSimpleName());
                paramInfo.put("paramValue", functionParam.getValue());
                toolCallInfoData.add(paramInfo);
            }

            toolCallInfo.put("data", toolCallInfoData);
            output.put("toolCall", toolCallInfo);
            
            //remove the execution of the function for now. will add back later with a boolean passed in
//            Object functionReturn = function.execute((Map<String, Object> )functionParams.get("map"));
//            String functionReturnString = null;
//
//            try {
//                functionReturnString = mapper.writeValueAsString(functionReturn);
//            } catch (JsonProcessingException e) {
//                // Handle the exception, maybe log it or return a default value
//                e.printStackTrace();
//                functionReturnString = "{}";
//            }
//
//            toolExecutionMap.put("content", functionReturnString);         
//            paramMap.put("toolExecution", toolExecutionMap);
//            AskModelEngineResponse toolExecutionResponse = modelEngine.ask("", null, this.insight, paramMap);
//            output = toolExecutionResponse.toMap();
        }

        
//        else {
        	//this is a standard response - process it for code blocks.
        	
            //TODO Alter the askTool logic flow, such that if there is a tool,
        	//return based on above. Otherwise, check if there is markdown. 
        	// If there is markdown, process it similar to AskRoom and return like below.
        	
//        	tool:{
//        		name: code_engine
//        		type: code_engine
//        		data:[{ 
//        			language: py,
//        			title: helloWorld.py,
//        			code: "print("hello world")",
//        		},
//        		]
//        	}

 //       }
        
        Object response = modelResponse.toMap().get(AbstractModelEngineResponse.RESPONSE);

        
        
        return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
    }
    
    

 // Method to parse markdown code blocks
    private List<CodeBlock> parseMarkdownCodeBlocks(String response) {
        List<CodeBlock> codeBlocks = new ArrayList<>();
        Pattern pattern = Pattern.compile("```(.*?)\n(.*?)```", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(response);

        while (matcher.find()) {
            String language = matcher.group(1).trim();
            String code = matcher.group(2).trim();
            codeBlocks.add(new CodeBlock(language, code));
        }

        return codeBlocks;
    }

	/**
	 * 
	 * @return list of engines 
	 */
	public List<String> getEngineIDs() {
		List<String> inputStrings = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				inputStrings.add(grs.get(i).toString());
			}
			return inputStrings;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			inputStrings.add(this.curRow.get(i).toString());
		}
		
		return inputStrings;
	}

    private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[3]);
        if (mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if (mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
    

    // Class to represent a code block
    private static class CodeBlock {
        private String language;
        private String code;

        public CodeBlock(String language, String code) {
            this.language = language;
            this.code = code;
        }

        public String getLanguage() {
            return language;
        }

        public String getCode() {
            return code;
        }
    }
}
