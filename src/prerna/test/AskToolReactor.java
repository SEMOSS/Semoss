package prerna.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;

public class AskToolReactor extends AbstractReactor {

    public AskToolReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
                ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
        this.keyRequired = new int[] { 1, 1, 0, 0 };
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

            IFunctionEngine function = Utility.getFunctionEngine((String) functionParams.get("function_id"));

            Object functionReturn = function.execute((Map<String, Object>)functionParams.get("map"));
            String functionReturnString = null;

            try {
                functionReturnString = mapper.writeValueAsString(functionReturn);
            } catch (JsonProcessingException e) {
                // Handle the exception, maybe log it or return a default value
                e.printStackTrace();
                functionReturnString = "{}";
            }

            toolExecutionMap.put("content", functionReturnString);

            paramMap.put("toolExecution", toolExecutionMap);
            // remove tools from paramMap since we have the tool result
            paramMap.remove("tools");
            paramMap.remove("tool_choice");
            AskModelEngineResponse toolExecutionResponse = modelEngine.ask(question, context, this.insight, paramMap);

            output = toolExecutionResponse.toMap();
        }
        
        Object response = output.get(AbstractModelEngineResponse.RESPONSE);
//        //add logic here for checking if output is a map or if its a string
//        
//        // Logic to check if the response is a String or a Map
//        if (response instanceof String) {
//            // If it's a string, simply return it
//            output.put("processedResponse", response);
//        } else if (response instanceof Map) {
//            // If it's a map, convert it to a string representation
//            output.put("processedResponse", ((AskToolModelEngineResponse)modelResponse).getStringResponse());
//            
//            //execute tool
//            
//            // get tool response 
//            
//            //feed tool response, back to the model.
//            
//        } else {
//            // Handle other types if necessary
//            output.put("processedResponse", "Unsupported response type");
//        }

//	     // Parse markdown code blocks
//	     List<CodeBlock> codeBlocks = parseMarkdownCodeBlocks(response);
//
//	 	//if codeBlocks is not empty
//	     if(codeBlocks.size()>0) {
//	    	 output.put("codeBlocks", codeBlocks);
//	     }
//	     
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
