//package prerna.test;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import prerna.auth.User;
//import prerna.auth.utils.SecurityEngineUtils;
//import prerna.engine.api.IModelEngine;
//import prerna.engine.impl.model.AbstractModelEngine;
//import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
//import prerna.engine.impl.model.responses.AskModelEngineResponse;
//import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
//import prerna.reactor.AbstractReactor;
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.util.Utility;
//
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import java.util.ArrayList;
//
//public class AddFunctionToolReactor extends AbstractReactor {
//
//    public AddFunctionToolReactor() {
//        this.keysToGet = new String[] { 
//                "description", 
//                "id",
//                "parameters"
//        };
//        this.keyRequired = new int[] { 1, 1, 1 };
//    }
//
//    private Map<String, Object> createParameter(String type, String description) {
//        Map<String, Object> param = new HashMap<>();
//        param.put("type", type);
//        param.put("description", description);
//        return param;
//    }
//
//    @Override
//    public NounMetadata execute() {
//        organizeKeys();
//        String id = this.keyValue.get(this.keysToGet[1]);
//        User user = this.insight.getUser();
//        // Role check if user has access to this tool (use id to check)
//
//        // Call AskToolReactor to get a response
//        AskToolReactor askToolReactor = new AskToolReactor();
//        NounMetadata askToolResponse = askToolReactor.execute();
//
//        // Parse the response from AskToolReactor into parameters to create a new vector database tool
//        processResponse(askToolResponse);
//        Map<String, Object> functionEngine = createFunctionEngineMap();
////	     
//        return new NounMetadata(functionEngine, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
//    }
//
//    // Parse askToolResponse into this JSON Format:
//    //    {
//    //        "function": {
//    //            "name": "function_engine",
//    //            "description": "Funtion ID: <FUNCTION ID>. <FUNCTION DESCRIPTION>.",
//    //            "parameters": {
//    //                "type": "object",
//    //                "properties": {
//    //                    "function_id": {
//    //                        "type": "string",
//    //                        "description": "The unique identifier for the function_engine provided in the description."
//    //                    },
//    //                    "map": {
//    //                        "type": "object",
//    //                        "properties": {
//    //                            "<PARAM 1 NAME>": {
//    //                                "type": "<PARAM 1 TYPE>",
//    //                                "description": "<PARAM 1 DESCRIPTION>"
//    //                            },
//    //                            "<PARAM 2 NAME>": {
//    //                                "type": "<PARAM 2 TYPE>",
//    //                                "description": "<PARAM 2 DESCRIPTION>"
//    //                            },
//    //                            ...
//    //                        },
//    //                        "required": [
//    //                            "<PARAM 1 NAME>",
//    //                            "<PARAM 2 NAME>",
//    //                            ...
//    //                        ],
//    //                        "description": "<PARAM OBJECT DESCRIPTION>"
//    //                    }
//    //                },
//    //                "required": [
//    //                    "function_id",
//    //                    "map"
//    //                ]
//    //            }
//    //        }
//    //    }
//
//    private Map<String, Object> createFunctionEngineMap() {
//        // Function => Parameters => Properties => MAP => Properties => Parameter
//        Map<String, Object> param1 = createParameter("<PARAM 1 TYPE>", "<PARAM 1 DESCRIPTION>");
//        Map<String, Object> param2 = createParameter("<PARAM 2 TYPE>", "<PARAM 2 DESCRIPTION>");
//
//        Map<String, Object> parametersProperties = new HashMap<>(); // DYNAMIC
//        parametersProperties.put("<PARAM 1 NAME>", param1);
//        parametersProperties.put("<PARAM 2 NAME>", param2);
//        // Add more parameters from the askToolResponse
//
//        // Level 5: Parameters=>Properties=>function_id
//        Map<String, Object> functionIdMap = new HashMap<>();
//        functionIdMap.put("type", "string");
//        functionIdMap.put("description", "The unique identifier for the function_engine provided in the description.");
//       
//        // Level 5: Parameters=>Properties=>MAP
//        Map<String, Object> propertiesMap = new HashMap<>();
//        propertiesMap.put("type", "object"); // value is DYNAMIC
//        propertiesMap.put("properties", parametersProperties); 
//        propertiesMap.put("required", new String[]{"<PARAM 1 NAME>", "<PARAM 2 NAME>"}); // Add more required parameters as needed
//        propertiesMap.put("description", "<PARAM OBJECT DESCRIPTION>");
//        
//        //Level 4: Parameters=>Properties
//        Map<String, Object> parameters = new HashMap<>();
//        parameters.put("function_id", functionIdMap);
//        parameters.put("map", propertiesMap);
//
//        // Level 3: Parameters=>Type, Parameters=>Properties, Parameters=>Required
//        Map<String, Object> functionParameters = new HashMap<>();
//        functionParameters.put("type", "object");
//        functionParameters.put("properties", parameters);
//        // Dynamically inputs a list of the other level 3 elements that are required
//        functionParameters.put("required", new String[]{"function_id", "map"});
//
//        // Level 2: name, description, and parameters
//        Map<String, Object> function = new HashMap<>();
//        function.put("name", "function_engine");
//        function.put("description", "Function ID: <FUNCTION ID>. <FUNCTION DESCRIPTION>.");
//        function.put("parameters", functionParameters);
//
//        // Level 1: function 
//        Map<String, Object> functionEngine = new HashMap<>();
//        functionEngine.put("function", function);
//
//        return functionEngine;
//    }
//
//    // Parse AskToolReactor response
//    private Map<String, Object> processResponse(NounMetadata response) {
//        Map<String, Object> output = response.toMap();
//        Object responseContent = output.get(AbstractModelEngineResponse.RESPONSE);
//
//        // Case 1: Simple string response
//        if (responseContent instanceof String) {
//            output.put("processedResponse", responseContent);
//        // Case 2: Complicated structured response
//        } else if (responseContent instanceof Map) {
//            // Execute the tool and get the response
//            String toolResponseContent = executeTool((Map<String, Object>) responseContent);
//            
//            // Add the tool response to the output
//            output.put("processedResponse", toolResponseContent);
//            
//            // Call the model with the tool response and without the tool object
//            output.remove("toolExecution");
//            IModelEngine modelEngine = Utility.getModel(this.keyValue.get(this.keysToGet[0]));
//            AskModelEngineResponse toolExecutionResponse = modelEngine.ask("", "", this.insight, output);
//            output.putAll(toolExecutionResponse.toMap());
//        } else {
//            output.put("processedResponse", "Unsupported response type");
//        }
//    }
//
//    // 
//    private String executeTool(Map<String, Object> toolResponseContent) {
//        // I
//
//        return "";
//    }
//   
//}
