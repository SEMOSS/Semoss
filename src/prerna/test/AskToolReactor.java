/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskToolReactor extends AbstractReactor {
  private static final Pattern MARKDOWN_CODE_PATTERN =
      Pattern.compile(
          "```"
              + // Opening backticks
              "(?:([a-zA-Z0-9]+))?"
              + // Language (optional, group 1)
              "(?:"
              + // Non-capturing group for title alternatives
              "\\s+title=\"([^\"]+)\""
              + // Either title="filename" (group 2)
              "|\\s+([^\\s\\n]+)"
              + // Or direct filename (group 3)
              ")?"
              + // Title is optional
              "\\s*\\n"
              + // Whitespace and mandatory newline
              "(.*?)"
              + // Code content (group 4)
              "```", // Closing backticks
          Pattern.DOTALL);

  public AskToolReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.ENGINE.getKey(),
          ReactorKeysEnum.COMMAND.getKey(),
          ReactorKeysEnum.CONTEXT.getKey(),
          ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
          "engine_tools",
          "project_tools"
        };
    this.keyRequired = new int[] {1, 1, 0, 0, 0, 0};
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

    List<String> engineToolIDs = getEngineToolIDs();
    List<String> projectToolIDs = getProjectToolIDs();

    if (!engineToolIDs.isEmpty() || !projectToolIDs.isEmpty()) {

      // Check if the "tools_choice" key exists in the paramMap, else add it
      if (!paramMap.containsKey("tool_choice")) {
        paramMap.put("tool_choice", "auto");
      }

      List<Map<String, Object>> toolsList;

      // Check if the "tools" key exists in the paramMap
      // this is if a user has explicitly adding tools to the param map.
      if (paramMap.containsKey("tools")) {
        // Retrieve the existing list of tools
        toolsList = (List<Map<String, Object>>) paramMap.get("tools");
      } else {
        // Create a new list for tools
        toolsList = new ArrayList<Map<String, Object>>();
        paramMap.put("tools", toolsList);
      }

      // Iterate over each engine ID and add the tool to the tools list
      for (String engineToolID : engineToolIDs) {
        // TODO add a safety check here for function engines only
        IFunctionEngine function = Utility.getFunctionEngine(engineToolID);
        Map<String, Object> functionToolMap = function.buildFunctionEngineToolMap();
        toolsList.add(functionToolMap);
      }

      // Iterate over each project ID and add the tool to the tools list
      for (String projectToolID : projectToolIDs) {
        // TODO add a safety check here for code projects only
        IProject project = Utility.getProject(projectToolID);
        Map<String, Object> projectToolMap = project.buildProjectToolMap();
        toolsList.add(projectToolMap);
      }
    }

    AskModelEngineResponse modelResponse =
        modelEngine.ask(question, context, this.insight, paramMap);

    Map<String, ArrayList<Map<String, Object>>> output = processModelResponse(modelResponse);

    return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
  }

  private Map<String, ArrayList<Map<String, Object>>> processModelResponse(
      AskModelEngineResponse modelResponse) {
    Map<String, ArrayList<Map<String, Object>>> output =
        new HashMap<String, ArrayList<Map<String, Object>>>();
    output.put("response", new ArrayList<Map<String, Object>>());
    if (modelResponse.getMessageType().equalsIgnoreCase(AskModelEngineResponse.TOOL)) {
      // the response is for a tool call
      // we need to call the actual tool now.
      AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse) modelResponse;

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

      Map<String, Object> outputObject = new HashMap<String, Object>();
      String toolName;
      String toolType;

      if (toolResponse.getResponse().get(0).get("name").equals("project_engine")) {
        IProject project = Utility.getProject((String) functionParams.get("id"));
        toolName = project.getProjectName();
        toolType = "PROJECT";
      } else {
        IFunctionEngine function = Utility.getFunctionEngine((String) functionParams.get("id"));
        toolName = function.getEngineName();
        toolType = "FUNCTION";
      }

      // object to store params needed to call the tool
      List<HashMap<String, Object>> toolCallInfoData = new ArrayList<HashMap<String, Object>>();
      for (Entry<String, Object> functionParam :
          ((Map<String, Object>) functionParams.get("map")).entrySet()) {
        HashMap<String, Object> paramInfo = new HashMap<String, Object>();
        paramInfo.put("name", functionParam.getKey());
        paramInfo.put("type", functionParam.getValue().getClass().getSimpleName());
        paramInfo.put("value", functionParam.getValue());
        toolCallInfoData.add(paramInfo);
      }

      outputObject.put("type", toolType);
      outputObject.put("name", toolName);
      outputObject.put("id", (String) functionParams.get("id"));
      outputObject.put("parameters", toolCallInfoData);

      output.get("response").add(outputObject);

      // remove the execution of the function for now. will add back later with a boolean passed in
      //            Object functionReturn = function.execute((Map<String, Object>
      // )functionParams.get("map"));
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
      //            AskModelEngineResponse toolExecutionResponse = modelEngine.ask("", null,
      // this.insight, paramMap);
      //            output = toolExecutionResponse.toMap();
    } else {
      // 	this is a standard response - process it for code blocks.

      // Process the response to extract code blocks and replace with UUID references
      ProcessedResponse processedResponse =
          processMarkdownCodeBlocks(modelResponse.getStringResponse());

      // Add code blocks to output if any exist
      if (!processedResponse.getCodeBlocks().isEmpty()) {
        String[] splitResponse =
            processedResponse.getModifiedResponse().split("<CODEBLOCK>.*<\\/CODEBLOCK>");

        for (int i = 0; i < splitResponse.length; i++) {
          Map<String, Object> outputObject = new HashMap<String, Object>();
          outputObject.put("type", "CONTENT");
          outputObject.put("content", splitResponse[i]);
          output.get("response").add(outputObject);
          if (i < processedResponse.getCodeBlocks().values().toArray().length) {
            CodeBlock codeBlock =
                (CodeBlock) processedResponse.getCodeBlocks().values().toArray()[i];
            HashMap<String, Object> paramInfo = new HashMap<String, Object>();
            paramInfo.put("type", "CODE");
            paramInfo.put("language", codeBlock.getLanguage());
            paramInfo.put("name", codeBlock.getTitle());
            paramInfo.put("content", codeBlock.getCode());
            output.get("response").add(paramInfo);
          }
        }

        Map<String, Object> outputObject = new HashMap<String, Object>();
        outputObject.put("originalResponse", modelResponse.getStringResponse());
        output.get("response").add(outputObject);
      } else {
        Map<String, Object> outputObject = new HashMap<String, Object>();
        outputObject.put("type", "CONTENT");
        outputObject.put("content", modelResponse.getStringResponse());
        output.get("response").add(outputObject);
      }
    }
    return output;
  }

  // Method to parse markdown code blocks
  private ProcessedResponse processMarkdownCodeBlocks(String response) {
    Map<String, CodeBlock> codeBlocks = new HashMap<>();
    Matcher matcher = MARKDOWN_CODE_PATTERN.matcher(response);
    StringBuffer modifiedResponse = new StringBuffer();

    while (matcher.find()) {
      String language = matcher.group(1) != null ? matcher.group(1).trim() : "";
      // Check both title formats and use the first non-null one
      String title =
          matcher.group(2) != null
              ? matcher.group(2).trim()
              : matcher.group(3) != null ? matcher.group(3).trim() : "";
      String code = matcher.group(4).trim();

      String uuid = UUID.randomUUID().toString();
      codeBlocks.put(uuid, new CodeBlock(language, code, title));

      matcher.appendReplacement(
          modifiedResponse, Matcher.quoteReplacement("<CODEBLOCK>" + uuid + "</CODEBLOCK>"));
    }
    matcher.appendTail(modifiedResponse);

    return new ProcessedResponse(modifiedResponse.toString(), codeBlocks);
  }

  /**
   * @return list of engines
   */
  public List<String> getEngineToolIDs() {
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

  public List<String> getProjectToolIDs() {
    List<String> inputStrings = new ArrayList<>();

    // see if added as key
    GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
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

  // Helper class to represent the processed response
  private static class ProcessedResponse {
    private final String modifiedResponse;
    private final Map<String, CodeBlock> codeBlocks;

    public ProcessedResponse(String modifiedResponse, Map<String, CodeBlock> codeBlocks) {
      this.modifiedResponse = modifiedResponse;
      this.codeBlocks = codeBlocks;
    }

    public String getModifiedResponse() {
      return modifiedResponse;
    }

    public Map<String, CodeBlock> getCodeBlocks() {
      return codeBlocks;
    }
  }

  // Class to represent a code block
  private static class CodeBlock {
    private final String language;
    private final String code;
    private final String title;

    public CodeBlock(String language, String code, String title) {
      this.language = language;
      this.code = code;
      this.title = title;
    }

    public String getLanguage() {
      return language;
    }

    public String getCode() {
      return code;
    }

    public String getTitle() {
      return title;
    }
  }
}
