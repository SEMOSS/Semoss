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
package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * This class is responsible for creating a {@code IModelEngine} class that is directly linked to a
 * python process. The corresponding python class should handle all method implementations. This
 * java class is simply mechanism to forward calls to the python process.
 */
public abstract class AbstractPythonModelEngine extends AbstractModelEngine {

  private static final Logger classLogger = LogManager.getLogger(AbstractPythonModelEngine.class);

  // python server
  protected String prefix = null;
  protected String workingDirectory;
  protected String workingDirectoryBasePath = null;

  protected PyTranslator pyTranslator = null;
  protected File cacheFolder;
  private ClientProcessWrapper cpw = null;

  protected String varName = null;

  // string substitute vars
  protected Map<String, String> vars = new HashMap<>();

  private Map<String, ArrayList<Map<String, Object>>> chatHistory = new Hashtable<>();

  @Override
  public void open(String smssFilePath) throws Exception {
    setSmssFilePath(smssFilePath);
    this.open(Utility.loadProperties(smssFilePath));
  }

  @Override
  public void open(Properties smssProp) throws Exception {
    super.open(smssProp);

    if (!this.smssProp.containsKey(Settings.VAR_NAME)) {
      String randomString = "v_" + Utility.getRandomString(6);
      this.varName = randomString;
      this.smssProp.put(Settings.VAR_NAME, randomString);
    } else {
      this.varName = this.smssProp.getProperty(Settings.VAR_NAME);
    }

    // vars for string substitution
    for (Object smssKey : this.smssProp.keySet()) {
      String key = smssKey.toString();
      this.vars.put(key, this.smssProp.getProperty(key));
    }
  }

  /**
   * Gets a PyTranslator instance
   *
   * @return A configured PyTranslator instance
   * @throws IllegalArgumentException if insight is null
   * @throws IllegalStateException if the engine is not properly initialized or connection fails
   */
  public PyTranslator getEnginePyTranslator() {
    try {
      this.checkSocketStatus();
      return this.pyTranslator;
    } catch (Exception e) {
      classLogger.error(
          Constants.STACKTRACE,
          "Failed to create PyTranslator for engine: "
              + SmssUtilities.getUniqueName(this.engineName, this.engineId));
      throw new IllegalStateException("Failed to get PyTranslator: " + e.getMessage(), e);
    }
  }

  /**
   * This method is responsible for starting the python process that is linked to this model engine.
   *
   * @param port The port number to use when creating the server/client connection.
   */
  protected synchronized void startServer(int port) {
    if (this.cpw != null
        && this.cpw.getSocketClient() != null
        && this.cpw.getSocketClient().isConnected()) {
      return;
    }
    if (this.workingDirectoryBasePath == null) {
      this.createCacheFolder();
    }

    // check if we have already created a process wrapper
    ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
    if (this.cpw != null) {
      this.cpw.shutdown(false);
    }

    String timeout = "30";
    if (this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
      timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
    }
    if (cpwToInit.getSocketClient() == null) {
      boolean debug = false;

      // pull the relevant values from the smss
      String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
      String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
      String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "INFO");
      String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
      String venvPath =
          venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;

      if (port < 0) {
        // port has not been forced
        if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
          try {
            port = Integer.parseInt(forcePort);
            debug = true;
          } catch (NumberFormatException e) {
            // ignore
            classLogger.warn("Model " + this.getEngineName() + " has an invalid FORCE_PORT value");
          }
        }
      }

      String serverDirectory = this.cacheFolder.getAbsolutePath();
      boolean nativePyServer =
          true; // it has to be -- don't change this unless you can send engine calls from python
      try {
        cpwToInit.createProcessAndClient(
            nativePyServer,
            null,
            port,
            venvPath,
            serverDirectory,
            customClassPath,
            debug,
            timeout,
            loggerLevel);
      } catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
        throw new IllegalArgumentException("Unable to connect to server for python model engine.");
      }
    } else if (!cpwToInit.getSocketClient().isConnected()) {
      cpwToInit.shutdown(false);
      try {
        cpwToInit.reconnect();
      } catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
        throw new IllegalArgumentException(
            "Failed to start TCP Server for Python Model Engine = " + this.getEngineName());
      }
    }

    // create the py translator
    Insight processInsight = new Insight();
    InsightStore.getInstance().put(processInsight);
    this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

    try {
      // execute all the basic commands
      String initCommands = this.smssProp.getProperty(Constants.INIT_MODEL_ENGINE);
      // break the commands seperated by ;
      String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
      // replace the Vars
      for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
        commands[commandIndex] = fillVars(commands[commandIndex]);
      }
      this.pyTranslator.runEmptyPy(commands);
      // for debugging...
      classLogger.info(
          "Initializing "
              + SmssUtilities.getUniqueName(this.engineName, this.engineId)
              + " python process with commands >>> "
              + String.join("\n", commands));

      // run a prefix command
      setPrefix(cpwToInit);

      // finally set the cpw in the class
      this.cpw = cpwToInit;
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      if (cpwToInit != null) {
        classLogger.warn(
            "Able to start the python process for the python model engine "
                + SmssUtilities.getUniqueName(this.engineName, this.engineId)
                + " but the start script failed.");
        cpwToInit.shutdown(false);
      }
      throw e;
    }
  }

  /** This method checks whether the socket client is instantiated and connected. */
  protected void checkSocketStatus() {
    if (this.cpw == null
        || this.cpw.getSocketClient() == null
        || !this.cpw.getSocketClient().isConnected()) {
      this.startServer(-1);
    }
  }

  /** */
  private void setPrefix(ClientProcessWrapper cpwToInit) {
    this.prefix = cpwToInit.getPrefix();
    PayloadStruct prefixPayload = new PayloadStruct();
    prefixPayload.payload = new String[] {"prefix", this.prefix};
    prefixPayload.operation = PayloadStruct.OPERATION.CMD;
    cpwToInit.getSocketClient().executeCommand(prefixPayload);
  }

  @Override
  public AskModelEngineResponse askCall(
      String question,
      Object fullPrompt,
      String context,
      Insight insight,
      Map<String, Object> parameters) {
    checkSocketStatus();

    boolean keepConvoHisotry = this.keepsConversationHistory();
    final String TRIPLE_QUOTE = "\"\"\"";

    StringBuilder callMaker = new StringBuilder(varName + ".ask(");
    if (fullPrompt != null) {
      callMaker.append(FULL_PROMPT).append("=").append(PyUtils.determineStringType(fullPrompt));
      if (context != null) {
        if (context.startsWith("\"")) {
          context = " " + context;
        }
        if (context.endsWith("\"")) {
          context = context + " ";
        }
        context = context.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
        callMaker
            .append(",")
            .append("context=")
            .append(TRIPLE_QUOTE)
            .append(context)
            .append(TRIPLE_QUOTE);
      }
    } else {
      if (question.startsWith("\"")) {
        question = " " + question;
      }
      if (question.endsWith("\"")) {
        question = question + " ";
      }
      question = question.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
      callMaker.append("question=").append(TRIPLE_QUOTE).append(question).append(TRIPLE_QUOTE);

      if (context != null) {
        if (context.startsWith("\"")) {
          context = " " + context;
        }
        if (context.endsWith("\"")) {
          context = context + " ";
        }
        context = context.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
        callMaker
            .append(",")
            .append("context=")
            .append(TRIPLE_QUOTE)
            .append(context)
            .append(TRIPLE_QUOTE);
      }

      // if we are doing message_json (new world playground chat)
      // we should ignore trying to add additional history
      // TODO: remove the entire chatHistory object from the python model entirely
      // otherwise we end up with 2 history= params being sent to the json
      if (!parameters.containsKey("message_json")) {
        if (parameters.containsKey("toolExecution")) {
          Map<String, Object> toolExecutionMap =
              (Map<String, Object>) parameters.get("toolExecution");
          if (chatHistory.containsKey(insight.getInsightId())) {
            chatHistory.get(insight.getInsightId()).add(toolExecutionMap);
          }
          parameters.remove("toolExecution");
        }

        String history =
            getConversationHistory(insight.getUserId(), insight.getInsightId(), keepConvoHisotry);
        if (history != null) {
          // could still be null if its the first question in the convo
          callMaker.append(",").append("history=").append(history);
        }
      }
    }

    if (parameters != null && !parameters.isEmpty()) {
      Iterator<String> paramKeys = parameters.keySet().iterator();
      while (paramKeys.hasNext()) {
        String key = paramKeys.next();
        Object value = parameters.get(key);
        callMaker.append(",").append(key).append("=").append(PyUtils.determineStringType(value));
      }
    }

    if (this.prefix != null) {
      callMaker.append(", prefix='").append(prefix).append("'");
    }

    callMaker.append(")");

    classLogger.debug("Running >>> " + callMaker.toString());

    Object output = pyTranslator.runDirectPy(callMaker.toString());
    AskModelEngineResponse response = null;
    try {
      response = AskModelEngineResponse.fromObject(output);
    } catch (Exception e) {
      classLogger.warn("Could not create response object from output = " + output);
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(e.getMessage());
    }

    if (keepConvoHisotry) {
      // IF ITS A tool call - then append adjust history
      Map<String, Object> inputMap = new HashMap<>();
      Map<String, Object> outputMap = new HashMap<>();

      inputMap.put(ROLE, "user");
      inputMap.put(MESSAGE_CONTENT, question);

      outputMap.put(ROLE, "assistant");

      // TODO: handle multiple tools being returned
      // TODO: handle multiple tools being returned
      // TODO: handle multiple tools being returned
      if (response.getMessageType().equalsIgnoreCase(AskModelEngineResponse.TOOL)) {
        AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse) response;
        // Create the tool call structure
        Map<String, Object> toolCall = new HashMap<>();
        toolCall.put(TYPE, "function");
        toolCall.put(ID, toolResponse.getToolCallId());

        Map<String, String> functionMap = new HashMap<>();
        functionMap.put(ARGUMENTS, toolResponse.getToolCallArgumentsAsString());
        functionMap.put(NAME, toolResponse.getToolCallName());

        toolCall.put(FUNCTION, functionMap);

        // Add tool call to output map
        outputMap.put(TOOL_CALLS, Arrays.asList(toolCall));
        outputMap.put(MESSAGE_CONTENT, ""); // Empty content for tool
      } else {
        // Regular response
        outputMap.put(MESSAGE_CONTENT, response.getStringResponse());
      }
      // Update chat history
      if (chatHistory.containsKey(insight.getInsightId())) {
        chatHistory.get(insight.getInsightId()).add(inputMap);
        chatHistory.get(insight.getInsightId()).add(outputMap);
      }
    }

    return response;
  }

  @Override
  public InstructModelEngineResponse instructCall(
      String task,
      String context,
      List<Map<String, Object>> projectData,
      Insight insight,
      Map<String, Object> parameters) {
    checkSocketStatus();

    final String TRIPLE_QUOTE = "\"\"\"";
    StringBuilder callMaker = new StringBuilder(varName + ".instruct(");

    if (task.startsWith("\"")) {
      task = " " + task;
    }
    if (task.endsWith("\"")) {
      task = task + " ";
    }
    task = task.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");

    callMaker.append("task=").append(TRIPLE_QUOTE).append(task).append(TRIPLE_QUOTE);
    if (context != null) {
      if (context.startsWith("\"")) {
        context = " " + context;
      }
      if (context.endsWith("\"")) {
        context = context + " ";
      }
      context = context.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
      callMaker
          .append(",")
          .append("context=")
          .append(TRIPLE_QUOTE)
          .append(context)
          .append(TRIPLE_QUOTE);
    }

    callMaker.append(",").append("projectData=").append(PyUtils.determineStringType(projectData));

    if (parameters != null) {
      Iterator<String> paramKeys = parameters.keySet().iterator();
      while (paramKeys.hasNext()) {
        String key = paramKeys.next();
        Object value = parameters.get(key);
        callMaker.append(",").append(key).append("=").append(PyUtils.determineStringType(value));
      }
    }

    if (this.prefix != null) {
      callMaker.append(", prefix='").append(prefix).append("'");
    }

    callMaker.append(")");
    classLogger.debug("Running >>>" + callMaker.toString());

    Object output = pyTranslator.runDirectPy(callMaker.toString());
    InstructModelEngineResponse response = null;
    try {
      response = InstructModelEngineResponse.fromObject(output);
    } catch (Exception e) {
      classLogger.warn("Could not create response object from output = " + output);
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(e.getMessage());
    }
    return response;
  }

  @Override
  protected EmbeddingsModelEngineResponse embeddingsCall(
      List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters) {
    checkSocketStatus();

    String pythonListAsString = PyUtils.determineStringType(stringsToEmbed);

    StringBuilder callMaker = new StringBuilder();
    callMaker.append(varName).append(".embeddings(strings_to_embed = ").append(pythonListAsString);

    if (this.prefix != null) {
      callMaker.append(", prefix='").append(this.prefix).append("'");
    }

    if (parameters != null && !parameters.isEmpty()) {
      Iterator<String> paramKeys = parameters.keySet().iterator();
      while (paramKeys.hasNext()) {
        String key = paramKeys.next();
        Object value = parameters.get(key);
        callMaker.append(",").append(key).append("=").append(PyUtils.determineStringType(value));
      }
    }

    callMaker.append(")");

    Object output = pyTranslator.runDirectPy(callMaker.toString());
    EmbeddingsModelEngineResponse response = null;
    try {
      response = EmbeddingsModelEngineResponse.fromObject(output);
    } catch (Exception e) {
      classLogger.warn("Could not create response object from output = " + output);
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(e.getMessage());
    }
    return response;
  }

  @Override
  protected EmbeddingsModelEngineResponse imageEmbeddingsCall(
      List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters) {
    checkSocketStatus();

    String pythonListAsString = PyUtils.determineStringType(imagesToEmbed);

    StringBuilder callMaker = new StringBuilder();
    callMaker
        .append(varName)
        .append(".image_embeddings(images_to_embed = ")
        .append(pythonListAsString);

    if (this.prefix != null) {
      callMaker.append(", prefix='").append(this.prefix).append("'");
    }

    if (parameters != null && !parameters.isEmpty()) {
      Iterator<String> paramKeys = parameters.keySet().iterator();
      while (paramKeys.hasNext()) {
        String key = paramKeys.next();
        Object value = parameters.get(key);
        callMaker.append(",").append(key).append("=").append(PyUtils.determineStringType(value));
      }
    }

    callMaker.append(")");

    Object output = pyTranslator.runDirectPy(callMaker.toString());
    EmbeddingsModelEngineResponse response = null;
    try {
      response = EmbeddingsModelEngineResponse.fromObject(output);
    } catch (Exception e) {
      classLogger.warn("Could not create response object from output = " + output);
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(e.getMessage());
    }
    return response;
  }

  @Override
  public void close() throws IOException {
    if (this.cpw != null) {
      this.cpw.shutdown(true);
    }
  }

  /** */
  private void createCacheFolder() {
    String engineId = this.getEngineId();

    if (engineId == null || engineId.isEmpty()) {
      engineId = "";
    }
    // create a generic folder
    this.workingDirectory = "MODEL_" + engineId + "_" + Utility.getRandomString(6);
    this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
    this.cacheFolder = new File(workingDirectoryBasePath);

    // make the folder if one does not exist
    if (!this.cacheFolder.exists()) {
      this.cacheFolder.mkdir();
    }
  }

  /**
   * @param input
   * @return
   */
  private String fillVars(String input) {
    StringSubstitutor sub = new StringSubstitutor(vars);
    String resolvedString = sub.replace(input);
    return resolvedString;
  }

  /**
   * @param insightId
   * @param userId
   * @return
   */
  protected String getConversationHistoryFromInferenceLogs(String insightId, String userId) {
    List<Map<String, Object>> convoHistoryFromDb =
        ModelInferenceLogsUtils.doRetrieveConversation(userId, insightId, "ASC");
    if (convoHistoryFromDb.size() > 0) {
      for (Map<String, Object> record : convoHistoryFromDb) {
        Object messageData = record.get("MESSAGE_DATA");
        Map<String, Object> mapHistory = new HashMap<String, Object>();
        if (record.get("MESSAGE_TYPE").equals(ModelEngineInferenceLogsWorker.RESPONSE)) {
          mapHistory.put(ROLE, "assistant");
          mapHistory.put(MESSAGE_CONTENT, messageData);
        } else {
          mapHistory.put(ROLE, "user");
          mapHistory.put(MESSAGE_CONTENT, messageData);
        }
        chatHistory.get(insightId).add(mapHistory);
      }
      ArrayList<Map<String, Object>> convoHistory = chatHistory.get(insightId);
      StringBuilder convoList = new StringBuilder("[");
      boolean isFirstElement = true;
      for (Map<String, Object> record : convoHistory) {
        if (!isFirstElement) {
          convoList.append(",");
        } else {
          isFirstElement = false;
        }
        Object priorContent = PyUtils.determineStringType(record);
        convoList.append(priorContent);
      }
      convoList.append("]");
      return convoList.toString();
    }
    return null;
  }

  /**
   * @param userId
   * @param insightId
   * @param keepConvoHisotry
   * @return
   */
  protected String getConversationHistory(
      String userId, String insightId, boolean keepConvoHisotry) {
    if (keepConvoHisotry) {
      if (chatHistory.containsKey(insightId)) {
        ArrayList<Map<String, Object>> convoHistory = chatHistory.get(insightId);
        StringBuilder convoList = new StringBuilder("[");
        boolean isFirstElement = true;
        for (Map<String, Object> record : convoHistory) {
          if (!isFirstElement) {
            convoList.append(",");
          } else {
            isFirstElement = false;
          }
          Object priorContent = PyUtils.determineStringType(record);
          convoList.append(priorContent);
        }
        convoList.append("]");
        return convoList.toString();
      } else {
        // we want to start a conversation
        ArrayList<Map<String, Object>> userNewChat = new ArrayList<Map<String, Object>>();
        chatHistory.put(insightId, userNewChat);

        String dbConversation = null;
        if (Utility.isModelInferenceLogsEnabled()) {
          dbConversation = getConversationHistoryFromInferenceLogs(insightId, userId);
        }

        return dbConversation;
      }
    }
    return null;
  }
}
