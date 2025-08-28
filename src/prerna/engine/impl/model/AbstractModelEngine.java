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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractModelEngine extends AbstractEngine implements IModelEngine {

  private static final Logger classLogger = LogManager.getLogger(AbstractModelEngine.class);

  public static final String OPEN_AI_KEY = "OPEN_AI_KEY";
  public static final String AWS_SECRET_KEY = "AWS_SECRET_KEY";
  public static final String AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
  public static final String GCP_SERVICE_ACCOUNT_KEY = "GCP_SERVICE_ACCOUNT_KEY";

  public static final String MESSAGE_CONTENT = "content";
  public static final String ROLE = "role";
  public static final String TOOL_CALLS = "tool_calls";
  public static final String TYPE = "type";
  public static final String ID = "id";
  public static final String FUNCTION = "function";
  public static final String ARGUMENTS = "arguments";
  public static final String NAME = "name";
  // param keys
  public static final String FULL_PROMPT = "full_prompt";

  protected boolean keepConversationHistory = false;
  protected boolean keepInputOutput = false;
  protected boolean inferenceLogsEnbaled = Utility.isModelInferenceLogsEnabled();

  @Override
  public void open(Properties smssProp) throws Exception {
    super.open(smssProp);

    this.keepConversationHistory =
        Boolean.parseBoolean(this.smssProp.getProperty(Constants.KEEP_CONVERSATION_HISTORY));
    this.keepInputOutput =
        Boolean.parseBoolean(this.smssProp.getProperty(Constants.KEEP_INPUT_OUTPUT));

    if (this.smssProp.containsKey(Constants.KEEP_CONTEXT)) {
      boolean keepContext = Boolean.parseBoolean(this.smssProp.getProperty(Constants.KEEP_CONTEXT));
      this.keepConversationHistory = keepContext;
      this.keepInputOutput = keepContext;
    }
  }

  /**
   * This is an abstract method for the implementation class such that tracking occurs
   *
   * @param question
   * @param fullPrompt
   * @param context
   * @param insight
   * @param hyperParameters
   * @return
   */
  protected abstract AskModelEngineResponse askCall(
      String question,
      Object fullPrompt,
      String context,
      Insight insight,
      Map<String, Object> hyperParameters);

  @Override
  public AskModelEngineResponse askRoom(
      String question, String context, Room room, Map<String, Object> parameters) {
    /*
     * We will check if there are any restrictions for the user's current token usage
     * There might be a value set on the user-engine permission which takes priority
     * or if there is none
     * there might be a value set on the user for all their model engine usage
     */

    // do we have any usage restriction on the user
    Map<String, Object> userRestrictionMap =
        ModelUsageRestrictionUtility.getModelUsageRestriction(
            room.getInsight().getUser(), this.engineId);

    if (parameters == null) {
      parameters = new HashMap<String, Object>();
    }

    Object fullPrompt = parameters.remove(FULL_PROMPT);
    ZonedDateTime inputTime = ZonedDateTime.now();
    AskModelEngineResponse askModelResponse =
        askCall(question, fullPrompt, context, room.getInsight(), parameters);
    ZonedDateTime outputTime = ZonedDateTime.now();
    askModelResponse.setMessageId(UUID.randomUUID().toString());
    askModelResponse.setRoomId(room.getId());

    String insightId = room.getInsight().getInsightId();
    if (inferenceLogsEnbaled) {
      Thread inferenceRecorder =
          new Thread(
              new ModelEngineInferenceLogsWorker(
                  /*messageId*/ askModelResponse.getMessageId(),
                  /*messageMethod*/ "ask",
                  /*engine*/ this,
                  /*insightId*/ room.getInsight().getInsightId(),
                  /*projectContextId*/ room.getInsight().getContextProjectId(),
                  /*projectId*/ room.getInsight().getProjectId(),
                  /*user*/ room.getInsight().getUser(),
                  /*sessionId*/ ThreadStore.getSessionId(),
                  /*roomId*/ room.getId(),
                  /*context*/ context,
                  /*prompt*/ question,
                  /*fullPrompt*/ fullPrompt,
                  /*promptTokens*/ askModelResponse.getNumberOfTokensInPrompt(),
                  /*inputTime*/ inputTime,
                  /*response*/ askModelResponse.getStringResponse(),
                  /*responseTokens*/ askModelResponse.getNumberOfTokensInResponse(),
                  /*outputTime*/ outputTime));
      inferenceRecorder.start();
    }

    // update current usage based on this new request
    ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(
        userRestrictionMap, askModelResponse, inputTime, outputTime);

    return askModelResponse;
  }

  @Override
  public AskModelEngineResponse ask(
      String question, String context, Insight insight, Map<String, Object> parameters) {
    /*
     * We will check if there are any restrictions for the user's current token usage
     * There might be a value set on the user-engine permission which takes priority
     * or if there is none
     * there might be a value set on the user for all their model engine usage
     */

    // do we have any usage restriction on the user
    Map<String, Object> userRestrictionMap =
        ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(), this.engineId);

    if (parameters == null) {
      parameters = new HashMap<String, Object>();
    }

    Object fullPrompt = parameters.remove(FULL_PROMPT);
    ZonedDateTime inputTime = ZonedDateTime.now();
    AskModelEngineResponse askModelResponse =
        askCall(question, fullPrompt, context, insight, parameters);
    ZonedDateTime outputTime = ZonedDateTime.now();
    askModelResponse.setMessageId(UUID.randomUUID().toString());
    askModelResponse.setRoomId(insight.getInsightId());

    if (inferenceLogsEnbaled) {
      Thread inferenceRecorder =
          new Thread(
              new ModelEngineInferenceLogsWorker(
                  /*messageId*/ askModelResponse.getMessageId(),
                  /*messageMethod*/ "ask",
                  /*engine*/ this,
                  /*insightId*/ insight.getInsightId(),
                  /*projectContextId*/ insight.getContextProjectId(),
                  /*projectId*/ insight.getProjectId(),
                  /*user*/ insight.getUser(),
                  /*sessionId*/ ThreadStore.getSessionId(),
                  /*roomId*/ ThreadStore.getInsightId(),
                  /*context*/ context,
                  /*prompt*/ question,
                  /*fullPrompt*/ fullPrompt,
                  /*promptTokens*/ askModelResponse.getNumberOfTokensInPrompt(),
                  /*inputTime*/ inputTime,
                  /*response*/ askModelResponse.getStringResponse(),
                  /*responseTokens*/ askModelResponse.getNumberOfTokensInResponse(),
                  /*outputTime*/ outputTime));
      inferenceRecorder.start();
    }

    // update current usage based on this new request
    ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(
        userRestrictionMap, askModelResponse, inputTime, outputTime);

    return askModelResponse;
  }

  /**
   * This is an abstract method for the implementation class such that tracking occurs
   *
   * @param task
   * @param context
   * @param insight
   * @param hyperParameters
   * @return
   */
  protected abstract InstructModelEngineResponse instructCall(
      String task,
      String context,
      List<Map<String, Object>> projectData,
      Insight insight,
      Map<String, Object> hyperParameters);

  @Override
  public InstructModelEngineResponse instruct(
      String task,
      String context,
      List<Map<String, Object>> projectData,
      Insight insight,
      Map<String, Object> parameters) {
    // do we have any usage restriction on the user
    Map<String, Object> userRestrictionMap =
        ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(), this.engineId);

    if (parameters == null) {
      parameters = new HashMap<String, Object>();
    }

    ZonedDateTime inputTime = ZonedDateTime.now();
    InstructModelEngineResponse instructModelResponse =
        instructCall(task, context, projectData, insight, parameters);
    ZonedDateTime outputTime = ZonedDateTime.now();

    String messageId = UUID.randomUUID().toString();
    instructModelResponse.setMessageId(UUID.randomUUID().toString());
    instructModelResponse.setRoomId(insight.getInsightId());

    if (inferenceLogsEnbaled) {
      Gson gson = new GsonBuilder().disableHtmlEscaping().create();
      Thread inferenceRecorder =
          new Thread(
              new ModelEngineInferenceLogsWorker(
                  /*messageId*/ messageId,
                  /*messageMethod*/ "instruct",
                  /*engine*/ this,
                  /*insightId*/ insight.getInsightId(),
                  /*projectContextId*/ insight.getContextProjectId(),
                  /*projectId*/ insight.getProjectId(),
                  /*user*/ insight.getUser(),
                  /*sessionId*/ ThreadStore.getSessionId(),
                  /*roomId*/ ThreadStore.getInsightId(),
                  /*context*/ context,
                  /*prompt*/ null,
                  /*fullPrompt*/ task,
                  /*promptTokens*/ instructModelResponse.getNumberOfTokensInPrompt(),
                  /*inputTime*/ inputTime,
                  /*response*/ gson.toJson(instructModelResponse.getResponse()),
                  /*responseTokens*/ instructModelResponse.getNumberOfTokensInResponse(),
                  /*outputTime*/ outputTime));
      inferenceRecorder.start();
    }

    // update current usage based on this new request
    ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(
        userRestrictionMap, instructModelResponse, inputTime, outputTime);

    return instructModelResponse;
  }

  /**
   * This is an abstract method for the implementation class such that tracking occurs
   *
   * @param stringsToEmbed
   * @param insight
   * @param parameters
   * @return
   */
  protected abstract EmbeddingsModelEngineResponse embeddingsCall(
      List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters);

  @Override
  public EmbeddingsModelEngineResponse embeddings(
      List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters) {
    // do we have any usage restriction on the user
    Map<String, Object> userRestrictionMap =
        ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(), this.engineId);

    ZonedDateTime inputTime = ZonedDateTime.now();
    EmbeddingsModelEngineResponse embeddingsResponse =
        embeddingsCall(stringsToEmbed, insight, parameters);
    ZonedDateTime outputTime = ZonedDateTime.now();

    if (inferenceLogsEnbaled) {
      String messageId = UUID.randomUUID().toString();
      Thread inferenceRecorder =
          new Thread(
              new ModelEngineInferenceLogsWorker(
                  /*messageId*/ messageId,
                  /*messageMethod*/ "embeddings",
                  /*engine*/ this,
                  /*insightId*/ insight.getInsightId(),
                  /*projectContextId*/ insight.getContextProjectId(),
                  /*projectId*/ insight.getProjectId(),
                  /*user*/ insight.getUser(),
                  /*sessionId*/ ThreadStore.getSessionId(),
                  /*roomId*/ ThreadStore.getInsightId(),
                  /*context*/ null,
                  /*prompt*/ null,
                  /*fullPrompt*/ stringsToEmbed,
                  /*promptTokens*/ embeddingsResponse.getNumberOfTokensInPrompt(),
                  /*inputTime*/ inputTime,
                  /*response*/ "",
                  /*responseTokens*/ embeddingsResponse.getNumberOfTokensInResponse(),
                  /*outputTime*/ outputTime));
      inferenceRecorder.start();
    }

    // update current usage based on this new request
    ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(
        userRestrictionMap, embeddingsResponse, inputTime, outputTime);

    return embeddingsResponse;
  }

  /**
   * This is an abstract method for the implementation class such that tracking occurs
   *
   * @param stringsToEmbed
   * @param insight
   * @param parameters
   * @return
   */
  protected abstract EmbeddingsModelEngineResponse imageEmbeddingsCall(
      List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters);

  @Override
  public EmbeddingsModelEngineResponse imageEmbeddings(
      List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters) {
    Map<String, Object> userRestrictionMap =
        ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(), this.engineId);

    ZonedDateTime inputTime = ZonedDateTime.now();
    EmbeddingsModelEngineResponse embeddingsResponse =
        imageEmbeddingsCall(imagesToEmbed, insight, parameters);
    ZonedDateTime outputTime = ZonedDateTime.now();

    if (inferenceLogsEnbaled) {
      String messageId = UUID.randomUUID().toString();
      Thread inferenceRecorder =
          new Thread(
              new ModelEngineInferenceLogsWorker(
                  /*messageId*/ messageId,
                  /*messageMethod*/ "embeddings",
                  /*engine*/ this,
                  /*insightId*/ insight.getInsightId(),
                  /*projectContextId*/ insight.getContextProjectId(),
                  /*projectId*/ insight.getProjectId(),
                  /*user*/ insight.getUser(),
                  /*sessionId*/ ThreadStore.getSessionId(),
                  /*roomId*/ ThreadStore.getInsightId(),
                  /*context*/ null,
                  /*prompt*/ null,
                  /*fullPrompt*/ imagesToEmbed,
                  /*promptTokens*/ embeddingsResponse.getNumberOfTokensInPrompt(),
                  /*inputTime*/ inputTime,
                  /*response*/ "",
                  /*responseTokens*/ embeddingsResponse.getNumberOfTokensInResponse(),
                  /*outputTime*/ outputTime));
      inferenceRecorder.start();
    }

    // update current usage based on this new request
    ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(
        userRestrictionMap, embeddingsResponse, inputTime, outputTime);

    return embeddingsResponse;
  }

  @Override
  public Map<String, Object> buildOpenAIFunctionEngineToolMap() {
    throw new NotImplementedException("This method has not been implemented yet...");
  }

  @Override
  public Map<String, Object> buildBedrockToolSpec() {
    throw new NotImplementedException("This method has not been implemented yet...");
  }

  /**
   * @return
   */
  public boolean keepsConversationHistory() {
    return this.keepConversationHistory;
  }

  /**
   * @return
   */
  public boolean keepInputOutput() {
    return this.keepInputOutput;
  }

  @Override
  public IEngine.CATALOG_TYPE getCatalogType() {
    return IEngine.CATALOG_TYPE.MODEL;
  }

  @Override
  public String getCatalogSubType(Properties smssProp) {
    return this.getModelType().toString();
  }

  @Override
  public boolean holdsFileLocks() {
    return false;
  }
}
