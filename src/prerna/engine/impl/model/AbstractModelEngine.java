package prerna.engine.impl.model;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
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
	public static final String APPEND_FULL_PROMPT = "append_full_prompt";

	protected boolean keepConversationHistory = false;
	protected boolean inferenceLogsEnbaled = Utility.isModelInferenceLogsEnabled();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.keepConversationHistory = Boolean
				.parseBoolean(this.smssProp.getProperty(Constants.KEEP_CONVERSATION_HISTORY));
	}

	/**
	 * This is an abstract method for the implementation class such that tracking
	 * occurs
	 * 
	 * @param question
	 * @param fullPrompt
	 * @param context
	 * @param insight
	 * @param roomId
	 * @param hyperParameters
	 * @return
	 */
	protected abstract AskModelEngineResponse askCall(String question, Object fullPrompt, String context,
			Insight insight, String roomId, Map<String, Object> hyperParameters);

	@Override
	public AskModelEngineResponse askRoom(String question, Room room, AbstractMessage inputMessage,
			Map<String, Object> parameters) {
		/*
		 * We will check if there are any restrictions for the user's current token
		 * usage There might be a value set on the user-engine permission which takes
		 * priority or if there is none there might be a value set on the user for all
		 * their model engine usage
		 */

		// do we have any usage restriction on the user
		Map<String, Object> userRestrictionMap = ModelUsageRestrictionUtility
				.getModelUsageRestriction(room.getInsight().getUser(), this.engineId);

		if (parameters == null) {
			parameters = new HashMap<String, Object>();
		}

		String context = null;
		if (inputMessage instanceof InputMessage) {
			context = ((InputMessage) inputMessage).getSystemPrompt();
		}

		// if full prompt is being sent, convert the full prompt to a set of
		// AbstractMessages
		// then set the message_json to be the new abstractMessages
		Object fullPrompt = parameters.remove(FULL_PROMPT);
		if (fullPrompt != null) {
			List<AbstractMessage> messageList = MessageUtils.convertFullPrompt(fullPrompt, room, this);
			Object appendFullPrompt = parameters.remove(APPEND_FULL_PROMPT);
			if (appendFullPrompt != null && Boolean.parseBoolean(appendFullPrompt + "")) {
				room.getMessages().addAll(messageList);
				messageList = room.getMessages();
			} else {
				room.setMessages(messageList);
			}
			String messageJson = MessageUtils.toJsonArrayWithImageData(messageList);
			question = messageJson;
			parameters.put("message_json", messageJson);

			Object toolChoiceObj = parameters.get("tool_choice");
			if (toolChoiceObj != null) {
				Map<String, Object> mcpToolChoice = MessageUtils.toMCPToolChoice(toolChoiceObj);
				if (mcpToolChoice != null) {
					parameters.put("tool_choice", mcpToolChoice);
				}
			}

			Object toolsObj = parameters.get("tools");
			if (toolsObj instanceof List<?>) {
				boolean convertable = true;
				List<?> toolsListRaw = (List<?>) toolsObj;
				for (Object obj : toolsListRaw) {
					if (!(obj instanceof Map)) {
						convertable = false;
						break;
					}
				}
				if (convertable) {
					@SuppressWarnings("unchecked")
					List<Map<String, Object>> toolsList = (List<Map<String, Object>>) toolsObj;
					List<Map<String, Object>> mcpTools = MessageUtils.convertOpenAIToMCPTools(toolsList);
					if (mcpTools != null) {
						parameters.put("tools", mcpTools);
					}
				}
			}

		}

		ZonedDateTime inputTime = ZonedDateTime.now();
		AskModelEngineResponse askModelResponse = askCall(question, null, context, room.getInsight(), room.getId(),
				parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();
		
		if (AskModelEngineResponse.ERROR.equals(askModelResponse.getMessageType())) {
	        classLogger.warn("Model Engine returned an error for Room {}: {}", room.getId(), askModelResponse.getStringResponse());
	        
	        // We set the IDs so the frontend knows which request failed, 
	        // but we DO NOT add this to room.getMessages() or call the database.
	        askModelResponse.setMessageId(GUID.v7().toUUID().toString());
	        askModelResponse.setRoomId(room.getId());
	        
	        return askModelResponse; // Bypasses all history saving logic below
	    }
		
		askModelResponse.setMessageId(GUID.v7().toUUID().toString());
		askModelResponse.setRoomId(room.getId());

		String insightId = room.getInsight().getInsightId();
		String projectId = room.getInsight().getProjectId();
		// if the insight project id is null, check fi one exists on the room
		if (projectId == null) {
			projectId = room.getProjectId();
		}
		// @formatter:off
		if (inferenceLogsEnbaled) {
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/ inputMessage.getMessageId(), 
					/*transactionId*/askModelResponse.getMessageId(), 
					/*messageMethod*/"ask", 
					/*engine*/this,
					/*insightId*/room.getInsight().getInsightId(),
					/*projectContextId*/room.getInsight().getContextProjectId(),
					/*projectId*/room.getInsight().getProjectId(),
					/*user*/room.getInsight().getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/room.getId(),
					/*context*/context, 
					/*prompt*/question,
					/*fullPrompt*/fullPrompt,
					/*promptTokens*/askModelResponse.getNumberOfTokensInPrompt(),
					/*inputTime*/inputTime, 
					/*response*/askModelResponse.getStringResponse(),
					/*responseTokens*/askModelResponse.getNumberOfTokensInResponse(),
					/*outputTime*/outputTime
			));
			inferenceRecorder.start();
		}
		// @formatter:on

		// update current usage based on this new request
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(userRestrictionMap, askModelResponse, inputTime,
				outputTime);

		// save the output if its full prompt since it short circuits the room object.
		if (fullPrompt != null) {
			String roomName = null;
			// Check if first message is input and has a prompt, use it as room name
			if (!room.getMessages().isEmpty()) {
				AbstractMessage first = room.getMessages().get(0);
				if (first instanceof InputMessage) {
					String uiPrompt = ((InputMessage) first).getInputUIPrompt();
					if (uiPrompt != null && !uiPrompt.trim().isEmpty()) {
						roomName = uiPrompt.substring(0, Math.min(uiPrompt.length(), 100));
					}
				}
			}

			if (roomName != null && !roomName.trim().isEmpty()) {
				ModelInferenceLogsUtils.doSetNameForRoom(room.getInsight().getUser().getPrimaryLoginToken().getId(),
						room.getId(), roomName);
			}

			ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(askModelResponse).build();
			room.getMessages().add(response);

			// set transaction id for both pieces
			inputMessage.setTransactionId(response.getMessageId());
			inputMessage.setTokensInMessage(askModelResponse.getNumberOfTokensInPrompt());
			response.setTransactionId(response.getMessageId());

			// Create the assistant's response message and add to history
			response.setModel(this);
			response.setParentMessageId(inputMessage.getMessageId());
			response.setTokensInMessage(askModelResponse.getNumberOfTokensInResponse());

			ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(),
					room.getInsight().getUser().getPrimaryLoginToken().getId(),
					MessageUtils.toJsonArrayWithImageData(room.getMessages()));

		}

		return askModelResponse;
	}

	@Override
	@Deprecated
	public AskModelEngineResponse ask(String question, String context, Insight insight,
			Map<String, Object> parameters) {
		Room room = RoomUtils.createRoomIfNotExists(null, insight, this, question);
		InputMessage msg = InputMessage.builder(room).withSystemPrompt(context).withInputUIPrompt(question)
				.withInputPrompt(question).withModelType(this.getModelType()).withParamMap(parameters).build();
		ResponseMessage response = room.ask(msg, this);
		return response.getModelEngineResponse();
	}

	/**
	 * This is an abstract method for the implementation class such that tracking
	 * occurs
	 * 
	 * @param task
	 * @param context
	 * @param insight
	 * @param hyperParameters
	 * @return
	 */
	protected abstract InstructModelEngineResponse instructCall(String task, String context,
			List<Map<String, Object>> projectData, Insight insight, Map<String, Object> hyperParameters);

	@Override
	public InstructModelEngineResponse instruct(String task, String context, List<Map<String, Object>> projectData,
			Insight insight, Map<String, Object> parameters) {
		// do we have any usage restriction on the user
		Map<String, Object> userRestrictionMap = ModelUsageRestrictionUtility
				.getModelUsageRestriction(insight.getUser(), this.engineId);

		if (parameters == null) {
			parameters = new HashMap<String, Object>();
		}

		ZonedDateTime inputTime = ZonedDateTime.now();
		InstructModelEngineResponse instructModelResponse = instructCall(task, context, projectData, insight,
				parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();

		instructModelResponse.setMessageId(GUID.v7().toUUID().toString());
		instructModelResponse.setRoomId(insight.getInsightId());

		// @formatter:off
		if (inferenceLogsEnbaled) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/instructModelResponse.getMessageId(),
					/*transactionId*/instructModelResponse.getMessageId(), 
					/*messageMethod*/"instruct", 
					/*engine*/this, 
					/*insightId*/insight.getInsightId(),
					/*projectContextId*/insight.getContextProjectId(),
					/*projectId*/insight.getProjectId(),
					/*user*/insight.getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/ThreadStore.getInsightId(),
					/*context*/context,
					/*prompt*/null,
					/*fullPrompt*/task,
					/*promptTokens*/instructModelResponse.getNumberOfTokensInPrompt(),
					/*inputTime*/inputTime, 
					/*response*/gson.toJson(instructModelResponse.getResponse()),
					/*responseTokens*/instructModelResponse.getNumberOfTokensInResponse(),
					/*outputTime*/outputTime
			));
			inferenceRecorder.start();
		}
		// @formatter:on

		// update current usage based on this new request
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(userRestrictionMap, instructModelResponse,
				inputTime, outputTime);

		return instructModelResponse;
	}

	/**
	 * This is an abstract method for the implementation class such that tracking
	 * occurs
	 * 
	 * @param stringsToEmbed
	 * @param insight
	 * @param parameters
	 * @return
	 */
	protected abstract EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters);

	@Override
	public EmbeddingsModelEngineResponse embeddings(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		// do we have any usage restriction on the user
		Map<String, Object> userRestrictionMap = ModelUsageRestrictionUtility
				.getModelUsageRestriction(insight.getUser(), this.engineId);

		ZonedDateTime inputTime = ZonedDateTime.now();
		EmbeddingsModelEngineResponse embeddingsResponse = embeddingsCall(stringsToEmbed, insight, parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();

		// @formatter:off
		if (inferenceLogsEnbaled) {
			String messageId = GUID.v7().toUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/messageId,
					/*transactionId*/messageId, 
					/*messageMethod*/"embeddings", 
					/*engine*/this, 
					/*insightId*/insight.getInsightId(),
					/*projectContextId*/insight.getContextProjectId(),
					/*projectId*/insight.getProjectId(),
					/*user*/insight.getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/ThreadStore.getInsightId(),
					/*context*/null,
					/*prompt*/null,
					/*fullPrompt*/stringsToEmbed,
					/*promptTokens*/embeddingsResponse.getNumberOfTokensInPrompt(),
					/*inputTime*/inputTime, 
					/*response*/"",
					/*responseTokens*/embeddingsResponse.getNumberOfTokensInResponse(),
					/*outputTime*/outputTime
			));
			inferenceRecorder.start();
		}
		// @formatter:on

		// update current usage based on this new request
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(userRestrictionMap, embeddingsResponse, inputTime,
				outputTime);

		return embeddingsResponse;
	}

	/**
	 * This is an abstract method for the implementation class such that tracking
	 * occurs
	 * 
	 * @param stringsToEmbed
	 * @param insight
	 * @param parameters
	 * @return
	 */
	protected abstract EmbeddingsModelEngineResponse imageEmbeddingsCall(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters);

	@Override
	public EmbeddingsModelEngineResponse imageEmbeddings(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters) {
		Map<String, Object> userRestrictionMap = ModelUsageRestrictionUtility
				.getModelUsageRestriction(insight.getUser(), this.engineId);

		ZonedDateTime inputTime = ZonedDateTime.now();
		EmbeddingsModelEngineResponse embeddingsResponse = imageEmbeddingsCall(imagesToEmbed, insight, parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();

		// @formatter:off
		if (inferenceLogsEnbaled) {
			String messageId = GUID.v7().toUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/messageId,
					/*transactionId*/messageId, 
					/*messageMethod*/"embeddings", 
					/*engine*/this, 
					/*insightId*/insight.getInsightId(),
					/*projectContextId*/insight.getContextProjectId(),
					/*projectId*/insight.getProjectId(),
					/*user*/insight.getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/ThreadStore.getInsightId(),
					/*context*/null,
					/*prompt*/null,
					/*fullPrompt*/imagesToEmbed,
					/*promptTokens*/embeddingsResponse.getNumberOfTokensInPrompt(),
					/*inputTime*/inputTime, 
					/*response*/"",
					/*responseTokens*/embeddingsResponse.getNumberOfTokensInResponse(),
					/*outputTime*/outputTime
			));
			inferenceRecorder.start();
		}
		// @formatter:on

		// update current usage based on this new request
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(userRestrictionMap, embeddingsResponse, inputTime,
				outputTime);

		return embeddingsResponse;
	}

	/**
	 * 
	 * @return
	 */
	@Override
	public boolean keepsConversationHistory() {
		return this.keepConversationHistory;
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