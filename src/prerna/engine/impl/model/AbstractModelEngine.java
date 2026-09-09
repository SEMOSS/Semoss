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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelModalityEnum;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossModelEngineException;
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
	public static final String CONTEXT_WINDOW = "context_window";
	public static final String BUILT_IN_TOOLS = "built_in_tools";
	public static final String MAX_TOKENS = "max_tokens";
	public static final String THINKING = "thinking";
	public static final String EFFORT = "effort";
	public static final String TEMPERATURE = "temperature";
	public static final String THINKING_BUDGET = "thinking_budget";

	// the init script loading tells us the provider we are using
	// but we also want to know what the model brand actually is
	public static final String MODEL_BRAND = "MODEL_BRAND";

	protected boolean keepConversationHistory = false;
	protected int contextWindow = 0;
	protected boolean inferenceLogsEnbaled = Utility.isModelInferenceLogsEnabled();

	/**
	 * The engine's saved built-in tool selection from MODELMETADATA - a JSON object
	 * keyed by tool name. The security database is its only source of truth (the
	 * value is deliberately kept out of the smss), and it rides along on ask calls
	 * as the built_in_tools param unless the caller supplies their own.
	 */
	protected Object builtinTools = null;

	/**
	 * The max output tokens to request when the caller does not name one - the smss
	 * value when defined, otherwise the MODELMETADATA row's maxOutputTokens. Null
	 * when neither names one, in which case the python clients fall back to the
	 * model's own output cap.
	 */
	protected Long maxTokens = null;

	/**
	 * Whether the model is capable of thinking/reasoning per the MODELMETADATA row.
	 * Null when the table does not say either way - only an explicit false blocks a
	 * caller from requesting thinking.
	 */
	protected Boolean reasoning = null;

	/**
	 * The model's reasoning config from MODELMETADATA - the catalog object holding
	 * default_enabled, default_effort, mandatory and supported_efforts. Null when
	 * the table does not define one, in which case caller-supplied efforts are
	 * passed through unchecked.
	 */
	protected Map<String, Object> reasoningConfig = null;

	/**
	 * Whether the model accepts a temperature per the MODELMETADATA row. Null when
	 * the table does not say either way - only an explicit false makes us drop a
	 * caller supplied temperature.
	 */
	protected Boolean temperatureSupported = null;

	/**
	 * Input modalities configured in MODELMETADATA, overridable per engine by an
	 * INPUT_MODALITIES smss property (an explicitly blank smss value disables
	 * enforcement). Null means request content is not restricted.
	 */
	protected Set<ModelModalityEnum> inputModalities = null;

	protected Double inputTokenCredit = null;
	protected Double outputTokenCredit = null;
	protected Double cacheReadMultiplier = null;
	protected Double cacheWriteMultiplier = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		// backfill runtime settings from the MODELMETADATA table into the working
		// smss properties - a value defined in the smss file always wins
		fillModelSettingsFromMetadata();
		applySmssInputModalitiesOverride();

		this.keepConversationHistory = Boolean
				.parseBoolean(this.smssProp.getProperty(Constants.KEEP_CONVERSATION_HISTORY));
		String contextWindowStr = this.smssProp.getProperty(Constants.CONTEXT_WINDOW);
		this.contextWindow = contextWindowStr != null && !contextWindowStr.trim().isEmpty()
				? Integer.parseInt(contextWindowStr.trim())
				: 0;
		this.maxTokens = resolveMaxTokens();
	}

	/**
	 * The effective max output tokens after the metadata merge: the smss file wins
	 * under either key casing, then the metadata backfill placed under the
	 * lowercase param key. A value that does not parse reads as unset rather than
	 * failing engine open.
	 */
	private Long resolveMaxTokens() {
		String value = this.smssProp.getProperty(Constants.MAX_TOKENS);
		if (value == null || value.trim().isEmpty()) {
			value = this.smssProp.getProperty(MAX_TOKENS);
		}
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			classLogger.warn("Model {} has an invalid max tokens value '{}' - ignoring it", this.engineId, value);
			return null;
		}
	}

	/**
	 * Query the MODELMETADATA table once on engine open and fill in any model
	 * settings that are not defined in the smss file. Downstream consumers (the
	 * INIT_MODEL_ENGINE var substitution, getSmssProp callers, getContextWindow)
	 * all read from the merged smssProp so they do not need to know about the
	 * table. The original file contents remain untouched in origSmssProp.
	 */
	private void fillModelSettingsFromMetadata() {
		if (this.engineId == null || this.engineId.trim().isEmpty()) {
			return;
		}

		Map<String, Object> metadata = null;
		try {
			metadata = SecurityModelMetadataUtils.getModelMetadata(this.engineId);
		} catch (Exception e) {
			classLogger.warn("Unable to load model metadata for engine {} - using smss values only", this.engineId, e);
			return;
		}
		if (metadata == null) {
			return;
		}

		fillIfMissing("context_window", metadata.get("contextWindow"));
		fillIfMissing("max_tokens", metadata.get("maxOutputTokens"));
		this.builtinTools = metadata.get("builtinTools");
		this.inputModalities = toModalitySet(metadata.get("inputModalities"));

		if (metadata.get("reasoning") instanceof Boolean) {
			this.reasoning = (Boolean) metadata.get("reasoning");
		}
		if (metadata.get("temperature") instanceof Boolean) {
			this.temperatureSupported = (Boolean) metadata.get("temperature");
		}
		if (metadata.get("reasoningConfig") instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> config = (Map<String, Object>) metadata.get("reasoningConfig");
			this.reasoningConfig = config.isEmpty() ? null : config;
		}
		this.inputTokenCredit = (Double) metadata.get("inputTokenCredit");
		this.outputTokenCredit = (Double) metadata.get("outputTokenCredit");
		this.cacheReadMultiplier = (Double) metadata.get("cacheReadMultiplier");
		this.cacheWriteMultiplier = (Double) metadata.get("cacheWriteMultiplier");
	}

	public Double getInputTokenCredit() { return inputTokenCredit; }
	public Double getOutputTokenCredit() { return outputTokenCredit; }
	public Double getCacheReadMultiplier() { return cacheReadMultiplier; }
	public Double getCacheWriteMultiplier() { return cacheWriteMultiplier; }

	/**
	 * The smss file wins over the MODELMETADATA row for input modalities, same as
	 * the other metadata-backed settings. An INPUT_MODALITIES property that is
	 * present but blank disables enforcement entirely - the per-engine escape hatch
	 * when stored metadata is wrong.
	 */
	private void applySmssInputModalitiesOverride() {
		String smssModalities = this.smssProp.getProperty(Constants.INPUT_MODALITIES);
		if (smssModalities == null) {
			return;
		}
		this.inputModalities = smssModalities.isBlank() ? null
				: toModalitySet(Arrays.asList(smssModalities.split(",")));
	}

	/**
	 * Normalize a stored modality list. Unrecognized values are logged and skipped
	 * rather than thrown - a dirty metadata row must not stop the engine from
	 * opening (strict validation happens at the write path).
	 */
	private Set<ModelModalityEnum> toModalitySet(Object value) {
		if (!(value instanceof Collection<?>)) {
			return null;
		}
		Set<ModelModalityEnum> modalities = EnumSet.noneOf(ModelModalityEnum.class);
		for (Object modality : (Collection<?>) value) {
			if (modality == null || modality.toString().isBlank()) {
				continue;
			}
			try {
				modalities.add(ModelModalityEnum.fromName(modality.toString()));
			} catch (IllegalArgumentException e) {
				classLogger.warn("Model {} has an unrecognized input modality '{}' - ignoring it", this.engineId,
						modality);
			}
		}
		return modalities.isEmpty() ? null : modalities;
	}

	/**
	 * Set the smss property to the metadata value only when the smss file does not
	 * already define a non-empty value for the key.
	 */
	private void fillIfMissing(String smssKey, Object metadataValue) {
		if (metadataValue == null) {
			return;
		}
		String current = this.smssProp.getProperty(smssKey);
		if (current != null && !current.trim().isEmpty()) {
			return;
		}
		this.smssProp.put(smssKey, String.valueOf(metadataValue));
	}

	/**
	 * Resolve the thinking params for an ask call against the model's reasoning
	 * metadata. Caller-supplied values win, but a caller cannot turn thinking on
	 * when the metadata marks reasoning false, and a named effort must sit inside
	 * the config's supported_efforts list. When the caller says nothing, the
	 * config's defaults (default_enabled/mandatory and default_effort) ride along
	 * instead. Without a reasoning config the caller's effort is passed through
	 * unchecked.
	 *
	 * @return the parameters map, created when metadata defaults need a home
	 */
	protected Map<String, Object> applyReasoningParameters(Map<String, Object> parameters) {
		boolean callerSetThinking = parameters != null && parameters.containsKey(THINKING);
		boolean callerSetEffort = parameters != null
				&& (parameters.containsKey(EFFORT) || parameters.containsKey(THINKING_BUDGET));

		boolean thinkingOn;
		if (callerSetThinking) {
			thinkingOn = isThinkingRequested(parameters.get(THINKING));
			if (thinkingOn && Boolean.FALSE.equals(this.reasoning)) {
				throw new IllegalArgumentException(
						"Thinking was requested but this model does not support thinking/reasoning");
			}
		} else {
			thinkingOn = this.reasoningConfig != null && !Boolean.FALSE.equals(this.reasoning)
					&& (Boolean.TRUE.equals(this.reasoningConfig.get("default_enabled"))
							|| Boolean.TRUE.equals(this.reasoningConfig.get("mandatory")));
		}

		if (!thinkingOn) {
			return parameters;
		}

		if (callerSetEffort && this.reasoningConfig != null) {
			validateEffortAllowed(parameters.get(EFFORT));
		}

		if (parameters == null) {
			parameters = new HashMap<>();
		}
		if (!callerSetThinking) {
			parameters.put(THINKING, Boolean.TRUE);
		}
		if (!callerSetEffort) {
			String defaultEffort = getDefaultEffort();
			if (defaultEffort != null) {
				parameters.put(EFFORT, defaultEffort);
			}
		}
		return parameters;
	}

	/**
	 * Drop a caller supplied temperature when the MODELMETADATA row marks the model
	 * as not accepting one - the reasoning models in particular reject the param
	 * outright, and callers (including our own reactors that hardcode a
	 * temperature) should not have to know which model is behind the engine. A null
	 * or true metadata value leaves the parameters untouched.
	 *
	 * @return the same parameters map, minus the temperature when unsupported
	 */
	protected Map<String, Object> applyTemperatureParameter(Map<String, Object> parameters) {
		if (parameters == null || parameters.isEmpty() || !Boolean.FALSE.equals(this.temperatureSupported)) {
			return parameters;
		}

		Iterator<Map.Entry<String, Object>> entries = parameters.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry<String, Object> entry = entries.next();
			if (entry.getKey() != null && TEMPERATURE.equalsIgnoreCase(entry.getKey().trim())) {
				entries.remove();
				classLogger.info(
						"Dropping the temperature param for model {} - the model metadata says it is not supported",
						Utility.cleanLogString(this.engineId));
			}
		}
		return parameters;
	}

	/**
	 * Whether a caller-supplied thinking param asks for thinking to be on - either
	 * a truthy flag (mirroring the python string_to_bool values) or an anthropic
	 * style config map whose type is enabled/adaptive.
	 */
	private static boolean isThinkingRequested(Object thinkingParam) {
		if (thinkingParam == null) {
			return false;
		}
		if (thinkingParam instanceof Map) {
			Object type = ((Map<?, ?>) thinkingParam).get("type");
			return "enabled".equals(type) || "adaptive".equals(type);
		}
		if (thinkingParam instanceof Boolean) {
			return (Boolean) thinkingParam;
		}
		if (thinkingParam instanceof Number) {
			return ((Number) thinkingParam).intValue() != 0;
		}
		String value = thinkingParam.toString().trim().toLowerCase();
		return value.equals("true") || value.equals("t") || value.equals("yes") || value.equals("y")
				|| value.equals("1");
	}

	/**
	 * A caller-named effort must be one of the config's supported_efforts when the
	 * config defines that list. Numeric token budgets are left for the python side
	 * to bucket onto the effort ladder.
	 */
	private void validateEffortAllowed(Object effortParam) {
		if (effortParam == null || effortParam instanceof Number) {
			// only a thinking_budget or a raw budget - nothing to check
			return;
		}
		Object supported = this.reasoningConfig.get("supported_efforts");
		if (!(supported instanceof List) || ((List<?>) supported).isEmpty()) {
			return;
		}
		String effort = effortParam.toString().trim();
		if (effort.isEmpty() || effort.matches("-?\\d+")) {
			return;
		}
		for (Object allowed : (List<?>) supported) {
			if (allowed != null && effort.equalsIgnoreCase(allowed.toString().trim())) {
				return;
			}
		}
		throw new IllegalArgumentException(
				"Effort '" + effort + "' is not supported for this model. Supported efforts: " + supported);
	}

	/**
	 * The config's default_effort as a canonical lowercase string, or null when the
	 * config does not name one.
	 */
	private String getDefaultEffort() {
		if (this.reasoningConfig == null) {
			return null;
		}
		Object value = this.reasoningConfig.get("default_effort");
		if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
			return null;
		}
		return ((String) value).trim().toLowerCase();
	}

	/**
	 * Executes a model request using the current input message.
	 *
	 * @param inputMessage    current input sent to the model
	 * @param insight         current insight
	 * @param roomId          current room identifier
	 * @param hyperParameters model request parameters
	 * @return model response
	 */
	protected abstract AskModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
			Map<String, Object> hyperParameters);

	/**
	 * Adapts the legacy model request arguments to an {@link InputMessage}.
	 *
	 * @param question        input text
	 * @param fullPrompt      optional full prompt
	 * @param context         optional system prompt
	 * @param insight         current insight
	 * @param roomId          current room identifier
	 * @param hyperParameters model request parameters
	 * @return model response
	 * @deprecated override and call
	 *             {@link #askCall(InputMessage, Insight, String, Map)} instead
	 */
	@Deprecated
	public AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> hyperParameters) {
		Room room = new Room();
		room.setId(roomId);
		room.setInsight(insight);

		Map<String, Object> messageParameters = hyperParameters == null ? new HashMap<>()
				: new HashMap<>(hyperParameters);
		if (fullPrompt != null) {
			messageParameters.put(FULL_PROMPT, fullPrompt);
		}

		InputMessage inputMessage = InputMessage.builder(room).withText(question).withSystemPrompt(context)
				.withParamMap(messageParameters).build();
		return askCall(inputMessage, insight, roomId, hyperParameters);
	}

	/**
	 * Returns the final message in a full prompt as the input that owns the next
	 * model response.
	 *
	 * @param messages normalized messages created from the full prompt
	 * @return the final input message
	 * @throws IllegalArgumentException when the full prompt does not end with an
	 *                                  input message
	 */
	static InputMessage requireFinalInputMessage(List<AbstractMessage> messages) {
		if (messages == null || messages.isEmpty()) {
			throw new IllegalArgumentException("Full prompt must end with an input message");
		}
		AbstractMessage finalMessage = messages.get(messages.size() - 1);
		if (!(finalMessage instanceof InputMessage)) {
			throw new IllegalArgumentException("Full prompt must end with an input message");
		}
		return (InputMessage) finalMessage;
	}

	@Override
	public AskModelEngineResponse askRoom(InputMessage inputMessage, Room room, Map<String, Object> parameters) {
		if (inputMessage == null) {
			throw new IllegalArgumentException("Input message cannot be null");
		}

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

		String promptForLogs = inputMessage.getFullInputPrompt();
		InputMessage inferenceInputMessage = inputMessage;

		// if full prompt is being sent, convert the full prompt to a set of
		// AbstractMessages
		// then set the message_json to be the new abstractMessages
		Object fullPrompt = parameters.remove(FULL_PROMPT);
		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore
				.acquireMutationLock(fullPrompt != null ? room : null)) {
			if (fullPrompt != null) {
				List<AbstractMessage> messageList = MessageUtils.convertFullPrompt(fullPrompt, room, this);
				inferenceInputMessage = requireFinalInputMessage(messageList);
				Object appendFullPrompt = parameters.remove(APPEND_FULL_PROMPT);
				if (appendFullPrompt != null && Boolean.parseBoolean(appendFullPrompt + "")) {
					String userId = room.getInsight().getUser().getPrimaryLoginToken().getId();
					RoomMessageStore.refreshFromLatestProjection(room, userId);
					// the provider payload is the full merged list; validate it
					// before mutating the room so a rejected request does not
					// leave never-sent messages in the cached room history
					List<AbstractMessage> outbound = new ArrayList<>(room.getMessages());
					outbound.addAll(messageList);
					validateInputModalities(outbound);
					room.getMessages().addAll(messageList);
					messageList = room.getMessages();
				} else {
					validateInputModalities(messageList);
					room.setMessages(messageList);
				}
				parameters.put("message_json", RoomMessageStore.providerMessageHistory(room, messageList));

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

				fullPrompt = MessageUtils.toJsonArray(room.getMessages());
				promptForLogs = (String) fullPrompt;
			}

			if (fullPrompt == null) {
				// incremental ask: the provider payload is the branch ending at
				// inputMessage (the fullPrompt path validated its list above)
				validateInputModalities(room.getMessages(), inferenceInputMessage);
				parameters.put("message_json",
						RoomMessageStore.messageHistoryWithNewMessage(room, inferenceInputMessage));
			}

			ZonedDateTime inputTime = ZonedDateTime.now();
			AskModelEngineResponse askModelResponse = askCall(inferenceInputMessage, room.getInsight(), room.getId(),
					parameters);
			ZonedDateTime outputTime = ZonedDateTime.now();

			if (AskModelEngineResponse.ERROR.equals(askModelResponse.getMessageType())) {
				AskErrorModelEngineResponse errorDetails = (AskErrorModelEngineResponse) askModelResponse;
				classLogger.error(
						"An error occurred in the {} client with status code {} for model {}. ERROR: {} TRACEBACK: {}",
						errorDetails.getClient(), errorDetails.getCode(), errorDetails.getModel(),
						errorDetails.getStringResponse(), errorDetails.getTraceback());

				askModelResponse.setMessageId(GUID.v7().toUUID().toString());
				askModelResponse.setRoomId(room.getId());

				throw new SemossModelEngineException(askModelResponse);
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
						/*messageId*/ inferenceInputMessage.getMessageId(),
						/*transactionId*/askModelResponse.getMessageId(),
						/*messageMethod*/inferenceLogMessageMethod("ask"),
						/*engine*/this,
						/*insightId*/room.getInsight().getInsightId(),
						/*projectContextId*/room.getInsight().getContextProjectId(),
						/*projectId*/room.getInsight().getProjectId(),
						/*user*/room.getInsight().getUser(),
						/*sessionId*/ThreadStore.getSessionId(),
						/*roomId*/room.getId(),
						/*context*/inferenceInputMessage.getSystemPrompt(),
						/*prompt*/promptForLogs,
						/*fullPrompt*/fullPrompt,
						/*promptTokens*/askModelResponse.getNumberOfTokensInPrompt(),
						/*inputTime*/inputTime,
						/*response*/askModelResponse.getStringResponse(),
						/*responseTokens*/askModelResponse.getNumberOfTokensInResponse(),
						/*outputTime*/outputTime,
						/*inputTokens*/askModelResponse.getNumberOfTokensInPrompt(),
						/*outputTokens*/askModelResponse.getNumberOfTokensInResponse(),
						/*cacheReadTokens*/askModelResponse.getNumberOfCacheReadTokens(),
						/*cacheCreationTokens*/askModelResponse.getNumberOfCacheCreationTokens(),
						/*thinkingTokens*/askModelResponse.getNumberOfThinkingTokens()
						));
				inferenceRecorder.start();
			}
			// @formatter:on

			// update current usage based on this new request
			ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(userRestrictionMap, askModelResponse,
					inputTime, outputTime, this.inputTokenCredit, this.outputTokenCredit,
					this.cacheReadMultiplier, this.cacheWriteMultiplier);

			String currentRoomName = room.getRoomName();

			if (fullPrompt != null) {
				// Grabbing room name from first input message if room name is empty
				if (currentRoomName == null || currentRoomName.isEmpty()) {
					String roomName = null;
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
						ModelInferenceLogsUtils.doSetNameForRoom(
								room.getInsight().getUser().getPrimaryLoginToken().getId(), room.getId(), roomName);
					}
				}

				ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(askModelResponse).build();
				room.getMessages().add(response);

				// set transaction id for both pieces
				inferenceInputMessage.setTransactionId(response.getMessageId());
				inferenceInputMessage.setTokensInMessage(askModelResponse.getNumberOfTokensInPrompt());
				inferenceInputMessage.setCacheReadTokens(askModelResponse.getNumberOfCacheReadTokens());
				response.setTransactionId(response.getMessageId());

				// Create the assistant's response message and add to history
				response.setModel(this);
				response.setParentMessageId(inferenceInputMessage.getMessageId());
				response.setTokensInMessage(askModelResponse.getNumberOfTokensInResponse());

				RoomMessageStore.persist(room, room.getInsight().getUser().getPrimaryLoginToken().getId());
			}

			return askModelResponse;
		}
	}

	/**
	 * Enforce the configured input modalities on the exact outbound message list.
	 * The rules, chosen so enforcement never rejects content the model is not
	 * actually asked to accept:
	 * <ul>
	 * <li>Only INPUT-direction messages are checked - the model's own responses in
	 * history are its output, not caller input.</li>
	 * <li>Only MEDIA parts are checked - a text command is required on every ask
	 * and system prompts are platform-injected, so TEXT is never a basis for
	 * rejection.</li>
	 * <li>Media whose modality cannot be determined (no MIME type, opaque URL)
	 * passes - enforcement fails open on unknowns; strict typing belongs to the
	 * metadata write path.</li>
	 * </ul>
	 */
	@Override
	public void validateInputModalities(List<AbstractMessage> outboundMessages) {
		if (this.inputModalities == null || outboundMessages == null) {
			return;
		}
		for (AbstractMessage message : outboundMessages) {
			validateInputModalities(message);
		}
	}

	/**
	 * Branch-scoped variant for the incremental ask path: validates the
	 * root-to-leaf branch ending at {@code inputMessage} (or at the current tail
	 * when null), matching the payload the provider history builders serialize.
	 */
	void validateInputModalities(List<AbstractMessage> messages, AbstractMessage inputMessage) {
		if (this.inputModalities == null) {
			return;
		}
		List<AbstractMessage> requestMessages = messages == null ? List.of() : messages;
		AbstractMessage leaf = inputMessage;
		if (leaf == null) {
			if (requestMessages.isEmpty()) {
				return;
			}
			leaf = requestMessages.get(requestMessages.size() - 1);
		}
		validateInputModalities(messageBranch(requestMessages, leaf));
	}

	/**
	 * Lightweight root-to-leaf parent walk. Unlike the payload builders this only
	 * needs to read part types, so it skips their tool-pruning deep copies. Broken
	 * parent links end the walk; cycles are guarded.
	 */
	private static List<AbstractMessage> messageBranch(List<AbstractMessage> messages, AbstractMessage leaf) {
		Map<String, AbstractMessage> messagesById = new HashMap<>();
		for (AbstractMessage message : messages) {
			if (message != null && message.getMessageId() != null) {
				messagesById.put(message.getMessageId(), message);
			}
		}
		List<AbstractMessage> branch = new ArrayList<>();
		Set<String> visited = new HashSet<>();
		AbstractMessage current = leaf;
		while (current != null) {
			String messageId = current.getMessageId();
			if (messageId != null && !visited.add(messageId)) {
				break;
			}
			branch.add(current);
			String parentMessageId = current.getParentMessageId();
			current = parentMessageId == null ? null : messagesById.get(parentMessageId);
		}
		return branch;
	}

	private void validateInputModalities(AbstractMessage message) {
		if (message == null || message.getIo() == MessageIO.OUTPUT) {
			return;
		}
		List<MessagePart> parts = message.getParts();
		if (parts == null) {
			return;
		}
		for (MessagePart part : parts) {
			ModelModalityEnum modality = modalityFor(part);
			if (modality != null) {
				requireInputModalityAllowed(modality);
			}
		}
	}

	/**
	 * Throw when the given modality is not in the configured input modalities.
	 * No-op when the engine does not restrict input.
	 */
	protected void requireInputModalityAllowed(ModelModalityEnum modality) {
		if (this.inputModalities == null || this.inputModalities.contains(modality)) {
			return;
		}
		String model = this.engineName == null || this.engineName.isBlank() ? this.engineId : this.engineName;
		throw new IllegalArgumentException("Model " + model + " does not allow " + modality.name()
				+ " input. Configured input modalities: " + this.inputModalities);
	}

	private static ModelModalityEnum modalityFor(MessagePart part) {
		if (part == null) {
			return null;
		}
		// exhaustive on purpose: a new part type must decide here whether it
		// carries a validatable modality
		return switch (part.getType()) {
		case MEDIA -> modalityFor((MediaMessagePart) part);
		case TEXT, SYSTEM, TOOL_CALL, TOOL_RESULT, THINKING, UNKNOWN -> null;
		};
	}

	private static ModelModalityEnum modalityFor(MediaMessagePart part) {
		MessageInputMedia media = part.getMediaInfo();
		return media == null ? null : ModelModalityEnum.fromMimeType(media.resolveMimeType());
	}

	/**
	 * messageMethod recorded on inference log rows written by this engine.
	 * Delegating engines (e.g. the model router) override this to tag their rows,
	 * so ask-history queries and usage aggregations can separate the delegating row
	 * from the actual model call.
	 */
	protected String inferenceLogMessageMethod(String method) {
		return method;
	}

	@Override
	@Deprecated
	public AskModelEngineResponse ask(String question, String context, Insight insight,
			Map<String, Object> parameters) {
		Room room = RoomUtils.createRoomIfNotExists(null, insight, this, question);
		InputMessage msg = InputMessage.builder(room).withSystemPrompt(context).withText(question)
				.withModelType(this.getModelType()).withParamMap(parameters).build();
		ResponseMessage response = room.ask(msg, this);
		return response.getModelEngineResponse();
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
					/*messageMethod*/inferenceLogMessageMethod("embeddings"),
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
				outputTime, this.inputTokenCredit, this.outputTokenCredit,
				this.cacheReadMultiplier, this.cacheWriteMultiplier);

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
		// if we know the model brand
		// return that for subtype
		if (smssProp.containsKey(MODEL_BRAND)) {
			return smssProp.getProperty(MODEL_BRAND);
		}
		// default to the model provider
		return this.getModelType().toString();
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

	@Override
	public int getContextWindow() {
		return this.contextWindow;
	}
}
