package prerna.playground.reactors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddCOTToolExecutionReactor extends AbstractReactor {

	public AddCOTToolExecutionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				"roomId", // 1
				"toolId", // 2
				"toolName", // 3
				"toolPredictedArguments", // 4
				"toolExecutionResponse", // 5
				"toolParameterValues", // 6
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), // 7
		};
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String modelId = this.keyValue.get(this.keysToGet[index++]);
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		String toolId = this.keyValue.get(this.keysToGet[index++]);
		String toolName = this.keyValue.get(this.keysToGet[index++]);
		Map<String, Object> toolPredictionArgs = getMap(this.keysToGet[index++]);
		String toolResponseRaw = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[index++]));
		if (toolResponseRaw == null) {
			throw new IllegalArgumentException("Field " + this.keysToGet[index - 1] + " cannot be empty");
		}
		Map<String, Object> toolParamterValues = getMap(this.keysToGet[index++]);
		String parentMessageId = this.keyValue.get(this.keysToGet[index++]);

		User user = this.insight.getUser();
		String userId = user.getPrimaryLoginToken().getId();

		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}
		IModelEngine modelEngine = Utility.getModel(modelId);

		// --- 1. Security/room loading ---
		if (!ModelInferenceLogsUtils.validUserRoom(roomId, userId)) {
			throw new IllegalArgumentException("User does not have access to room " + roomId);
		}
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
		}
		boolean requireInputMessage = false;
		if (parentMessageId == null) {
			AbstractMessage lastMessage = messages.getLast();
			requireInputMessage = MessageType.isResponseMessage(lastMessage.getMessageType());
			parentMessageId = lastMessage.getMessageId();
		}

		// we will now mock and add a fake input message to get the tool
		// and then we will add the tool execution
		InputMessage inputMessageForToolResponse = null;
		ResponseMessage toolResponseMessage = null;
		InputMessage toolExecutionMessage = null;
		if (requireInputMessage) {
			Map<String, Object> paramMap = new HashMap<>();
			paramMap.put("tool_choice", MessageUtils.makeToolChoice(MessageUtils.ToolChoiceType.FORCED, toolName));

			inputMessageForToolResponse = InputMessage.builder(room)
					.withInputUIPrompt("Continue running the tools in the chain of thought prompt")
					.withInputPrompt("Continue running the tools in the chain of thought prompt")
					.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

			inputMessageForToolResponse.setVisibile(false);
			inputMessageForToolResponse.setPlatformGenerated(true);
			inputMessageForToolResponse.setParentMessageId(parentMessageId);
			// set the new parent message id
			parentMessageId = inputMessageForToolResponse.getMessageId();
		}
		// now we will mock the tool response
		{
			Map<String, Object> toolResponse = new HashMap<>();
			toolResponse.put("id", toolId);
			toolResponse.put("name", toolName);
			toolResponse.put("type", "function");
			toolResponse.put("arguments", toolPredictionArgs);
			toolResponseMessage = ResponseMessage.toolResponse(toolResponse);
			toolResponseMessage.setRoom(room);
			toolResponseMessage.setModel(modelEngine);
			toolResponseMessage.setPlatformGenerated(true);
			toolResponseMessage.setParentMessageId(parentMessageId);
			// this will append the title
			MCPUtility.updateToolResponseWithProjectMeta(toolResponseMessage);
			// set the new parent message id
			parentMessageId = toolResponseMessage.getMessageId();
		}
		// and finally, we will add the tool execution
		{
			toolExecutionMessage = InputMessage.toolExecution(room, toolId, toolName, toolResponseRaw,
					toolParamterValues);
			toolResponseMessage.setPlatformGenerated(true);
			toolExecutionMessage.setModel(modelEngine);
			toolExecutionMessage.setParentMessageId(parentMessageId);
		}

		if (inputMessageForToolResponse != null) {
			messages.add(inputMessageForToolResponse);
		}
		messages.add(toolResponseMessage);
		messages.add(toolExecutionMessage);

		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		if (inputMessageForToolResponse != null) {
			pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMessageForToolResponse)));
		}
		pixelReturn.put("toolResponse", jsonToMap(MessageUtils.toJson(toolResponseMessage)));
		pixelReturn.put("toolExecution", jsonToMap(MessageUtils.toJson(toolExecutionMessage)));

		ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), insight.getUser().getPrimaryLoginToken().getId(),
				room.getMessagesAsString());

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return """
				Add a tool execution input message to the message history.
				This will add 2 or 3 messages
					1. Input message for calling the tool - only if the last response in the history was a response message
					2. Response message of the tool
					3. Input message for the tool execution
				This does not execute the messages, only appends to the message history
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id of the model used for the message. If all the tools are added for the tool_resposne message, this model is used to invoke for the response.";
		} else if (key.equals("roomId")) {
			return "The room id corresponding to the message history";
		} else if (key.equals("toolId")) {
			return "The id of the tool that was executed - must match the tool id of tool_response message";
		} else if (key.equals("toolName")) {
			return "The name of the tool that was executed - must match the tool name of tool_response message";
		} else if (key.equals("toolExecutionResponse")) {
			return "The raw string output of the tool output";
		} else if (key.equals("toolParameterValues")) {
			return "Map object with the string parameterName to object value for the tool execution";
		}
		return super.getDescriptionForKey(key);
	}

}