package prerna.playground.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * AddToolExecutionReactor: Input: roomId, toolId, toolName,
 * tool_execution_response, tool_
 */
public class AddPlaygroundToolExecutionReactor extends AbstractReactor {

	@Deprecated
	private final String tool_execution_response = "tool_execution_response";

	public AddPlaygroundToolExecutionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				"roomId", // 1
				"toolId", // 2
				"toolName", // 3
				"toolExecutionResponse", // 4
				"toolParameterValues", // 5
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), // 6
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), // 7
				tool_execution_response };
		// TODO: once we remove the legacy tool_execution_response, we will make
		// toolExecutionResponse mandatory field
		this.keyRequired = new int[] { 1, 1, 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		String roomId = this.keyValue.get(this.keysToGet[1]);
		String toolId = this.keyValue.get(this.keysToGet[2]);
		String toolName = this.keyValue.get(this.keysToGet[3]);
		String toolResponseRaw = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[4]));
		if (toolResponseRaw == null) {
			toolResponseRaw = Utility.decodeURIComponent(this.keyValue.get(tool_execution_response));
		}
		if (toolResponseRaw == null) {
			throw new IllegalArgumentException("Field " + this.keysToGet[4] + " cannot be empty");
		}
		Map<String, Object> toolParamterValues = getMap(this.keysToGet[5]);
		String parentMessageId = this.keyValue.get(this.keysToGet[6]);
		Map<String, Object> paramMap = getMap(this.keysToGet[7]);
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

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

		AskModelEngineResponse response = room.addToolExecutionResult(toolId, toolName, toolResponseRaw,
				toolParamterValues, paramMap, parentMessageId, modelEngine, insight);

		Map<String, Object> pixelReturn = new HashMap<>();
		if (response == null) {
			pixelReturn.put("responseMessage",
					"Tool output added successfully. Additional tool executions required to continue");
			return new NounMetadata("Tool output added successfully", PixelDataType.CONST_STRING);
		} else {
			// parse the response for code blocks
			ResponseMessage lastMessage = (ResponseMessage) room.getMessages().getLast();
			if (lastMessage.getMessageType() == MessageType.RESPONSE_TEXT) {
				lastMessage = MessageUtils.processMarkdownCodeBlocks(lastMessage, modelEngine, room);
				ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(),
						insight.getUser().getPrimaryLoginToken().getId(), room.getMessagesAsString());
			} else if (lastMessage.getMessageType() == MessageType.RESPONSE_TOOL) {
				MCPUtility.updateToolResponseWithProjectMeta(lastMessage);
			}
			pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(lastMessage)));
			return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Add a tool execution input message to the message history.
				If all the tools have been executed from the previous tool response message, this will return the LLM response.
				Otherwise, a default string message that more tools responses are needed
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
		} else if (key.equals(tool_execution_response)) {
			return "Deprecated parameter. Please switch to toolExecutionResponse";
		}
		return super.getDescriptionForKey(key);
	}
}
