package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PredictCOTToolReactor extends AbstractReactor {

	public PredictCOTToolReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				ReactorKeysEnum.ROOM_ID.getKey(), // 1
				"stepNumber", // 2
				"toolName", // 3
		};
		this.keyRequired = new int[] { 1, 1, 1, 1, };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String modelId = this.keyValue.get(this.keysToGet[index++]);
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		String stepNumber = this.keyValue.get(this.keysToGet[index++]);
		String toolName = this.keyValue.get(this.keysToGet[index++]);

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException(
					"Room message history is empty. Cannot predict tool parameters before COT has been executed");
		}
		// we are making a new room
		// with the same id
		// but with its own messages object so we dont mess up the values
		Room tempRoom = new Room();
		tempRoom.setId(roomId + "_args");
		tempRoom.setInsight(this.insight);
		tempRoom.setMessages(new ArrayList<>(messages));
		// we need this for tools
		tempRoom.setOptionsMap(room.getOptionsMap());
		IModelEngine modelEngine = Utility.getModel(modelId);

		String stepPart = stepNumber != null ? "For step: " + stepNumber : "";
		String userPrompt = String.format(PlaygroundUtils.TOOL_ARGUMENTS_PREDICTION_PROMPT, toolName, stepPart);

		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("tool_choice", MessageUtils.makeToolChoice(MessageUtils.ToolChoiceType.FORCED, toolName));

		InputMessage inputMsg = InputMessage.builder(tempRoom).withInputPrompt(userPrompt)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		inputMsg.setVisibile(false);

		// Run LLM (not saving in history for now)
		ResponseMessage response = tempRoom.ask(inputMsg, PlaygroundUtils.COT_SYSTEM_PROMPT, modelEngine);
		response.setParentMessageId(inputMsg.getParentMessageId());

		// parse the response for code blocks
		if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
			response = MessageUtils.processMarkdownCodeBlocks(response, modelEngine, room);
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(),
					insight.getUser().getPrimaryLoginToken().getId(), room.getMessagesAsString());
		} else if (response.getMessageType() == MessageType.RESPONSE_TOOL) {
			MCPUtility.updateToolResponseWithProjectMeta(response);
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return """
				Predict the tool execution for a specific step in the COT plan.
				The prediction does not affect the message history in the room.
				""";
	}

}