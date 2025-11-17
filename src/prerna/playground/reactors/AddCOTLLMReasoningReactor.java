package prerna.playground.reactors;

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

public class AddCOTLLMReasoningReactor extends AbstractReactor {

	public AddCOTLLMReasoningReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				ReactorKeysEnum.ROOM_ID.getKey(), // 1
				"stepNumber", // 2
		};
		this.keyRequired = new int[] { 1, 1, 1, };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String modelId = this.keyValue.get(this.keysToGet[index++]);
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		String stepNumber = this.keyValue.get(this.keysToGet[index++]);

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException(
					"Room message history is empty. Cannot run an LLM reasoning step before COT has been executed");
		}

		IModelEngine modelEngine = Utility.getModel(modelId);

		String userPrompt = String.format("Continue the chain of thought to perform reasoning for step: " + stepNumber);

		Map<String, Object> paramMap = new HashMap<>();
		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(PlaygroundUtils.COT_SYSTEM_PROMPT)
				.withInputPrompt(userPrompt).withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();
		inputMsg.setVisibile(false);

		// Run LLM (not saving in history for now)
		ResponseMessage response = room.ask(inputMsg, modelEngine);
		response.setParentMessageId(inputMsg.getParentMessageId());

		// parse the response for code blocks
		// this should only be response text
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
				Add an LLM reasoning step for the Chain of Thought processing.
				""";
	}

}