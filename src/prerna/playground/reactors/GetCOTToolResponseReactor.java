package prerna.playground.reactors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
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

public class GetCOTToolResponseReactor extends AbstractReactor {

	public GetCOTToolResponseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				ReactorKeysEnum.ROOM_ID.getKey(), // 1 (optional, for history)
				"stepNumber", // 2 (required)
				"toolName", // 3 (required)
				// TODO remove this - likely not needed
				"toolMeta", // 4 (optional: schema/options/desc for the tool)
				// TODO remove this - likely not needed
				"context", // 5 (optional: additional context)
		};
		// Only ENGINE and toolName are required
		this.keyRequired = new int[] { 1, 0, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		String roomId = this.keyValue.get(this.keysToGet[1]);
		String stepNumber = this.keyValue.get(this.keysToGet[2]);
		String toolName = this.keyValue.get(this.keysToGet[3]);
		String toolMeta = this.keyValue.get(this.keysToGet[4]); // Could be JSON or desc string
		String extraContext = this.keyValue.get(this.keysToGet[5]);

		// Optional: fetch room and context/history, but this is a "one-off"
		Room room = (roomId != null && !roomId.isEmpty()) ? RoomUtils.getOrLoadRoom(roomId, this.insight) : null;
		IModelEngine modelEngine = prerna.util.Utility.getModel(modelId);

		String stepPart = stepNumber != null ? "For step: " + stepNumber : "";
		// TODO remove this - likely not needed
		String contextPart = extraContext != null ? "Context: " + extraContext : "";
		// TODO remove this - likely not needed
		String toolPart = toolMeta != null ? toolMeta : "(No further tool meta supplied)";
		String userPrompt = String.format(PlaygroundUtils.TOOL_ARGUMENTS_PROMPT, toolName, toolPart, stepPart,
				contextPart);

		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("tool_choice", MessageUtils.makeToolChoice(MessageUtils.ToolChoiceType.FORCED, toolName));

		InputMessage inputMsg = InputMessage.builder(room).withInputUIPrompt(userPrompt).withInputPrompt(userPrompt)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		inputMsg.setVisibile(false); // this is a hidden message

		// Run LLM (not saving in history for now)
		ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.COT_SYSTEM_PROMPT, modelEngine);
		// skip the input message with respect to the
		// history
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
}