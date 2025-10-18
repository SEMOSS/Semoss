package prerna.playground.reactors;

import java.util.Arrays;
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

public class AskPlaygroundReactor extends AbstractReactor {

	public AskPlaygroundReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.IMAGE.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.MCP_TOOL_ID.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		////// SET UP //////////
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		List<String> inputImages = getListString(ReactorKeysEnum.IMAGE.getKey());
		List<String> inputImageURLs = getListString(ReactorKeysEnum.URL.getKey());

		IModelEngine modelEngine = Utility.getModel(engineId);

		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);

		List<String> mcpToolIDs = getListString(ReactorKeysEnum.MCP_TOOL_ID.getKey());
		if (mcpToolIDs != null && !mcpToolIDs.isEmpty()) {
			room.getOptionsMap().put(ReactorKeysEnum.MCP_TOOL_ID.getKey(), mcpToolIDs);
		}

		List<String> copiedImages = MessageUtils.copyFilesToRoomFolder(inputImages, room, insight);

		// ---- Build the InputMessage
		InputMessage msg = InputMessage.builder(room).withInputUIPrompt(question).withInputPrompt(question)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).withImages(copiedImages, room)
				.withImageUrls(inputImageURLs)
				// .withTools(tools)
				.build();

		// ---- Actually run LLM call
		ResponseMessage response = room.ask(msg, context, modelEngine, parentMessageId);

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

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(msg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call (Playground)—returns both input and response message objects.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the prompt to execute against the LLM";
		} else if (key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
			return "The system prompt to use for the LLM call";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "This is the room ID that will be used for storing messages. If no room id is passed in, then insight id will be used for the room";
		} else if (key.equals(ReactorKeysEnum.IMAGE.getKey())) {
			return "This is  an array of image file names that have already been uploaded to the insight folder.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return """
					Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc.
					In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for
					<replacement>
					"""
					.replace("<replacement>", Arrays.asList(ReactorKeysEnum.COMMAND.getKey(),
							ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey()).toString());
		}
		return super.getDescriptionForKey(key);
	}

}