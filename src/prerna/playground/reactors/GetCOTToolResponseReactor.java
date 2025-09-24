package prerna.playground.reactors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCOTToolResponseReactor extends AbstractReactor {
    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

    public GetCOTToolResponseReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.ENGINE.getKey(),      // 0
            ReactorKeysEnum.ROOM_ID.getKey(),     // 1 (optional, for history)
            "stepNumber",                             // 2 (required)
            "toolName",                           // 3 (required)
            "toolMeta",                           // 4 (optional: schema/options/desc for the tool) //TODO remove this - likely not needed
            "context",                            // 5 (optional: additional context) //TODO remove this - likely not needed
        };
        // Only ENGINE and toolName are required
        this.keyRequired = new int[] {1, 0, 0, 1, 0, 0};
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
        Room room = (roomId != null && !roomId.isEmpty())
            ? RoomUtils.getOrLoadRoom(roomId, this.insight)
            : null;
        IModelEngine modelEngine = prerna.util.Utility.getModel(modelId);

        
        String stepPart = stepNumber != null ? "For step: " + stepNumber : "";
        String contextPart = extraContext != null ? "Context: " + extraContext : ""; //TODO remove this - likely not needed
        String toolPart = toolMeta != null ? toolMeta : "(No further tool meta supplied)"; //TODO remove this - likely not needed
        String userPrompt = String.format(PlaygroundUtils.TOOL_ARGUMENTS_PROMPT, toolName, toolPart, stepPart, contextPart);

        // Optionally, get message history as additional context (up to you)
        // List<AbstractMessage> history = room != null ? room.getMessages() : null;

        Map<String, Object> paramMap = new HashMap<>(); 
        Map<String, String> forcedTool = new HashMap<>(); 
        forcedTool.put("type", "forced");
        forcedTool.put("name", "toolName");
       // paramMap.put("tool_choice", forcedTool);
       // paramMap.put("tool_choice", "required"); // temporary! 

        InputMessage inputMsg = InputMessage.builder(room)
            .withInputUIPrompt(userPrompt)
            .withInputPrompt(userPrompt)
            .withModelType(modelEngine.getModelType())
            .withParamMap(paramMap)
            .build();
        
        inputMsg.setVisibile(false); //this is a hidden message

        // Run LLM (not saving in history for now)
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.COT_SYSTEM_PROMPT, modelEngine);
        response.setParentMessageId(inputMsg.getParentMessageId()); // skip the input message with respect to the history
//        
//        List<Map<String, Object>> tools = response.getToolResponses();
//
//        // Try to parse the response as tool-argument JSON map
//        Map<String, Object> argMap = null;
//        try {
//            argMap = GSON.fromJson(response.getContent(), new TypeToken<Map<String, Object>>(){}.getType());
//        } catch (Exception e) {
//            argMap = new LinkedHashMap<>();
//            argMap.put("output", response.getContent());
//        }

        // Return only argument map for FE usage (could add other verbose info if desired)
        

		// parse the response for code blocks
		if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
			response = MessageUtils.processMarkdownCodeBlocks(response, modelEngine, room);
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(),
					insight.getUser().getPrimaryLoginToken().getId(), room.getMessagesAsString());
		} else if (response.getMessageType() == MessageType.RESPONSE_TOOL) {
			MessageUtils.updateToolResponseWithProjectMeta(response);
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(response)));
		
        return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
    }
}