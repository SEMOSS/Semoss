package prerna.playground.reactors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.MessageUtils.ToolChoiceType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ConfirmCOTReactor extends AbstractReactor {
    private static final Logger logger = LogManager.getLogger(ConfirmCOTReactor.class);

    public ConfirmCOTReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.ENGINE.getKey(),    // 0, required
            ReactorKeysEnum.ROOM_ID.getKey(),   // 1, optional
            "cotPlan",  // 2, required (json from FE)
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 3, optional
        };
        this.keyRequired = new int[]{1, 0, 1, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String modelId = this.keyValue.get(this.keysToGet[0]);
        String roomId = this.keyValue.get(this.keysToGet[1]);
        // User query is not vital here, included for completeness
        String cotPlanStr = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[3])); // JSON string

        User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
            throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to this model");
        }

        IModelEngine modelEngine = Utility.getModel(modelId);
        Room room = (roomId != null && !roomId.isEmpty())
                ? RoomUtils.getOrLoadRoom(roomId, this.insight)
                : null;

        Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        paramMap.put("tool_choice", MessageUtils.makeToolChoice(ToolChoiceType.NONE, null));

        // Compose inputUI prompt for FE display
        final String inputUIPrompt = "Confirmed Plan";
        // Compose input prompt for LLM -- just feed the JSON plan string
        final String inputPrompt = PlaygroundUtils.CONFIRM_COT_PLAN.formatted(cotPlanStr);

        InputMessage inputMsg = InputMessage.builder(room)
            .withInputUIPrompt(inputUIPrompt)
            .withInputPrompt(inputPrompt)
            .withModelType(modelEngine.getModelType())
            .withParamMap(paramMap)
            .build();

        // IMPORTANT: Use your COT_SYSTEM_PROMPT as system prompt
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.COT_SYSTEM_PROMPT, modelEngine);

        response.setOrnament(PlaygroundUtils.PLAYGROUND_MESSAGE_TYPE, "COT_CONFIRM");

        Map<String, Object> pixelReturn = new LinkedHashMap<>();
        pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
        pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(response)));
        return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
    }


	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

    @Override
    public String getReactorDescription() {
        return """
        Takes a chain-of-thought JSON plan confirmed by the user and sends it (as JSON, no wrapping)
        to the LLM using the execution system prompt.
        inputUIPrompt is 'Confirmed Plan', inputPrompt is the plan JSON string as-is.
        """;
    }
}