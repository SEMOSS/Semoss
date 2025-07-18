package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskPlaygroundReactor extends AbstractReactor {

    private static final Gson gson = new Gson();

    
    public AskPlaygroundReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.ENGINE.getKey(),
            ReactorKeysEnum.ROOM_ID.getKey(),
            ReactorKeysEnum.COMMAND.getKey(),
            ReactorKeysEnum.CONTEXT.getKey(),
            ReactorKeysEnum.IMAGE.getKey(),
            ReactorKeysEnum.URL.getKey(),
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 0, 1, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        ////// SET UP //////////
        organizeKeys();
        String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
        User user = this.insight.getUser();
        if (user == null) throw new IllegalArgumentException("You are not properly logged in");
        String userId = user.getPrimaryLoginToken().getId();

        if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
            throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
        }

        String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));
        String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
        if (context != null) context = Utility.decodeURIComponent(context);

        Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();

        List<String> inputImages = getImages();
        List<String> inputImageURLs = getImageURLs();
        IModelEngine modelEngine = Utility.getModel(engineId);

        Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
        MessageUtils.copyFilesToRoomFolder(inputImages, room, insight);

        // ---- Build the InputMessage
        InputMessage msg = InputMessage.builder(room)
            .withInputUIPrompt(question)
            .withInputPrompt(question)
            .withModelType(modelEngine.getModelType())
            .withParamMap(paramMap)
            .withImages(inputImages, room)
            .withImageUrls(inputImageURLs)
            .build();

        // ---- Actually run LLM call
        ResponseMessage response = room.ask(msg, context, modelEngine);

        // ---- Return both messages as a Map
        Map<String, Object> pixelReturn = new LinkedHashMap<>();

         pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(msg)));
         pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

        return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    // ------- image/file helpers, paramMap etc. ---------------
    public List<String> getImages() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }

    public List<String> getImageURLs() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
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
        return "This method is used to run an LLM text-generation call (Playground)—returns both input and response message objects.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if(key.equals(ReactorKeysEnum.COMMAND.getKey())) {
            return "This is the prompt to execute against the LLM";
        } else if(key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
            return "The system prompt to use for the LLM call";
        } else if(key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
            return "This is the room ID that will be used for storing messages. If no room id is passed in, then insight id will be used for the room";
        } else if(key.equals(ReactorKeysEnum.IMAGE.getKey())) {
            return "This is  an array of image file names that have already been uploaded to the insight folder.";
        } else if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
            return "Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc. "
                + "In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for " +
                Arrays.asList(ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey());
        }
        return super.getDescriptionForKey(key);
    }
    
    /**
     * Converts a JSON object string to a Map<String, Object>
     * @param json The JSON string (must be a JSON object: { ... })
     * @return The parsed Map
     */
    public static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
            throw new IllegalArgumentException("Input must be a valid JSON object string.");
        }
        return gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
    }

}