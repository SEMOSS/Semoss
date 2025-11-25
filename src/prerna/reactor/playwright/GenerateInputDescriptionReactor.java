package prerna.reactor.playwright;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import java.util.Map;
import java.util.UUID;

public class GenerateInputDescriptionReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GenerateInputDescriptionReactor.class);

    public GenerateInputDescriptionReactor() {
        this.keysToGet = new String[] {"engineId", ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
        this.keyRequired = new int[] {1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        if (engineId == null || engineId.isEmpty()) {
            throw new IllegalArgumentException("Model engineId is required");
        }

        Map<String, String> paramValues = getMap(this.keysToGet[1]);
        String label = paramValues.get("label");
        String placeholder = paramValues.get("placeholder");
        String type = paramValues.get("type");
        String pageTitle = paramValues.get("pageTitle");
        return new NounMetadata(generateInputDescription(engineId, label, placeholder, type, pageTitle), PixelDataType.MAP);
    }

    private String generateInputDescription(String engineId, String label, String placeholder, String type, String pageTitle) {


        String prompt = generatePrompt(label, placeholder, type, pageTitle);
        classLogger.info(prompt);

        IModelEngine modelEngine = Utility.getModel(engineId);
        Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), insight, modelEngine, null, null, null, null);
        InputMessage inputMessage = InputMessage.builder(room).withInputPrompt(prompt).build();
        ResponseMessage response = room.ask(inputMessage, modelEngine);

        return response.getContent();
    }

    private String generatePrompt(String label, String placeholder, String type, String pageTitle) {
        StringBuilder prompt = new StringBuilder();

        prompt.append("Generate a description for an input field in a browser page with the following details. Return back the description only without any additional text. ");

        if (label != null && !label.isEmpty()) {
            prompt.append("Label: ").append(label).append(". ");
        }
        if (placeholder != null && !placeholder.isEmpty()) {
            prompt.append("Placeholder: ").append(placeholder).append(". ");
        }
        if( type != null && !type.isEmpty()) {
            prompt.append("Type: ").append(type).append(". ");
        }
        if (pageTitle != null && !pageTitle.isEmpty()) {
            prompt.append("Page Title: ").append(pageTitle).append(". ");
        }

        return prompt.toString().trim();
    }
}
