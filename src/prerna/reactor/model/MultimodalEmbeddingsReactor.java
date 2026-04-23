package prerna.reactor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class MultimodalEmbeddingsReactor extends AbstractReactor {

    private static final String INPUTS_KEY = "inputs";

    public MultimodalEmbeddingsReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.ENGINE.getKey(),
            INPUTS_KEY,
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] {1, 1, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);

        if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
            throw new IllegalArgumentException(
                "Model " + engineId + " does not exist or user does not have access to this model"
            );
        }

        List<Map<String, Object>> inputsToEmbed = getMultimodalInputs();
        Map<String, Object> paramMap = getMap();
        if (paramMap == null) {
            paramMap = new HashMap<>();
        }

        IModelEngine engine = Utility.getModel(engineId);
        Object output = engine.multimodalEmbeddings(inputsToEmbed, this.insight, paramMap);
        return new NounMetadata(output, PixelDataType.MAP);
    }

    /**
     * Extract the multimodal inputs from the reactor store.
     * Each input should be a Map with "type" and "content" keys.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getMultimodalInputs() {
        List<Map<String, Object>> inputs = new ArrayList<>();

        GenRowStruct grs = this.store.getGenRowStruct(INPUTS_KEY);
        if (grs != null && !grs.isEmpty()) {
            List<NounMetadata> mapInputs = grs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null) {
                for (NounMetadata noun : mapInputs) {
                    inputs.add((Map<String, Object>) noun.getValue());
                }
            }
        }

        if (inputs.isEmpty()) {
            throw new IllegalArgumentException(
                "Must provide multimodal inputs. Each input must be a map with 'type' and 'content' keys."
            );
        }

        return inputs;
    }

    private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getGenRowStruct(this.keysToGet[2]);
        if (mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        return null;
    }

    @Override
    public String getReactorDescription() {
        return "This reactor is used to interact with Multimodal Embedding Model Engines. "
             + "It accepts inputs of mixed modalities (text, image, video, audio) and returns "
             + "embedding vectors for each input.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(INPUTS_KEY)) {
            return "A list of maps, each with 'type' (text|image|video|audio) and 'content' (the data to embed).";
        }
        return super.getDescriptionForKey(key);
    }
}