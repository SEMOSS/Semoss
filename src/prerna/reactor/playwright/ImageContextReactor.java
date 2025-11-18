package prerna.reactor.playwright;

import java.util.*;

import prerna.engine.api.IModelEngine;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ImageContextReactor extends AbstractReactor {
    
    public ImageContextReactor() {
        this.keysToGet = new String[] {
            "sessionId",
            "tabId",
            ReactorKeysEnum.ENGINE.getKey(),
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 1 };
    }
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String tabId = this.keyValue.get(this.keysToGet[1]);
        String engineId = this.keyValue.get(this.keysToGet[2]);
        Map<String, Object> paramValues = getMap(this.keysToGet[3]);
        
        String userPrompt = (String) paramValues.get("userPrompt");
        
        Map<String, Object> result = analyzeCroppedImageWithVision(sessionId, engineId, paramValues, userPrompt, tabId);
        
        return new NounMetadata(result, PixelDataType.MAP);
    }

    public Map<String, Object> analyzeCroppedImageWithVision(String sessionId, String engineId, Map<String, Object> paramValues, String userPrompt, String tabId) {
        try {
            ScreenshotReactor screenshotReactor = new ScreenshotReactor();
            screenshotReactor.setInsight(this.insight);
            screenshotReactor.setNounStore(this.store);
            
            // add sessionId parameter to the noun store
            GenRowStruct sessionGrs = this.store.makeNoun(ReactorKeysEnum.SESSION_ID.getKey());
            sessionGrs.add(new NounMetadata(sessionId, PixelDataType.CONST_STRING));
            sessionGrs.add(new NounMetadata(tabId, PixelDataType.CONST_STRING));
            
            GenRowStruct cropGrs = this.store.makeNoun("cropParams");
            Map<String, Object> cropParams = new HashMap<>();

            cropParams.put("startX", paramValues.get("startX"));
            cropParams.put("startY", paramValues.get("startY"));
            cropParams.put("endX", paramValues.get("endX"));
            cropParams.put("endY", paramValues.get("endY"));
            cropGrs.add(new NounMetadata(cropParams, PixelDataType.MAP));
            
            
            NounMetadata screenshotResult = screenshotReactor.execute();
            
            ScreenshotResponse croppedImage = (ScreenshotResponse) screenshotResult.getValue();
            
            String instruction = String.format("Given the prompt \"%s\", what information would be useful from this image?", userPrompt);
            
            IModelEngine modelEngine = Utility.getModel(engineId);

            String insightFolder = this.insight.getInsightFolder();

            String imageName = "playwright_screenshot_" + System.currentTimeMillis() + ".png";
            String modelOutput = PlaywrightUtility.callModel(insightFolder, imageName, croppedImage, modelEngine, instruction, this.insight);

            Map<String, Object> result = new HashMap<>();
            result.put("response", modelOutput);

            return result;
            
        } catch (Exception e) {
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("response", "Error: " + e.getMessage());
            return errorResult;
        }
    }

    @Override
    public String getReactorDescription() {
        return "Reactor that extract a context from a given clipped image";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        return switch (key) {
            case "sessionId" -> "The id of the current session of the playwright";
            case "engineId" -> "The id of the Model Engine";
            case "userPrompt" -> "The custom prompt from the user for the LLM";
            case "startX" -> "the start x coordinates for the clipped image";
            case "startY" -> "the start y coordinates for the clipped image";
            case "endX" -> "the end x coordinates for the clipped image";
            case "endY" -> "the end y coordinates for the clipped image";
            default -> super.getDescriptionForKey(key);
        };
    }
}