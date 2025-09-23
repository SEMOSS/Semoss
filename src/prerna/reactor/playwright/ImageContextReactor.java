package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.Map;

//import com.microsoft.playwright.Page;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.model.VisionReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ImageContextReactor extends AbstractReactor {
    
    public ImageContextReactor() {
        this.keysToGet = new String[] {
            "sessionId",
            ReactorKeysEnum.ENGINE.getKey(),
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 1 };
    }
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String engineId = this.keyValue.get(this.keysToGet[1]);
        Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
        
        String userPrompt = (String) paramValues.get("userPrompt");
        
        Map<String, Object> result = analyzeCroppedImageWithVision(sessionId, engineId, paramValues, userPrompt);
        
        return new NounMetadata(result, PixelDataType.MAP);
    }
    public Map<String, Object> analyzeCroppedImageWithVision(String sessionId, String engineId, Map<String, Object> paramValues, String userPrompt) {
        try {
            ScreenshotReactor screenshotReactor = new ScreenshotReactor();
            screenshotReactor.setInsight(this.insight);
            screenshotReactor.setNounStore(this.store);
            
            // add sessionId parameter to the noun store
            GenRowStruct sessionGrs = this.store.makeNoun(ReactorKeysEnum.SESSION_ID.getKey());
            sessionGrs.add(new NounMetadata(sessionId, PixelDataType.CONST_STRING));
            
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
            
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("image_encoded", croppedImage.base64Png());
            
            Map<String, Object> modelOutput = modelEngine.ask(instruction, null, this.insight, paramMap).toMap();
            
            Map<String, Object> result = new HashMap<>();
            result.put("response", modelOutput.get("response"));
            
            return result;
            
        } catch (Exception e) {
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("response", "Error: " + e.getMessage());
            return errorResult;
        }
    }
}