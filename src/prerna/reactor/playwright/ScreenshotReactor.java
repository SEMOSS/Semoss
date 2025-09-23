package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Page;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

//modified to accept either normal screenshot or a cropped one
//cropped
// Screenshot(sessionId="${sessionId}",
// paramValues=[{"startX": ${cropArea.startX}, "startY": ${cropArea.startY}, "endX": ${cropArea.endX},"endY": ${cropArea.endY} }])
// normal
// Screenshot(sessionId="${sessionId}")
public class ScreenshotReactor extends AbstractReactor{
	
    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	private static final Logger classLogger = LogManager.getLogger(ScreenshotReactor.class);

	public ScreenshotReactor(){
		this.keysToGet = new String[] {
				"sessionId",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()  
				};
		this.keyRequired = new int[] { 1, 0 };  // extra parameters optional
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		
		// check if crop params are provided
		Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
		
		if (paramValues != null && paramValues.containsKey("startX")) {
			//log the crop params
			classLogger.info("Crop params provided.");
			classLogger.info("Crop params: " + paramValues.toString());
			// cropped screenshot
			int startX = ((Number) paramValues.get("startX")).intValue();
			int startY = ((Number) paramValues.get("startY")).intValue();
			int endX = ((Number) paramValues.get("endX")).intValue();
			int endY = ((Number) paramValues.get("endY")).intValue();
			
			return new NounMetadata(croppedScreenshot(sessionId, startX, startY, endX, endY), PixelDataType.MAP);
		} else {
			// normal screenshot
			return new NounMetadata(screenshot(sessionId), PixelDataType.MAP);
		}
	}
	
	public static ScreenshotResponse screenshot(String sessionId) {
        Session s = SessionReactor.get(sessionId);
        byte[] buf = s.page.screenshot(new Page.ScreenshotOptions().setFullPage(false));
        String b64 = java.util.Base64.getEncoder().encodeToString(buf);

        int vpW = s.page.viewportSize().width;
        int vpH = s.page.viewportSize().height;

        Object raw = s.page.evaluate("() => Number.isFinite(window.devicePixelRatio) ? window.devicePixelRatio : 1");
        double dpr = (raw instanceof Number) ? ((Number) raw).doubleValue() : 1.0;

        return new ScreenshotResponse(b64, vpW, vpH, dpr);
    }
    
    public static ScreenshotResponse croppedScreenshot(String sessionId, int startX, int startY, int endX, int endY) {
        Session s = SessionReactor.get(sessionId);
        
        int x = Math.min(startX, endX);
        int y = Math.min(startY, endY);  
        int width = Math.abs(endX - startX);
        int height = Math.abs(endY - startY);
        
        byte[] buf = s.page.screenshot(new Page.ScreenshotOptions()
            .setFullPage(false)
            .setClip(x, y, width, height));
            
        String b64 = java.util.Base64.getEncoder().encodeToString(buf);
        
        return new ScreenshotResponse(b64, width, height, 1.0);
    }
}