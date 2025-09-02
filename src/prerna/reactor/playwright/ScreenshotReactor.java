package prerna.reactor.playwright;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;


public class ScreenshotReactor extends AbstractReactor{
	
    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	Browser browser;
	
	public ScreenshotReactor(){
		this.keysToGet = new String[] {
				"sessionId"
				};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		return new NounMetadata(screenshot(sessionId), PixelDataType.MAP);
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

}
