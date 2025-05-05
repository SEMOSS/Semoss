package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetScreenshotReactor extends AbstractReactor {
	
	private final static String REACTOR_DESCRIPTION = "Get the current screenshot of the Browser App rendered on the server.";

	public GetScreenshotReactor() {
		
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		BrowserUtils.ensureUserLoggedIn(user);
		
		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}
		
		
		/**
		 * Call the playwright browser and run method in playwright that returns screenshot of 
		 * current page on browser. Return that value.
		 */
		Map<String, Object> actions = new HashMap<>();
		
		actions.put("actor", "system");
		actions.put("action", "screenshot");
		
		String json = BrowserUtils.mapToJsonString(actions);
		
		JSONObject jo = new JSONObject(json);
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			throw new IllegalArgumentException("There is no Playwright Browser currently open for this insight.");
		}
		String file = pbu.getScreenShot();

		return new NounMetadata(file, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}
	
}
