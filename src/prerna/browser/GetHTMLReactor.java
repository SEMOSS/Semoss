package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetHTMLReactor extends AbstractReactor {

	private final static String REACTOR_DESCRIPTION = "Get the current HTML of the Browser App rendered on the server.";

	public GetHTMLReactor() {
		
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
		 * Call the playwright browser and run method in playwright that returns HTML of 
		 * current page on browser. Return that value.
		 */
		Map<String, Object> actions = new HashMap<>();
		
		actions.put("actor", "system");
		actions.put("action", "getHTML");
		
		String json = BrowserUtils.mapToJsonString(actions);
		
		JSONObject jo = new JSONObject(json);
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			throw new IllegalArgumentException("There is no Playwright Browser currently open for this insight.");
		}
		String html = pbu.getHTML();

		return new NounMetadata(html, PixelDataType.CONST_STRING);
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

}
