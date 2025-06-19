package prerna.reactor.browser;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloseURLReactor extends AbstractReactor {

	private final static String REACTOR_DESCRIPTION = "Close the URL of the Browser App rendered on the server.";
	private final static String URL_KEY_DESCRIPTION = "A URL address to close on the Browser App rendered on the server.";

	public CloseURLReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.URL.getKey()};
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		BrowserUtils.ensureUserLoggedIn(user);
		
		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}
		
		String url = this.keyValue.get(this.keysToGet[0]);
		
		/**
		 * Not sure URL is really needed. Won't we only have one browser open per user?
		 * So in this case, we simply just close the open browser. 
		 * Edge case may be opening tabs or something like this.
		 */
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			throw new IllegalArgumentException("There is no Playwright Browser currently open for this insight.");
		}	
		Map<String, Object> actions = new HashMap<>();
		actions.put("actor", "system");
		actions.put("action", "close");
		pbu.close();

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return URL_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
