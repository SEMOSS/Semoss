package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloseURLReactor extends AbstractReactor {

	private final static String REACTOR_DESCRIPTION = "Close the URL of the Browser App rendered on the server.";
	private final static String URL_KEY_DESCRIPTION = "A URL address to close on the Browser App rendered on the server.";

	public CloseURLReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.URL.getKey()};
		this.keyRequired = new int[] { 1 };
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
		
		Map<String, Object> actions = new HashMap<>();
		return null;
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
