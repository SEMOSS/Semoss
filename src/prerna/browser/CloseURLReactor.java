package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloseURLReactor extends AbstractReactor {

	public CloseURLReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.URL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		BrowserUtils.ensureUserLoggedIn(user);
		
		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}
		
		String url = BrowserUtils.getNonNullString(this.keyValue, this.keysToGet[0]);
		
		/**
		 * Not sure URL is really needed. Won't we only have one browser open per user?
		 * So in this case, we simply just close the open browser. 
		 * Edge case may be opening tabs or something like this.
		 */
		
		Map<String, Object> actions = new HashMap<>();
		return null;
	}
}
