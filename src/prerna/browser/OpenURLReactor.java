package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenURLReactor extends AbstractReactor {
	
	public OpenURLReactor() {
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
		
		Map<String, Object> actions = new HashMap<>();
		return null;
	}

}
