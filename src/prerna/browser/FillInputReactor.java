package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FillInputReactor extends AbstractReactor {
	
	public FillInputReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.INPUT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		BrowserUtils.ensureUserLoggedIn(user);
		
		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}
		
		String input = BrowserUtils.getNonNullString(this.keyValue, this.keysToGet[0]);
		
		// Ideally, previous call would have been ClickXY and the form to be input would already
		// Be selected. That way, the next action would be. FillInputForm.
		
		Map<String, Object> actions = new HashMap<>();
		return null;
	}
}
