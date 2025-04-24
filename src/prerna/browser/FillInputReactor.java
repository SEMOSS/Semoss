package prerna.browser;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FillInputReactor extends AbstractReactor {
	
	private final static String REACTOR_DESCRIPTION = "Fill the currently selected input of the Browser App rendered on the server.";
	private final static String INPUT_KEY_DESCRIPTION = "The text to fill the currently selected input of the browser app rendered on the server.";
	
	public FillInputReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.INPUT.getKey()};
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
		
		String input = this.keyValue.get(this.keysToGet[0]);
		
		// Ideally, previous call would have been ClickXY and the form to be input would already
		// Be selected. That way, the next action would be. FillInputForm.
		
		Map<String, Object> actions = new HashMap<>();
		return null;
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.INPUT.getKey())) {
			return INPUT_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
