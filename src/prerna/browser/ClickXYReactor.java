package prerna.browser;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClickXYReactor extends AbstractReactor {
	
	private final static String REACTOR_DESCRIPTION = "Click on the x, y coordinate of the Browser App rendered on the server.";
	private final static String X_KEY_DESCRIPTION = "The X coordiante of the Browser App rendered on the server.";
	private final static String Y_KEY_DESCRIPTION = "The Y coordinate of the Browser App rendered on the server.";
	
	public ClickXYReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.X.getKey(), ReactorKeysEnum.Y.getKey()};
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		BrowserUtils.ensureUserLoggedIn(user);
		
		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}
		
		int x = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
		int y = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));
		
		// We would map the x,y coordinate that we clicked in the UI to the browser being
		// rendered locally. 
		
		return null;
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.X.getKey())) {
			return X_KEY_DESCRIPTION;
		} else if (key.equals(ReactorKeysEnum.Y.getKey())) {
			return Y_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
