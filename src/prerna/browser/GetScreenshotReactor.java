package prerna.browser;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
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
		
		return null;
	}

	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}
	
}
