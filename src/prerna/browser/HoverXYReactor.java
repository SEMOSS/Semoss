package prerna.browser;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class HoverXYReactor extends AbstractReactor {

	public HoverXYReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.X.getKey(), ReactorKeysEnum.Y.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		BrowserUtils.ensureUserLoggedIn(user);
		
		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}
		
		int x = BrowserUtils.getNonNullInt(this.keyValue, this.keysToGet[0]);
		int y = BrowserUtils.getNonNullInt(this.keyValue, this.keysToGet[1]);
		
		// We would map the x,y coordinate that hover in the UI to the browser being
		// rendered locally. 
		
		return null;
	}

}
