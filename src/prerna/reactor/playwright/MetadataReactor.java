package prerna.reactor.playwright;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class MetadataReactor extends AbstractReactor {
	

	@Override
	public NounMetadata execute() {
		return new NounMetadata(getMetadata(), PixelDataType.MAP);
	}
	
	private UserMetadata getMetadata() {
		User user = this.insight.getUser();
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		UserMetadata userMetadata = new UserMetadata(token.getName(), token.getEmail(), token.getId(), user.getZoneId().toString());
		return userMetadata;
	}

}
