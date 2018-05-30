package prerna.sablecc2.reactor;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TestingCACTempReactorToUse extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		if(this.insight.getUser2() != null) {
			User2 user2 = this.insight.getUser2();
			AccessToken cac = user2.getAccessToken(AuthProvider.CAC.toString());
			if(cac != null) {
				StringBuilder cacInfo = new StringBuilder();
				cacInfo.append("name: ").append(cac.getName()).append("\n");
				cacInfo.append("provider: ").append(cac.getProvider()).append("\n");
				return new NounMetadata(cacInfo.toString(), PixelDataType.CONST_STRING);
			} else {
				return new NounMetadata("User but no CAC", PixelDataType.CONST_STRING);
			}
			
		} else {
			return new NounMetadata("No user", PixelDataType.CONST_STRING);
		}
	}

}
