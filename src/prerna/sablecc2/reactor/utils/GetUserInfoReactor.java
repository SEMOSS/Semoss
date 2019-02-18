package prerna.sablecc2.reactor.utils;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetUserInfoReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Map<String, Object> returnMap = new HashMap<String, Object>();
		User user = this.insight.getUser();
		for (AuthProvider provider : user.getLogins()) {
			String providerName = provider.name();
			AccessToken token = user.getAccessToken(provider);
			Map<String, Object> providerMap = new HashMap<String, Object>();
			providerMap.put("name", token.getName() == null ? "null" : token.getName());
			providerMap.put("email", token.getEmail() == null ? "null" : token.getEmail());
			returnMap.put(providerName, providerMap);
		}
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.MAP, PixelOperationType.USER_INFO);
		return noun;
	}

}
