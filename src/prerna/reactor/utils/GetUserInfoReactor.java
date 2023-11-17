package prerna.reactor.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserInfoReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Map<String, Object> returnMap = new HashMap<String, Object>();
		User user = this.insight.getUser();
		if(user != null) {
			for (AuthProvider provider : user.getLogins()) {
				String providerName = provider.name();
				AccessToken token = user.getAccessToken(provider);
				Map<String, Object> providerMap = new HashMap<>();
				providerMap.put("id", token.getId() == null ? "null" : token.getId());
				providerMap.put("name", token.getName() == null ? "null" : token.getName());
				providerMap.put("username", token.getUsername() == null ? "null" : token.getUsername());
				providerMap.put("email", token.getEmail() == null ? "null" : token.getEmail());
				providerMap.put("lastPwdReset", token.getLastPasswordReset() == null ? "null" : token.getLastPasswordReset());
				Map<String, String> san = token.getSAN();
				providerMap.put("san", san);
				String groupType = token.getUserGroupType();
				Set<String> groups = token.getUserGroups();
				Map<String, Object> groupMap = new HashMap<>();
				groupMap.put("groupType", groupType);
				groupMap.put("groups", groups);
				providerMap.put("groupInfo", groupMap);
				returnMap.put(providerName, providerMap);
			}
		} else {
			returnMap.put("No User", "User is not logged in");
		}
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.MAP, PixelOperationType.USER_INFO);
		return noun;
	}

}
