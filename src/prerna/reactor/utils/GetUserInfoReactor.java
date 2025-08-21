package prerna.reactor.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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
		Map<String, Object> returnMap = new TreeMap<String, Object>();
		User user = this.insight.getUser();
		if(user != null) {
			String userEpoch = user.getUserEpoch();
			for (AuthProvider provider : user.getLogins()) {
				String providerName = provider.name();
				AccessToken token = user.getAccessToken(provider);
				
				// add basic user details we capture
				Map<String, Object> providerMap = new TreeMap<>();
				providerMap.put("id", token.getId() == null ? "null" : token.getId());
				providerMap.put("name", token.getName() == null ? "null" : token.getName());
				providerMap.put("username", token.getUsername() == null ? "null" : token.getUsername());
				providerMap.put("email", token.getEmail() == null ? "null" : token.getEmail());
				providerMap.put("lastPwdReset", token.getLastPasswordReset() == null ? "null" : token.getLastPasswordReset());
				providerMap.put("lastLogin", token.getLastLogin() == null ? "null" : token.getLastLogin());

				// get extended user properties
				Map<String, Collection<String>> meta = token.getMeta();
				providerMap.put("meta", meta);
				
				// add san info
				Map<String, String> san = token.getSAN();
				providerMap.put("san", san);
				
				// add group info
				List<Map<String, Object>> groupList = new ArrayList<>();
				Map<String, Object> groupMap = new HashMap<>();
				String groupType = token.getUserGroupType();
				Collection<String> groups = token.getUserGroups();
				groupMap.put("groupType", groupType);
				groupMap.put("groups", groups);
				groupList.add(groupMap);
				// add custom group info
				Collection<String> customGroups = token.getUserCustomGroups();
				if (customGroups != null && !customGroups.isEmpty()) {
					Map<String, Object> customGroupMap = new HashMap<>();
					customGroupMap.put("groupType", "CUSTOM");
					customGroupMap.put("groups", customGroups);
					groupList.add(customGroupMap);
				}
				providerMap.put("groupInfo", groupList);
				
				// add user epoch into the login map
				providerMap.put("userEpoch", userEpoch);
				
				// add the entire map
				returnMap.put(providerName, providerMap);
			}
		} else {
			returnMap.put("No User", "User is not logged in");
		}
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.MAP, PixelOperationType.USER_INFO);
		return noun;
	}

}
