package prerna.reactor.security;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserMetadataReactor extends AbstractReactor {
	
	public GetUserMetadataReactor() {
		this.keysToGet = new String[]{"userId", ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		
		String userId = StringUtils.trimToNull(this.keyValue.get("userId"));
		String userType = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.PROVIDER.getKey()));
		AuthProvider provider = AuthProvider.getProviderFromString(userType);
		
		if(userId == null) {
			AccessToken accessToken = null;
			if(userType == null) {
				accessToken = user.getPrimaryLoginToken();
			} else {
				accessToken = user.getAccessToken(provider);
				if(accessToken == null) {
					throw new IllegalArgumentException("Login for given provider not active in session");
				}
			}
			userId = accessToken.getId();
			provider = accessToken.getProvider();
		} else {
			if(userType == null) {
				throw new IllegalArgumentException("Provider parameter required");
			}
			AccessToken tokenForProvider = user.getAccessToken(provider);
			if(!SecurityAdminUtils.userIsAdmin(user) && (tokenForProvider == null || !userId.equalsIgnoreCase(tokenForProvider.getId()))) {
				throw new IllegalArgumentException("User does not have access to the requested user");
			}
		}
		
		Map<String, Object> userInfo = new HashMap<>();
		userInfo.putAll(SecurityUserUtils.getAggregateUserMetadata(userId, provider, getMetaKeys(), false));
		return new NounMetadata(userInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.USER_INFO);
	}
	
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}
	
	@Override
	public String getReactorDescription() {
		return "Retrieve metadata on a user";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("userId")) {
			return "ID of the SMSS_USER entry for which the metadata is being retrieved";
		}
		return super.getDescriptionForKey(key);
	}

}
