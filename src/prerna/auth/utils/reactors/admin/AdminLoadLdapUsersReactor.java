package prerna.auth.utils.reactors.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;
import prerna.util.ldap.ILdapAuthenticator;

public class AdminLoadLdapUsersReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminLoadLdapUsersReactor.class);

	public AdminLoadLdapUsersReactor() {
		this.keysToGet = new String[] {"searchContextName", "searchFilter", "searchContextScope", "authProvider"};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		String searchContextName = this.keyValue.get(this.keysToGet[0]);
		String searchFilter = this.keyValue.get(this.keysToGet[1]);
		int searchContextScope = -1;
		String searchContextScopeStr = this.keyValue.get(this.keysToGet[2]);
		if(searchContextScopeStr != null && !(searchContextScopeStr=searchContextScopeStr.trim()).isEmpty()) {
			try {
				searchContextScope = Integer.parseInt(searchContextScopeStr);
			} catch(NumberFormatException nfe) {
				throw new IllegalArgumentException(this.keysToGet[2] + " must be an integer value. Received input = " + searchContextScopeStr);
			}
		}
		
		AuthProvider newP = null;
		String newProvider = this.keyValue.get(this.keysToGet[3]);
		if(newProvider != null && !(newProvider=newProvider.trim()).isEmpty()) {
			try {
				newP = AuthProvider.valueOf(newProvider);
			} catch(Exception e) {
				throw new IllegalArgumentException("New provider " + newProvider + " is not a valid auth provider");
			}
		}
		
		List<AccessToken> foundUsers = new ArrayList<>();
		ILdapAuthenticator authenticator;
		try {
			authenticator = SocialPropertiesUtil.getInstance().getLdapAuthenticator();
			foundUsers = authenticator.findUsers(searchContextName, searchFilter, searchContextScope);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
		
		List<AccessToken> addedUsers = new ArrayList<>();
		
		for(AccessToken newUser : foundUsers) {
			if(newP != null) {
				newUser.setProvider(newP);
			}
			boolean newUserAdded = SecurityUpdateUtils.addOAuthUser(newUser);
			if(newUserAdded) {
				addedUsers.add(newUser);
			}
		}
		
		Map<String, List<AccessToken>> retMap = new HashMap<>();
		retMap.put("addedUsers", addedUsers);
		retMap.put("foundUsers", foundUsers);
		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
