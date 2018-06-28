package prerna.auth;

import java.util.Hashtable;

import prerna.om.AbstractValueObject;

public class AppTokens extends AbstractValueObject{
	
	// name of this user in the SEMOSS system if there is one
	
	public static AppTokens app = new AppTokens();
	
	private AppTokens() {
		
	}
	
	public static AppTokens getInstance() {
		return app;
	}
	
	// need to have an access token store
	Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<AuthProvider, AccessToken>();
	
	
	public void setAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		accessTokens.put(name, value);
	}
	
	public AccessToken getAccessToken(AuthProvider name) {
		return accessTokens.get(name);
	}
	
	public void dropAccessToken(AuthProvider name) {
		accessTokens.remove(name);
	}

}
