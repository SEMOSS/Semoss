package prerna.usertracking;

import prerna.auth.AuthProvider;
import prerna.auth.User;

public interface IUserTracking {

	String GEO_IP2 = "GEOIP2";
	
	void registerLogin(String sessionId, String ip, User user, AuthProvider ap);
	
	void registerLogout(String sessionId);

}
