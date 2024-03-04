package api;

import static org.junit.Assert.fail;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;

public class ApiSemossTestUserUtils {
	
	private static User USER = null; 
	
	public static User getUser() {
		return USER;
	}

	public static void setDefaultTestUser() {
		setUser("ater");
	}
	
	public static void addAndSetNewNativeUser(String userName, boolean isAdmin) {
		createUser(userName, isAdmin);
		setUser(userName);
	}
	
	private static void createUser(String userName, boolean isAdmin) {
		try {
			ApiSemossTestEngineUtils.createUser(userName, "Native", isAdmin);
		} catch (Exception e) {
			System.out.println("Could not create User");
			fail(e.toString());
		}
	}
	
	public static void setUser(String userName) {
		USER = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId(userName);
		at.setEmail(userName + "@" + userName + ".com");
		USER.setAccessToken(at);
		USER.setPrimaryLogin(AuthProvider.NATIVE);
		ApiSemossTestInsightUtils.getInsight().setUser(USER);
	}
	
}
