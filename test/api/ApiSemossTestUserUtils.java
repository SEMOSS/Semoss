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
		setUser(ApiTestsSemossConstants.USER_NAME, ApiTestsSemossConstants.USER_EMAIL);
	}
	
	public static void addAndSetNewNativeUser(String userName, String email, boolean isAdmin) {
		createUser(userName, email, isAdmin);
		setUser(userName, email);
	}
	
	private static void createUser(String userName, String email, boolean isAdmin) {
		try {
			ApiSemossTestEngineUtils.createUser(userName, email, "Native", isAdmin);
		} catch (Exception e) {
			System.out.println("Could not create User");
			fail(e.toString());
		}
	}
	
	public static void setUser(String userName, String email) {
		USER = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId(userName);
		at.setEmail(email);
		USER.setAccessToken(at);
		USER.setPrimaryLogin(AuthProvider.NATIVE);
		ApiSemossTestInsightUtils.getInsight().setUser(USER);
	}
	
}
