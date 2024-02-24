package api;

import static org.junit.Assert.fail;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;

public class ApiTestUserUtils {

	public static void setDefaultTestUser() {
		ApiTests.user = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId("ater");
		at.setEmail("ater@ater.com");
		ApiTests.user.setAccessToken(at);
		ApiTests.user.setPrimaryLogin(AuthProvider.NATIVE);
		ApiTests.insight.setUser(ApiTests.user);
	}
	
	public static void addAndSetNewNativeUser(String userName, boolean isAdmin) {
		createUser(userName, isAdmin);
		setUser(userName);
	}
	
	private static void createUser(String userName, boolean isAdmin) {
		try {
			ApiTestEngineUtils.createUser(userName, "Native", isAdmin);
		} catch (Exception e) {
			System.out.println("Could not create User");
			fail(e.toString());
		}
	}
	
	public static void setUser(String userName) {
		ApiTests.user = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId(userName);
		at.setEmail(userName + "@" + userName + ".com");
		ApiTests.user.setAccessToken(at);
		ApiTests.user.setPrimaryLogin(AuthProvider.NATIVE);
		ApiTests.insight.setUser(ApiTests.user);
	}
	
}
