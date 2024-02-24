package api;

import static org.junit.Assert.fail;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;

public class ApiSemossTestUserUtils {

	public static void setDefaultTestUser() {
		BaseSemossApiTests.user = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId("ater");
		at.setEmail("ater@ater.com");
		BaseSemossApiTests.user.setAccessToken(at);
		BaseSemossApiTests.user.setPrimaryLogin(AuthProvider.NATIVE);
		BaseSemossApiTests.insight.setUser(BaseSemossApiTests.user);
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
		BaseSemossApiTests.user = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId(userName);
		at.setEmail(userName + "@" + userName + ".com");
		BaseSemossApiTests.user.setAccessToken(at);
		BaseSemossApiTests.user.setPrimaryLogin(AuthProvider.NATIVE);
		BaseSemossApiTests.insight.setUser(BaseSemossApiTests.user);
	}
	
}
