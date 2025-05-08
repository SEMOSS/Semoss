package prerna.testing;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

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
	
	public static void clearUserDirectory() throws IOException {
		Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "user");
		if (Files.exists(p)) {
			FileUtils.cleanDirectory(p.toFile());
		}
	}
	
	public static Path getAssetsPath() {
		String id = USER.getAssetProjectId(USER.getPrimaryLogin());
		String assetId = "Asset__" + id;
		Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "user", assetId, "app_root", "version", "assets");
		return p;
	}
	
}
