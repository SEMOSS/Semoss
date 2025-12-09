package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;

public class SecurityUpdateUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;

	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = AbstractSecurityUtils.securityDb;
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// addOAuthUser
	///

	@Test
	void testAddOAuthUser_newUserIdNull() {
		AccessToken at = new AccessToken();

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityUpdateUtils.addOAuthUser(at));
		assertEquals("User id for the token is null or empty. Must provide a valid id.", e.getMessage());
	}

	@Test
	void addOAuthUser_newUserEmpty() {
		AccessToken at = new AccessToken();
		at.setId("");

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityUpdateUtils.addOAuthUser(at));
		assertEquals("User id for the token is null or empty. Must provide a valid id.", e.getMessage());
	}

	@Test
	void addOAuthUser_UserAlreadyExists() {
		String userId = "admin1";
		String projectId = "project1";
		String projectName = "pname1";
		String engineId = "engine1";
		String engineName = "ename1";
		User user = UnitTestSecurityAuthUtils.createAdminAddedUser(userId, true);

		UnitTestSecurityAuthUtils.createProject(projectId, projectName, user);
		UnitTestSecurityAuthUtils.createEngine(engineId, engineName, user);

		AccessToken newAt = AccessToken.copyToken(user.getAccessToken(AuthProvider.NATIVE));
		newAt.setId("newId");

		assertFalse(SecurityUpdateUtils.addOAuthUser(newAt));

		// user still has old access token. Make sure it doesn't work anymore
		assertFalse(SecurityProjectUtils.userIsOwner(user, projectId));
		assertFalse(SecurityEngineUtils.userIsOwner(user, engineId));

		user.setAccessToken(newAt);

		assertTrue(SecurityProjectUtils.userIsOwner(user, projectId));
		assertTrue(SecurityEngineUtils.userIsOwner(user, engineId));
	}

	@Test
	void addOAuthUser_UserNotAddedByAdmin() {
		String userId = "admin1";
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken(userId);
		assertTrue(SecurityUpdateUtils.addOAuthUser(at));

		// assert user exists
		assertTrue(SecurityQueryUtils.checkUserExist("admin1id"));

		// verify user is not admin. Cannot add an admin via this way
		User user = new User();
		user.setAccessToken(at);
		assertNull(SecurityAdminUtils.getInstance(user));
	}

	///
	/// VALIDATE USER LOGIN
	///
	@Test
	void validateUserLogin_UsersNeverLoggedIn() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin1", true);
		AccessToken at = AccessToken.copyToken(user.getAccessToken(AuthProvider.NATIVE));

		Map<String, Collection<String>> map = new HashMap<>();
		map.put("meta", Arrays.asList("metavalue"));
		SecurityUserUtils.updateUserMetadata(at, map);

		SecurityUpdateUtils.validateUserLogin(at);

		// verify metadata got updated
		Map<String, Collection<String>> meta = at.getMeta();
		assertTrue(meta.containsKey("meta"));
		assertEquals("metavalue", meta.get("meta").stream().findFirst().get());

		// verify last logged in updated
		Object[] os = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset("admin1id", AuthProvider.NATIVE);
		assertNotNull(os[1]);
	}

	///
	/// UpdateOAuthUser
	///
	@Test
	void updateOAuthUser_successful() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken at = AccessToken.copyToken(user.getAccessToken(AuthProvider.NATIVE));
		at.setEmail("newemail@test.com");

		assertTrue(SecurityUpdateUtils.updateOAuthUser(at));

		// assert email was changed
		assertEquals("newemail@test.com", SecurityNativeUserUtils.getUserEmail("adminid"));
	}

	///
	/// LockUserAccount
	///
	@Test
	void lockUserAccount_SuccessfulLock() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken at = user.getAccessToken(AuthProvider.NATIVE);
		SecurityUpdateUtils.lockUserAccount(true, at.getId(), at.getProvider());

		Object[] os = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset("adminid", AuthProvider.NATIVE);
		assertNotNull(os[0]);
		assertTrue(Boolean.parseBoolean(os[0].toString()));
	}

	///
	/// UpdateUserLastLogin
	///
	@Test
	void updateUserLastLogin_successful() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken at = user.getAccessToken(AuthProvider.NATIVE);
		SecurityUpdateUtils.updateUserLastLogin(at.getId(), at.getProvider());

		Object[] os = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset("adminid", AuthProvider.NATIVE);
		assertNotNull(os[1]);
	}

	///
	/// RegisterUser
	///
	@Test
	void registerUser_successful() {
		String prefix = "admin";

		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken(prefix);

		SecurityUpdateUtils.registerUser(prefix + "id", prefix + "name", prefix + "@test.com", "Test123!",
				AuthProvider.NATIVE.getLabel(), "5555555555", "001", "US", true, false, false, null, null, null, null);

		// verify new user
		SecurityNativeUserUtils.userEmailExists("admin@test.com");
	}

}
