package prerna.auth.utils;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SecurityPasswordResetUtilsUnitTests extends AbstractSecurityUtilsUnitTests {
	static String id = "test123";
	static String email = "test123@test.com";
	static String type = "NATIVE";
	
	@BeforeAll
	static void setUp() {
		// add user to security DB
		String name = "Test User";
		String password = "password123";
		String phone = "5551234567";
		String phoneextension = "001";
		String countrycode = "US";
		boolean admin = true;
		boolean publisher = false;
		boolean exporter = false;
		String modelUsageRestriction = null;
		String modelUsageFrequency = null;
		Integer modelMaxTokens = null;
		Double modelMaxResponseTime = null;

		boolean success = SecurityUpdateUtils.registerUser(id, name, email, password, type, phone, phoneextension,
				countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency, modelMaxTokens,
				modelMaxResponseTime);

		assertTrue(success, "Insertion of new user should be successful");
	}
	
	@Test
	void testUserEmailExists() throws Exception {
		boolean exists = SecurityPasswordResetUtils.userEmailExists(email, type);
		assertTrue(exists);

		exists = SecurityPasswordResetUtils.userEmailExists("test234", type);
		assertFalse(exists);
	}

	@Test
	void testGetUserIdFromEmail() throws Exception {
		String userId = SecurityPasswordResetUtils.getUserIdFromEmail(email, type);
		assertEquals(id, userId);

		userId = SecurityPasswordResetUtils.getUserIdFromEmail("invalid", type);
		assertEquals(null, userId);
	}

	@Test
	void testAllowUserResetPassword() throws Exception {
		String token = SecurityPasswordResetUtils.allowUserResetPassword(email, type);
		assertTrue(token != null && !token.isEmpty());

		try {
			SecurityPasswordResetUtils.allowUserResetPassword("test2343", type);
			fail("invalid email");
		} catch (IllegalArgumentException e) {
			assertEquals("The email 'test2343' does not exist for provider NATIVE", e.getMessage());
		}

		try {
			SecurityPasswordResetUtils.allowUserResetPassword("test2343", "GOOGLE");
			fail("invalid auth type");
		} catch (IllegalArgumentException e) {
			assertEquals("Cannot reset password for type = 'GOOGLE'", e.getMessage());
		}
	}

	@Test
	void testUserResetPassword() throws Exception {
		String token = SecurityPasswordResetUtils.allowUserResetPassword(email, type);
		String newPassword = "newPass123456!";
		Map<String, Object> retMap = SecurityPasswordResetUtils.userResetPassword(token, newPassword);
		assertFalse(retMap.isEmpty());
		assertEquals(id, retMap.get("userId"));
		assertEquals(email, retMap.get("email"));
	}

	@Test
	void testDeleteToken() throws Exception {
		String token = SecurityPasswordResetUtils.allowUserResetPassword(email, type);
		String newPassword = "newPass123!";
		Map<String, Object> retMap = SecurityPasswordResetUtils.userResetPassword(token, newPassword);
		assertFalse(retMap.isEmpty());
		assertEquals(id, retMap.get("userId"));
		assertEquals(email, retMap.get("email"));

		SecurityPasswordResetUtils.deleteToken(token);

		// make sure this is empty
		try {
			retMap = SecurityPasswordResetUtils.userResetPassword(token, newPassword);
			fail("invalid");
		} catch (Exception e) {
			assertEquals("Invalid attempt trying to update password", e.getMessage());
		}
	}


}
