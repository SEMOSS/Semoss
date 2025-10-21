package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;

class SecurityQueryUtilsUnitTests extends AbstractSecurityUtilsUnitTests {

	private RDBMSNativeEngine securityDb;

	private String testUserId;
	private String testUsername;
	private String testEmail;
	private String testDisplayName;
	private String testEngineId;
	private String testEngineName;
	private String testGlobalEngineId;
	private String testGlobalEngineName;
	private String testProjectId;
	private String testInsightId;
	private String testInsightName;
	private Timestamp testLastLogin;
	private Timestamp testLastPasswordReset;
	private Timestamp testInsightLastModified;
	private int testPermissionLevel;

	@BeforeEach
	void setUp() throws Exception {
		this.securityDb = AbstractSecurityUtils.securityDb;
		assertNotNull(this.securityDb, "Security database should be initialized by AbstractSecurityUtilsUnitTests");
		seedTestData();
	}

	@org.junit.jupiter.api.AfterEach
	void cleanupTestData() throws Exception {
		if (this.securityDb == null) {
			return;
		}
		executeUpdate("DELETE FROM ENGINEPERMISSION WHERE USERID = ? AND ENGINEID = ?", testUserId, testEngineId);
		executeUpdate("DELETE FROM INSIGHT WHERE INSIGHTID = ?", testInsightId);
		executeUpdate("DELETE FROM PROJECT WHERE PROJECTID = ?", testProjectId);
		executeUpdate("DELETE FROM ENGINE WHERE ENGINEID IN (?, ?)", testEngineId, testGlobalEngineId);
		executeUpdate("DELETE FROM SMSS_USER WHERE ID = ?", testUserId);
	}

	@Test
	void testUserEngineIdForAliasReturnsEngineWithExplicitPermission() throws Exception {
		User user = buildTestUser();
		String resolved = SecurityQueryUtils.testUserEngineIdForAlias(user, testEngineName);
		assertEquals(testEngineId, resolved);
	}

	@Test
	void testUserEngineIdForAliasFallsBackToGlobalEngine() throws Exception {
		String resolved = SecurityQueryUtils.testUserEngineIdForAlias(null, testGlobalEngineName);
		assertEquals(testGlobalEngineId, resolved);
	}

	@Test
	void testGetInsightNameForIdMatchesDatabase() throws Exception {
		String actualName = SecurityQueryUtils.getInsightNameForId(testProjectId, testInsightId);
		assertEquals(testInsightName, actualName);
	}

	@Test
	void testGetLastModifiedDateForInsightInProjectReturnsMaxTimestamp() throws Exception {
		SemossDate expected = toSemossDate(testInsightLastModified);
		SemossDate actual = SecurityQueryUtils.getLastModifiedDateForInsightInProject(testProjectId);
		assertNotNull(actual);
		assertEquals(expected.toString(), actual.toString());
	}

	@Test
	void testGetUserInfoReturnsDatabaseValues() throws Exception {
		Map<String, Map<String, Object>> info = SecurityQueryUtils
				.getUserInfo(Collections.singletonList(testUserId));

		assertTrue(info.containsKey(testUserId));
		Map<String, Object> details = info.get(testUserId);
		assertEquals(testUserId, details.get("ID"));
		assertEquals(testDisplayName, details.get("NAME"));
		assertEquals(testUsername, details.get("USERNAME"));
		assertEquals(testEmail, details.get("EMAIL"));
		assertEquals("NATIVE", details.get("TYPE"));
		assertEquals("false", details.get("ADMIN"));
		assertEquals("true", details.get("PUBLISHER"));
		assertEquals("true", details.get("EXPORTER"));
		assertEquals("123-456-7890", details.get("PHONE"));
		assertEquals("001", details.get("PHONEEXTENSION"));
		assertEquals("US", details.get("COUNTRYCODE"));
	}

	@Test
	void testGetApplicationUserCountMatchesDirectSql() throws Exception {
		int actual = SecurityQueryUtils.getApplicationUserCount();
		assertEquals(1, actual);
	}

	@Test
	void testUserIsPublisherIdentifiesPublisher() throws Exception {
		User user = buildTestUser();
		assertTrue(SecurityQueryUtils.userIsPublisher(user));
	}

	@Test
	void testUserIsExporterIdentifiesExporter() throws Exception {
		User user = buildTestUser();
		assertTrue(SecurityQueryUtils.userIsExporter(user));
	}

	@Test
	void testSearchForUserReturnsMatchingUser() throws Exception {
		List<Map<String, Object>> results = SecurityQueryUtils.searchForUser(testDisplayName);
		assertTrue(results.stream().anyMatch(entry -> testUserId.equalsIgnoreCase(Objects.toString(entry.get("id")))));
	}

	@Test
	void testCheckUserExistById() throws Exception {
		assertTrue(SecurityQueryUtils.checkUserExist(testUserId));
	}

	@Test
	void testCheckUserExistByCredentials() throws Exception {
		assertTrue(SecurityQueryUtils.checkUserExist(testUsername, testEmail));
	}

	@Test
	void testCheckUserEmailExist() throws Exception {
		assertTrue(SecurityQueryUtils.checkUserEmailExist(testEmail));
	}

	@Test
	void testCheckUsernameExist() throws Exception {
		assertTrue(SecurityQueryUtils.checkUsernameExist(testUsername));
	}

	@Test
	void testIsUserTypeMatchesDatabase() throws Exception {
		assertTrue(SecurityQueryUtils.isUserType(testUserId, AuthProvider.NATIVE));
	}

	@Test
	void testGetUserLockAndLastLoginAndLastPassResetReflectsDatabase() throws Exception {
		Object[] details = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset(testUserId, AuthProvider.NATIVE);
		assertNotNull(details);
		assertEquals(false, toBoolean(details[0]));
		assertEquals(testLastLogin.toString().substring(0, testLastLogin.toString().lastIndexOf(".")), details[1].toString());
		assertEquals(testLastPasswordReset.toString().substring(0, testLastPasswordReset.toString().lastIndexOf(".")), details[2].toString());
	}

	@Test
	void testGetEnginePermissionReturnsPermissionDetails() throws Exception {
		Map<String, Object> permission = SecurityQueryUtils.getEnginePermission(testUserId, testEngineId);

		assertNotNull(permission);
		assertEquals(Integer.toString(testPermissionLevel),
				Objects.toString(permission.getOrDefault("ENGINEPERMISSION__PERMISSION", permission.get("PERMISSION"))));
	}

	private void seedTestData() throws SQLException {
		String suffix = Long.toString(System.nanoTime());
		testUserId = "user-" + suffix;
		testUsername = "username_" + suffix;
		testEmail = "user_" + suffix + "@example.com";
		testDisplayName = "Test User " + suffix;
		testEngineId = "engine-" + suffix;
		testEngineName = "Engine " + suffix;
		testGlobalEngineId = "engine-global-" + suffix;
		testGlobalEngineName = "Global Engine " + suffix;
		testProjectId = "project-" + suffix;
		testInsightId = "insight-" + suffix;
		testInsightName = "Insight " + suffix;
		Instant baseInstant = Instant.parse("2024-01-01T00:00:00Z");
		testLastLogin = Timestamp.from(baseInstant);
		testLastPasswordReset = Timestamp.from(baseInstant.minusSeconds(3600));
		testInsightLastModified = Timestamp.from(baseInstant.plusSeconds(7200));
		testPermissionLevel = 3;

		executeUpdate(
				"INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER, EXPORTER, PHONE, PHONEEXTENSION, COUNTRYCODE, LOCKED, LASTLOGIN, LASTPASSWORDRESET) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				testUserId, testDisplayName, testUsername, testEmail, "NATIVE", Boolean.FALSE, Boolean.TRUE,
				Boolean.TRUE, "123-456-7890", "001", "US", Boolean.FALSE, testLastLogin, testLastPasswordReset);

		executeUpdate(
				"INSERT INTO PROJECT (PROJECTID, PROJECTNAME, GLOBAL, DISCOVERABLE, DATECREATED) VALUES (?, ?, ?, ?, ?)",
				testProjectId, "Project " + suffix, Boolean.FALSE, Boolean.TRUE, Timestamp.from(baseInstant));

		executeUpdate(
				"INSERT INTO INSIGHT (INSIGHTID, PROJECTID, INSIGHTNAME, LASTMODIFIEDON) VALUES (?, ?, ?, ?)",
				testInsightId, testProjectId, testInsightName, testInsightLastModified);

		executeUpdate(
				"INSERT INTO ENGINE (ENGINEID, ENGINENAME, GLOBAL, DISCOVERABLE, DATECREATED) VALUES (?, ?, ?, ?, ?)",
				testEngineId, testEngineName, Boolean.FALSE, Boolean.TRUE, Timestamp.from(baseInstant));

		executeUpdate(
				"INSERT INTO ENGINE (ENGINEID, ENGINENAME, GLOBAL, DISCOVERABLE, DATECREATED) VALUES (?, ?, ?, ?, ?)",
				testGlobalEngineId, testGlobalEngineName, Boolean.TRUE, Boolean.TRUE, Timestamp.from(baseInstant));

		executeUpdate(
				"INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, FAVORITE, DATEADDED) VALUES (?, ?, ?, ?, ?, ?)",
				testUserId, testEngineId, testPermissionLevel, Boolean.TRUE, Boolean.FALSE,
				Timestamp.from(baseInstant.plusSeconds(10800)));
	}

	private void executeUpdate(String sql, Object... params) throws SQLException {
		try (Connection connection = securityDb.getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {
			connection.setAutoCommit(true);
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}

	private User buildTestUser() {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.NATIVE);
		token.setId(testUserId);
		token.setEmail(testEmail);
		token.setUsername(testUsername);
		token.init();

		User user = new User();
		user.setAccessToken(token);
		return user;
	}

	private boolean toBoolean(Object value) {
		if (value == null) {
			return false;
		}
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue() != 0;
		}
		return Boolean.parseBoolean(value.toString());
	}

	private SemossDate toSemossDate(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof SemossDate) {
			return (SemossDate) value;
		}
		if (value instanceof Timestamp) {
			return new SemossDate(((Timestamp) value).toInstant());
		}
		if (value instanceof java.util.Date) {
			return new SemossDate((java.util.Date) value);
		}
		if (value instanceof Number) {
			return new SemossDate(((Number) value).longValue());
		}
		if (value instanceof Instant) {
			return new SemossDate((Instant) value);
		}
		return new SemossDate(value.toString(), "yyyy-MM-dd HH:mm:ss");
	}
}
