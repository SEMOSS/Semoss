package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.QueryExecutionUtility;
import prerna.rdf.engine.wrappers.WrapperManager;

class SecurityQueryUtilsUnitTests {

	private RDBMSNativeEngine securityDbMock;

	@BeforeEach
	void setUp() {
		securityDbMock = mock(RDBMSNativeEngine.class);
		AbstractSecurityUtils.securityDb = securityDbMock;
	}

	@AfterEach
	void tearDown() {
		AbstractSecurityUtils.securityDb = null;
	}

	@Test
	void testUserEngineIdForAliasReturnsResolvedIdWhenPermissionMatchFound() {
		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(Collections.singletonList("engine-123"));

			String resolved = SecurityQueryUtils.testUserEngineIdForAlias(null, "engine-alias");

			assertEquals("engine-123", resolved);
			qeu.verify(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)),
					times(1));
		}
	}

	@Test
	void testUserEngineIdForAliasChecksGlobalVisibilityWhenNoDirectMatch() {
		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(Collections.emptyList(), Collections.singletonList("engine-global"));

			String resolved = SecurityQueryUtils.testUserEngineIdForAlias(null, "engine-alias");

			assertEquals("engine-global", resolved);
			qeu.verify(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)),
					times(2));
		}
	}

	@Test
	void testUserEngineIdForAliasThrowsOnAmbiguousAlias() {
		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(Arrays.asList("engine-a", "engine-b"));

			IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
					() -> SecurityQueryUtils.testUserEngineIdForAlias(null, "engine-alias"));

			assertTrue(ex.getMessage().contains("There are 2 databases"));
		}
	}

	@Test
	void testGetInsightNameForIdReturnsNameWhenFound() {
		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(Collections.singletonList("My Insight"));

			String insightName = SecurityQueryUtils.getInsightNameForId("proj-1", "insight-1");

			assertEquals("My Insight", insightName);
		}
	}

	@Test
	void testGetInsightNameForIdReturnsNullWhenMissing() {
		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushToListString(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(Collections.emptyList());

			assertNull(SecurityQueryUtils.getInsightNameForId("proj-1", "insight-404"));
		}
	}

	@Test
	void testGetLastModifiedDateForInsightInProjectReturnsResult() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		IHeadersDataRow rowMock = mock(IHeadersDataRow.class);
		SemossDate expectedDate = new SemossDate(LocalDate.of(2024, 10, 21));

		when(wrapperMock.hasNext()).thenReturn(true);
		when(wrapperMock.next()).thenReturn(rowMock);
		when(rowMock.getValues()).thenReturn(new Object[] { expectedDate });

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			SemossDate result = SecurityQueryUtils.getLastModifiedDateForInsightInProject("proj-1");

			assertSame(expectedDate, result);
		}
	}

	@Test
	void testGetLastModifiedDateForInsightInProjectReturnsNullWhenNoRows() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(false);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertNull(SecurityQueryUtils.getLastModifiedDateForInsightInProject("proj-1"));
		}
	}

	@Test
	void testGetUserInfoBuildsUserMapFromWrapper() throws Exception {
		String[] headers = { "SMSS_USER__ID", "SMSS_USER__NAME", "SMSS_USER__USERNAME", "SMSS_USER__EMAIL",
				"SMSS_USER__TYPE", "SMSS_USER__ADMIN", "SMSS_USER__PUBLISHER", "SMSS_USER__EXPORTER",
				"SMSS_USER__PHONE", "SMSS_USER__PHONEEXTENSION", "SMSS_USER__COUNTRYCODE" };
		Object[] values = { "user-1", "Test User", "tuser", "user@example.com", AuthProvider.GOOGLE.name(), "false",
				"true", "false", "123-456-7890", "001", "US" };

		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		IHeadersDataRow rowMock = mock(IHeadersDataRow.class);

		when(wrapperMock.getHeaders()).thenReturn(headers);
		when(wrapperMock.hasNext()).thenReturn(true);
		when(wrapperMock.next()).thenReturn(rowMock);
		when(rowMock.getValues()).thenReturn(values);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			Map<String, Map<String, Object>> result = SecurityQueryUtils
					.getUserInfo(Collections.singletonList("user-1"));

			assertEquals(1, result.size());
			Map<String, Object> userInfo = result.get("user-1");
			assertEquals("user-1", userInfo.get("SMSS_USER__ID"));
			assertEquals("Test User", userInfo.get("SMSS_USER__NAME"));
			assertEquals("tuser", userInfo.get("SMSS_USER__USERNAME"));
			assertEquals("user@example.com", userInfo.get("SMSS_USER__EMAIL"));
			assertEquals(AuthProvider.GOOGLE.name(), userInfo.get("SMSS_USER__TYPE"));
			assertEquals("false", userInfo.get("SMSS_USER__ADMIN"));
			assertEquals("true", userInfo.get("SMSS_USER__PUBLISHER"));
			assertEquals("false", userInfo.get("SMSS_USER__EXPORTER"));
			assertEquals("123-456-7890", userInfo.get("SMSS_USER__PHONE"));
			assertEquals("001", userInfo.get("SMSS_USER__PHONEEXTENSION"));
			assertEquals("US", userInfo.get("SMSS_USER__COUNTRYCODE"));
		}
	}

	@Test
	void testGetApplicationUserCountReturnsValue() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		IHeadersDataRow rowMock = mock(IHeadersDataRow.class);

		when(wrapperMock.hasNext()).thenReturn(true);
		when(wrapperMock.next()).thenReturn(rowMock);
		when(rowMock.getValues()).thenReturn(new Object[] { 7 });

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertEquals(7, SecurityQueryUtils.getApplicationUserCount());
		}
	}

	@Test
	void testUserIsPublisherReturnsTrueWhenEntryExists() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(true);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertTrue(SecurityQueryUtils.userIsPublisher(null));
		}
	}

	@Test
	void testUserIsExporterReturnsTrueWhenEntryExists() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(true);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertTrue(SecurityQueryUtils.userIsExporter(null));
		}
	}

	@Test
	void testSearchForUserReturnsResultsFromQueryExecutionUtility() {
		List<Map<String, Object>> expected = new ArrayList<>();
		Map<String, Object> row = new HashMap<>();
		row.put("id", "user-1");
		expected.add(row);

		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushRsToMap(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			List<Map<String, Object>> results = SecurityQueryUtils.searchForUser("user");

			assertSame(expected, results);
		}
	}

	@Test
	void testCheckUserExistByIdReturnsTrueWhenRowFound() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(true);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertTrue(SecurityQueryUtils.checkUserExist("user-1"));
		}
	}

	@Test
	void testCheckUserExistByCredentialsReturnsTrueWhenRowFound() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(true);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertTrue(SecurityQueryUtils.checkUserExist("username", "user@example.com"));
		}
	}

	@Test
	void testCheckUserEmailExistReturnsTrueWhenRowFound() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(true);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertTrue(SecurityQueryUtils.checkUserEmailExist("user@example.com"));
		}
	}

	@Test
	void testCheckUsernameExistReturnsTrueWhenRowFound() throws Exception {
		IRawSelectWrapper wrapperMock = mock(IRawSelectWrapper.class);
		when(wrapperMock.hasNext()).thenReturn(true);

		WrapperManager managerMock = mock(WrapperManager.class);
		when(managerMock.getRawWrapper(eq(securityDbMock), any(SelectQueryStruct.class))).thenReturn(wrapperMock);

		try (MockedStatic<WrapperManager> wm = Mockito.mockStatic(WrapperManager.class)) {
			wm.when(WrapperManager::getInstance).thenReturn(managerMock);

			assertTrue(SecurityQueryUtils.checkUsernameExist("username"));
		}
	}

	@Test
	void testIsUserTypeReturnsTrueWhenResultFound() {
		List<String[]> entries = new ArrayList<>();
		entries.add(new String[] { "Test User" });

		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushRsToListOfStrArray(eq(securityDbMock),
					any(SelectQueryStruct.class))).thenReturn(entries);

			assertTrue(SecurityQueryUtils.isUserType("user-1", AuthProvider.GOOGLE));
		}
	}

	@Test
	void testGetUserLockAndLastLoginAndLastPassResetReturnsFirstRow() {
		Object[] expected = new Object[] { Boolean.TRUE, new SemossDate(LocalDate.of(2024, 1, 1)),
				new SemossDate(LocalDate.of(2024, 2, 2)) };
		List<Object[]> rows = new ArrayList<>();
		rows.add(expected);

		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushRsToListOfObjArray(eq(securityDbMock),
					any(SelectQueryStruct.class))).thenReturn(rows);

			Object[] result = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset("user-1",
					AuthProvider.GOOGLE);

			assertSame(expected, result);
		}
	}

	@Test
	void testGetEnginePermissionReturnsFirstResult() {
		Map<String, Object> permission = new HashMap<>();
		permission.put("ENGINEPERMISSION__PERMISSION", "READ");
		List<Map<String, Object>> rows = new ArrayList<>();
		rows.add(permission);

		try (MockedStatic<QueryExecutionUtility> qeu = Mockito.mockStatic(QueryExecutionUtility.class)) {
			qeu.when(() -> QueryExecutionUtility.flushRsToMap(eq(securityDbMock), any(SelectQueryStruct.class)))
					.thenReturn(rows);

			Map<String, Object> result = SecurityQueryUtils.getEnginePermission("user-1", "engine-1");

			assertSame(permission, result);
		}
	}
}
