/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.project.api.IProject;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AbstractSecurityUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
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

	@Test
	void testDefaultVariables() {
		assertFalse(AbstractSecurityUtils.anonymousUsersEnabled());
		assertFalse(AbstractSecurityUtils.anonymousUserUploadData());
		assertFalse(AbstractSecurityUtils.adminSetExporter());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyProjectSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyWorkspaceAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyWorkspaceDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyWorkspaceAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyWorkspaceSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyWorkspaceSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlySkillAdd());
		assertFalse(AbstractSecurityUtils.adminOnlySkillDelete());
		assertFalse(AbstractSecurityUtils.adminOnlySkillAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlySkillSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlySkillSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyDatabaseSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyModelAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyModelDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyModelAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyModelSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyModelSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyStorageSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyVectorSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyFunctionSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailAdd());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailDelete());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyGuardrailSetDiscoverable());
		assertFalse(AbstractSecurityUtils.adminOnlyInsightSetPublic());
		assertFalse(AbstractSecurityUtils.adminOnlyInsightAddAccess());
		assertFalse(AbstractSecurityUtils.adminOnlyInsightShare());
	}

	@Test
	void testAdminOnlyProjectFlagsRouteByProjectType() {
		AbstractSecurityUtils.adminOnlyProjectAdd = true;
		AbstractSecurityUtils.adminOnlyProjectDelete = false;
		AbstractSecurityUtils.adminOnlyProjectAddAccess = true;
		AbstractSecurityUtils.adminOnlyProjectSetPublic = false;
		AbstractSecurityUtils.adminOnlyProjectSetDiscoverable = true;
		AbstractSecurityUtils.adminOnlyWorkspaceAdd = false;
		AbstractSecurityUtils.adminOnlyWorkspaceDelete = true;
		AbstractSecurityUtils.adminOnlyWorkspaceAddAccess = false;
		AbstractSecurityUtils.adminOnlyWorkspaceSetPublic = true;
		AbstractSecurityUtils.adminOnlyWorkspaceSetDiscoverable = false;
		AbstractSecurityUtils.adminOnlySkillAdd = true;
		AbstractSecurityUtils.adminOnlySkillDelete = true;
		AbstractSecurityUtils.adminOnlySkillAddAccess = false;
		AbstractSecurityUtils.adminOnlySkillSetPublic = false;
		AbstractSecurityUtils.adminOnlySkillSetDiscoverable = true;

		try {
			assertAdminOnlyProjectFlags(IProject.PROJECT_TYPE.BLOCKS, true, false, true, false, true);
			assertAdminOnlyProjectFlags(IProject.PROJECT_TYPE.CODE, true, false, true, false, true);
			assertAdminOnlyProjectFlags(IProject.PROJECT_TYPE.INSIGHTS, true, false, true, false, true);
			assertAdminOnlyProjectFlags(IProject.PROJECT_TYPE.WORKSPACE, false, true, false, true, false);
			assertAdminOnlyProjectFlags(IProject.PROJECT_TYPE.SKILL, true, true, false, false, true);
		} finally {
			AbstractSecurityUtils.adminOnlyProjectAdd = false;
			AbstractSecurityUtils.adminOnlyProjectDelete = false;
			AbstractSecurityUtils.adminOnlyProjectAddAccess = false;
			AbstractSecurityUtils.adminOnlyProjectSetPublic = false;
			AbstractSecurityUtils.adminOnlyProjectSetDiscoverable = false;
			AbstractSecurityUtils.adminOnlyWorkspaceAdd = false;
			AbstractSecurityUtils.adminOnlyWorkspaceDelete = false;
			AbstractSecurityUtils.adminOnlyWorkspaceAddAccess = false;
			AbstractSecurityUtils.adminOnlyWorkspaceSetPublic = false;
			AbstractSecurityUtils.adminOnlyWorkspaceSetDiscoverable = false;
			AbstractSecurityUtils.adminOnlySkillAdd = false;
			AbstractSecurityUtils.adminOnlySkillDelete = false;
			AbstractSecurityUtils.adminOnlySkillAddAccess = false;
			AbstractSecurityUtils.adminOnlySkillSetPublic = false;
			AbstractSecurityUtils.adminOnlySkillSetDiscoverable = false;
		}
	}

	private static void assertAdminOnlyProjectFlags(IProject.PROJECT_TYPE type, boolean add, boolean delete,
			boolean addAccess, boolean setPublic, boolean setDiscoverable) {
		assertEquals(add, AbstractSecurityUtils.adminOnlyProjectAdd(type));
		assertEquals(delete, AbstractSecurityUtils.adminOnlyProjectDelete(type));
		assertEquals(addAccess, AbstractSecurityUtils.adminOnlyProjectAddAccess(type));
		assertEquals(setPublic, AbstractSecurityUtils.adminOnlyProjectSetPublic(type));
		assertEquals(setDiscoverable, AbstractSecurityUtils.adminOnlyProjectSetDiscoverable(type));
	}

	@Test
	void testAdminOnlyEngineAdd_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineAdd("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineAdd(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineAdd(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineAdd(type));
		}
	}

	@Test
	void testAdminOnlyEngineDelete_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineDelete("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineDelete(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineDelete(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineDelete(type));
		}
	}

	@Test
	void testAdminOnlyEngineAddAccess_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineAddAccess("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineAddAccess(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineAddAccess(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineAddAccess(type));
		}
	}

	@Test
	void testAdminOnlyEngineSetPublic_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineSetPublic("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineSetPublic(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineSetPublic(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineSetPublic(type));
		}
	}

	@Test
	void testAdminOnlyEngineDiscoverable_EngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);

		assertFalse(AbstractSecurityUtils.adminOnlyEngineSetDiscoverable("testEngine"));
	}

	@ParameterizedTest
	@EnumSource(IEngine.CATALOG_TYPE.class)
	void testAdminOnlyEngineDiscoverable(IEngine.CATALOG_TYPE type) {
		if (type == IEngine.CATALOG_TYPE.VENV || type == IEngine.CATALOG_TYPE.PROJECT) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> AbstractSecurityUtils.adminOnlyEngineSetDiscoverable(type));
			assertEquals("Admin only configuration must be defined for catalog type = " + type, e.getMessage());
		} else {
			assertFalse(AbstractSecurityUtils.adminOnlyEngineSetDiscoverable(type));
		}
	}

	@Test
	void testInitialize_alreadyInitialized() throws Exception {
		// just testing no errors thrown
		AbstractSecurityUtils.initialize();
	}

	@Test
	void testInitialize_copiesOldModelUsageLimitsAndDropsTheirColumns() throws Exception {
		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
		String[][] oldEngineColumns = { { "USAGERESTRICTION", "VARCHAR(255)" }, { "USAGEFREQUENCY", "VARCHAR(255)" },
				{ "MAXTOKENS", "INTEGER" }, { "MAXRESPONSETIME", "DOUBLE" } };
		for (String[] column : oldEngineColumns) {
			securityDb.insertData(queryUtil.alterTableAddColumn("ENGINEPERMISSION", column[0], column[1]));
		}
		String[][] oldUserColumns = { { "MODELUSAGERESTRICTION", "VARCHAR(255)" },
				{ "MODELUSAGEFREQUENCY", "VARCHAR(255)" }, { "MODELMAXTOKENS", "INTEGER" },
				{ "MODELMAXRESPONSETIME", "DOUBLE" } };
		for (String[] column : oldUserColumns) {
			securityDb.insertData(queryUtil.alterTableAddColumn("SMSS_USER", column[0], column[1]));
		}
		String insertGrant = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, USAGERESTRICTION, "
				+ "USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME) VALUES (?,?,?,?,?,?,?)";
		execute(insertGrant, "tokens", 3, "model", "token", "ALL_TIME", 5000, null);
		execute(insertGrant, "compute", 3, "model", "compute", "WEEK", null, 1500.0);
		execute(insertGrant, "blank", 3, "model", "token", "", 200, null);
		execute(insertGrant, "zero", 3, "model", "token", "DAY", 0, null);
		execute(insertGrant, "unknown", 3, "model", "bogus", "DAY", 10, null);
		execute(insertGrant, "none", 3, "model", null, null, null, null);
		execute("INSERT INTO SMSS_USER (ID, TYPE, NAME, MODELUSAGERESTRICTION, MODELUSAGEFREQUENCY, MODELMAXTOKENS) "
				+ "VALUES (?,?,?,?,?,?)", "jdoe", "NATIVE", "jdoe", "token", "MONTH", 100);
		execute("INSERT INTO GROUPENGINEPERMISSION (ID, TYPE, ENGINEID, PERMISSION) VALUES (?,?,?,?)", "data", "CUSTOM",
				"model", 3);
		execute("INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID, USERID, TYPE) VALUES (?,?,?)", "data", "jdoe", "NATIVE");

		AbstractSecurityUtils.initialize();

		// all time is a year; a direct limit whose user id has no login in SMSS_USER
		// keeps no login type
		assertOneRule(ModelUsageLimitKey.direct("model", "tokens", null), Metric.TOKENS, 5000L, Window.YEAR);
		assertOneRule(ModelUsageLimitKey.direct("model", "compute", null), Metric.COMPUTE_TIME, 1500L, Window.WEEK);
		// a blank frequency is a day
		assertOneRule(ModelUsageLimitKey.direct("model", "blank", null), Metric.TOKENS, 200L, Window.DAY);
		// 0, an unknown restriction and no restriction are no limit defined
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("model", "zero", null)));
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("model", "unknown", null)));
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("model", "none", null)));
		// the all-models default keeps the login type
		assertOneRule(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), Metric.TOKENS, 100L, Window.MONTH);

		for (String[] column : oldEngineColumns) {
			assertFalse(hasColumn("ENGINEPERMISSION", column[0]), column[0]);
		}
		for (String[] column : oldUserColumns) {
			assertFalse(hasColumn("SMSS_USER", column[0]), column[0]);
		}
		assertEquals(0L, count("SELECT COUNT(*) FROM GROUPENGINEPERMISSION WHERE DATEADDED IS NULL"));
		assertEquals(0L, count("SELECT COUNT(*) FROM CUSTOMGROUPASSIGNMENT WHERE DATEADDED IS NULL"));

		// it runs only while the old columns exist, so a second start changes nothing
		AbstractSecurityUtils.initialize();
		assertOneRule(ModelUsageLimitKey.direct("model", "tokens", null), Metric.TOKENS, 5000L, Window.YEAR);
		assertOneRule(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), Metric.TOKENS, 100L, Window.MONTH);
	}

	@Test
	void testInitialize_oldModelUsageLimitsKeepALimitAlreadyDefined() throws Exception {
		ModelUsageLimitKey key = ModelUsageLimitKey.direct("model", "jdoe", null);
		ModelUsageLimit current = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1000L, Window.MINUTE, Counts.EACH_PERSON),
						new ModelUsageLimitRule(Metric.TOKENS, 50000L, Window.DAY, Counts.EACH_PERSON)));
		QueryExecutionUtility.write(securityDb, connection -> {
			SecurityModelUsageLimitUtils.setLimit(connection, key, current, null);
			return null;
		});
		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
		securityDb.insertData(queryUtil.alterTableAddColumn("ENGINEPERMISSION", "USAGERESTRICTION", "VARCHAR(255)"));
		securityDb.insertData(queryUtil.alterTableAddColumn("ENGINEPERMISSION", "MAXTOKENS", "INTEGER"));
		execute("INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, USAGERESTRICTION, MAXTOKENS) "
				+ "VALUES (?,?,?,?,?)", "jdoe", 3, "model", "token", 10);

		AbstractSecurityUtils.initialize();

		assertEquals(2, SecurityModelUsageLimitUtils.getLimit(key).getRules().size());
		assertFalse(hasColumn("ENGINEPERMISSION", "USAGERESTRICTION"));
		assertFalse(hasColumn("ENGINEPERMISSION", "MAXTOKENS"));
	}

	@Test
	void testInitialize_indexesModelUsageLimitsByEngineLimitTypeAndSubjectId() throws Exception {
		List<String> columns = QueryExecutionUtility.queryList(securityDb,
				"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_NAME = 'MODELUSAGELIMIT_SUBJECT_INDEX' "
						+ "ORDER BY ORDINAL_POSITION",
				QueryExecutionUtility.NO_PARAMETERS, rs -> rs.getString(1));

		// SUBJECTTYPE is left out, so the key fits the index size MySQL allows with
		// utf8mb4
		assertEquals(List.of("ENGINEID", "LIMITTYPE", "SUBJECTID"), columns);
	}

	@Test
	void testInitialize_backfillsEngineTypesAndDisplayNames() throws Exception {
		execute("INSERT INTO ENGINE (ENGINEID, ENGINENAME) VALUES (?,?)", "engine1", "name1");

		AbstractSecurityUtils.initialize();

		assertEquals(IEngine.CATALOG_TYPE.DATABASE.toString(),
				queryValue("SELECT ENGINETYPE FROM ENGINE WHERE ENGINEID=?", "engine1"));
		assertEquals("name1", queryValue("SELECT ENGINEDISPLAYNAME FROM ENGINE WHERE ENGINEID=?", "engine1"));
	}

	@Test
	void testInitialize_engineBackfillFailureRollsBackTheEngineTypes() throws Exception {
		securityDb.insertData(
				"ALTER TABLE ENGINE ADD CONSTRAINT TEST_REFUSE_DISPLAY_NAME CHECK (ENGINEDISPLAYNAME <> 'refused')");
		try {
			execute("INSERT INTO ENGINE (ENGINEID, ENGINENAME) VALUES (?,?)", "engine1", "refused");

			// the display name backfill fails after the type backfill, and start up stops
			assertThrows(Exception.class, AbstractSecurityUtils::initialize);

			assertNull(queryValue("SELECT ENGINETYPE FROM ENGINE WHERE ENGINEID=?", "engine1"));
			assertNull(queryValue("SELECT ENGINEDISPLAYNAME FROM ENGINE WHERE ENGINEID=?", "engine1"));
		} finally {
			securityDb.insertData("ALTER TABLE ENGINE DROP CONSTRAINT IF EXISTS TEST_REFUSE_DISPLAY_NAME");
		}
	}

	@Test
	void testInitialize_backfillsProjectDisplayNamesAndTemplateFlags() throws Exception {
		execute("INSERT INTO PROJECT (PROJECTID, PROJECTNAME) VALUES (?,?)", "project1", "name1");

		AbstractSecurityUtils.initialize();

		assertEquals("name1", queryValue("SELECT PROJECTDISPLAYNAME FROM PROJECT WHERE PROJECTID=?", "project1"));
		assertEquals(Boolean.FALSE, queryValue("SELECT IS_TEMPLATE FROM PROJECT WHERE PROJECTID=?", "project1"));
	}

	@Test
	void testInitialize_projectBackfillFailureRollsBackTheDisplayNames() throws Exception {
		securityDb.insertData("ALTER TABLE PROJECT ADD CONSTRAINT TEST_REFUSE_TEMPLATE CHECK "
				+ "(IS_TEMPLATE IS NULL OR PROJECTNAME <> 'refused')");
		try {
			execute("INSERT INTO PROJECT (PROJECTID, PROJECTNAME) VALUES (?,?)", "project1", "refused");

			// the template flag backfill fails after the display name backfill, and start
			// up stops
			assertThrows(Exception.class, AbstractSecurityUtils::initialize);

			assertNull(queryValue("SELECT PROJECTDISPLAYNAME FROM PROJECT WHERE PROJECTID=?", "project1"));
			assertNull(queryValue("SELECT IS_TEMPLATE FROM PROJECT WHERE PROJECTID=?", "project1"));
		} finally {
			securityDb.insertData("ALTER TABLE PROJECT DROP CONSTRAINT IF EXISTS TEST_REFUSE_TEMPLATE");
		}
	}

	@Test
	void testInitialize_fillsTheEngineTypesOfProjectDependencies() throws Exception {
		execute("INSERT INTO ENGINE (ENGINEID, ENGINENAME, ENGINETYPE) VALUES (?,?,?)", "engine1", "name1", "MODEL");
		execute("INSERT INTO PROJECT (PROJECTID, PROJECTNAME) VALUES (?,?)", "project1", "name1");
		execute("INSERT INTO PROJECTDEPENDENCIES (PROJECTID, ENGINEID) VALUES (?,?)", "app", "engine1");
		execute("INSERT INTO PROJECTDEPENDENCIES (PROJECTID, ENGINEID) VALUES (?,?)", "app", "project1");

		AbstractSecurityUtils.initialize();

		assertEquals("MODEL", queryValue("SELECT ENGINETYPE FROM PROJECTDEPENDENCIES WHERE ENGINEID=?", "engine1"));
		assertEquals("PROJECT", queryValue("SELECT ENGINETYPE FROM PROJECTDEPENDENCIES WHERE ENGINEID=?", "project1"));
	}

	@Test
	void testInitialize_metaKeyDefaultsAreReplacedInOneTransaction() throws Exception {
		execute("DELETE FROM " + Constants.PROJECT_METAKEYS);
		execute("INSERT INTO " + Constants.PROJECT_METAKEYS + " (METAKEY, SINGLEMULTI, DISPLAYORDER, DISPLAYOPTIONS) "
				+ "VALUES (?,?,?,?)", "custom", "single", 0, "textarea");
		UnitTestFailingTrigger.install(securityDb, "TEST_REFUSE_PROJECTMETAKEYS", "INSERT", Constants.PROJECT_METAKEYS);
		try {
			// a table with fewer than the defaults is emptied and gets them; the inserts
			// fail, so the row deleted before them is back, and start up goes on
			AbstractSecurityUtils.initialize();

			assertEquals(List.of("custom"), strings("SELECT METAKEY FROM " + Constants.PROJECT_METAKEYS));
		} finally {
			UnitTestFailingTrigger.drop(securityDb, "TEST_REFUSE_PROJECTMETAKEYS");
		}

		AbstractSecurityUtils.initialize();

		assertEquals(6L, count("SELECT COUNT(*) FROM " + Constants.PROJECT_METAKEYS));
		assertEquals(0L, count("SELECT COUNT(*) FROM " + Constants.PROJECT_METAKEYS + " WHERE METAKEY = 'custom'"));
	}

	@Test
	void testInitialize_permissionLevelsAreReplacedInOneTransaction() throws Exception {
		execute("INSERT INTO PERMISSION (ID, NAME) VALUES (?,?)", 4, "EXTRA");
		try {
			UnitTestFailingTrigger.install(securityDb, "TEST_REFUSE_PERMISSION", "INSERT", "PERMISSION");
			try {
				// a table with more than the three levels is emptied and gets them; the
				// inserts fail, so the rows deleted before them are back
				AbstractSecurityUtils.initialize();

				assertEquals(List.of("OWNER", "EDIT", "READ_ONLY", "EXTRA"),
						strings("SELECT NAME FROM PERMISSION ORDER BY ID"));
			} finally {
				UnitTestFailingTrigger.drop(securityDb, "TEST_REFUSE_PERMISSION");
			}

			AbstractSecurityUtils.initialize();

			assertEquals(List.of("OWNER", "EDIT", "READ_ONLY"), strings("SELECT NAME FROM PERMISSION ORDER BY ID"));
		} finally {
			// the levels are kept between tests
			execute("DELETE FROM PERMISSION WHERE ID=?", 4);
		}
	}

	@Test
	void testUpdateUserTypeEnum_aColumnThatFailsKeepsItsValuesAndTheOthersChange() throws Exception {
		execute("INSERT INTO SMSS_USER (ID, NAME, TYPE) VALUES (?,?,?)", "user1", "name1", "Native");
		execute("INSERT INTO PASSWORD_HISTORY (ID, USERID, TYPE) VALUES (?,?,?)", "history1", "user1", "Native");
		UnitTestFailingTrigger.install(securityDb, "TEST_REFUSE_PASSWORD_HISTORY", "UPDATE", "PASSWORD_HISTORY");
		try {
			updateUserTypeEnum();

			assertEquals("NATIVE", queryValue("SELECT TYPE FROM SMSS_USER WHERE ID=?", "user1"));
			assertEquals("Native", queryValue("SELECT TYPE FROM PASSWORD_HISTORY WHERE ID=?", "history1"));
		} finally {
			UnitTestFailingTrigger.drop(securityDb, "TEST_REFUSE_PASSWORD_HISTORY");
		}

		updateUserTypeEnum();

		assertEquals("NATIVE", queryValue("SELECT TYPE FROM PASSWORD_HISTORY WHERE ID=?", "history1"));
	}

	/**
	 * Runs the private start up step that changes stored login types to their
	 * provider's label.
	 */
	private static void updateUserTypeEnum() throws Exception {
		Method method = AbstractSecurityUtils.class.getDeclaredMethod("updateUserTypeEnum");
		method.setAccessible(true);
		method.invoke(null);
	}

	/**
	 * Reads the first column of the first row of a query on the security database.
	 *
	 * @return the value, or null when there is no row
	 */
	private Object queryValue(String sql, Object... values) throws Exception {
		return QueryExecutionUtility.queryOne(securityDb, sql, ps -> {
			for (int i = 0; i < values.length; i++) {
				ps.setObject(i + 1, values[i]);
			}
		}, rs -> rs.getObject(1));
	}

	/**
	 * Reads the first column of every row of a query on the security database.
	 *
	 * @return the values, in row order
	 */
	private List<String> strings(String sql) throws Exception {
		return QueryExecutionUtility.queryList(securityDb, sql, QueryExecutionUtility.NO_PARAMETERS,
				rs -> rs.getString(1));
	}

	private void execute(String sql, Object... values) throws Exception {
		QueryExecutionUtility.executeUpdate(securityDb, sql, ps -> {
			for (int i = 0; i < values.length; i++) {
				ps.setObject(i + 1, values[i]);
			}
		});
	}

	private boolean hasColumn(String table, String column) throws Exception {
		return count("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table
				+ "' AND COLUMN_NAME = '" + column + "'") > 0;
	}

	private long count(String sql) throws Exception {
		return ((Number) UnitTestGroupTables.queryValue(securityDb, sql)).longValue();
	}

	private static void assertOneRule(ModelUsageLimitKey key, Metric metric, long amount, Window window) {
		ModelUsageLimit limit = SecurityModelUsageLimitUtils.getLimit(key);
		assertNotNull(limit, key.toString());
		assertFalse(limit.isUnlimited());
		assertEquals(1, limit.getRules().size());
		ModelUsageLimitRule rule = limit.getRules().get(0);
		assertEquals(metric, rule.getMetric());
		assertEquals(Long.valueOf(amount), rule.getAmount());
		assertEquals(window, rule.getWindow());
		assertEquals(Counts.EACH_PERSON, rule.getCounts());
	}

	@Test
	void testContainsEngineName_doesContain() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);
		assertTrue(AbstractSecurityUtils.containsEngineName("testName"));
	}

	@Test
	void testContainsEngineName_notThere() {
		assertFalse(AbstractSecurityUtils.containsEngineName("testName"));
	}

	@Test
	void testContainsProjectName_present() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", user);
		assertTrue(AbstractSecurityUtils.containsProjectName("testName"));
	}

	@Test
	void testContainsProjectName_notPresent() {
		assertFalse(AbstractSecurityUtils.containsProjectName("testName"));
	}

	@Test
	void testContainsEngineId_Security() {
		assertTrue(AbstractSecurityUtils.containsEngineId("security"));
	}

	@Test
	void testContainsEngineId_present() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("testEngine", "testName", user);
		assertTrue(AbstractSecurityUtils.containsEngineId("testEngine"));
	}

	@Test
	void testContainsEngineId_missing() {
		assertFalse(AbstractSecurityUtils.containsEngineId("testEngine"));
	}

	@Test
	void testContainsProjectId_Security() {
		assertTrue(AbstractSecurityUtils.containsProjectId("security"));
	}

	@Test
	void testContainsProjectId_present() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", user);
		assertTrue(AbstractSecurityUtils.containsProjectId("testProject"));
	}

	@Test
	void testContainsProjectId_missing() {
		assertFalse(AbstractSecurityUtils.containsProjectId("testProject"));
	}

	@Test
	void testIgnoreDatabase_security() {
		assertTrue(AbstractSecurityUtils.ignoreDatabase("security"));
	}

	@Test
	void testIgnoreDatabase_isAssetProject() throws SQLException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", user);

		UserAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, "testProject");

		assertTrue(AbstractSecurityUtils.ignoreDatabase("testProject"));
	}

	@Test
	void testIgnoreDatabase_enginePropertiesDatabaseApp() throws IOException {
		Properties properties = new Properties();
		properties.setProperty(Constants.IS_ASSET_APP, "true");
		UnitTestSecurityAuthUtils.createSmssFileFromProps(properties, databaseFolder, "test.smss");
		String smssFile = databaseFolder + File.separator + "test.smss";
		DIHelper.getInstance().setEngineProperty("test_" + Constants.STORE, smssFile);

		assertTrue(AbstractSecurityUtils.ignoreDatabase("test"));

		// teardown this test
		DIHelper.getInstance().removeEngineProperty("test_" + Constants.STORE);
	}

	@Test
	void testIgnoreDatabase_false() {
		assertFalse(AbstractSecurityUtils.ignoreDatabase("noDatabase"));
	}

	@Test
	void testGetStockImage_layoutNull() {
		UnitTestSecurityAuthUtils.setupImageDir(semossDir);

		File f = AbstractSecurityUtils.getStockImage("blah", "blah");

		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("color-logo.png", f.getName());
	}

	@Test
	void testGetStockImage_layoutTreemap() {
		UnitTestSecurityAuthUtils.setupImageDir(semossDir, "treemap.png");

		User adminUser = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", adminUser);
		UnitTestSecurityAuthUtils.createInsight("testProject", "insightId", "insightName", "treemap");

		File f = AbstractSecurityUtils.getStockImage("testProject", "insightId");
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("treemap.png", f.getName());
	}

	@Test
	void testGetStockImage_unknownLayout() {
		UnitTestSecurityAuthUtils.setupImageDir(semossDir, "treemap.png");

		User adminUser = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProject", "testName", adminUser);
		UnitTestSecurityAuthUtils.createInsight("testProject", "insightId", "insightName", "foobar");

		File f = AbstractSecurityUtils.getStockImage("testProject", "insightId");
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("color-logo.png", f.getName());
	}

	@Test
	void testCreateFilter_singleFilter() {
		assertEquals(" IN ('testOne')", AbstractSecurityUtils.createFilter("testOne"));
	}

	@Test
	void testCreateFilter_multipleFilters() {
		assertEquals(" IN ('testOne', 'testTwo')", AbstractSecurityUtils.createFilter("testOne", "testTwo"));
	}

	@Test
	void testCreateFilter_Empty() {
		List<String> values = new ArrayList<>();
		String s = AbstractSecurityUtils.createFilter(values);
		assertEquals(" IN () ", s);
	}

	@Test
	void testCreateFilter_EscapeSQLStatement() {
		List<String> values = new ArrayList<>();
		values.add("'DELETE * FROM SMSS_USER'");
		values.add("test");
		String s = AbstractSecurityUtils.createFilter(values);
		assertEquals(" IN ('''DELETE * FROM SMSS_USER''', 'test')", s);
	}

	@Test
	void testGetUserFilters_userNull() {
		assertEquals("()", AbstractSecurityUtils.getUserFilters(null));
	}

	@Test
	void testGetUserFilters_singleLogin() {
		User user = new User();
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		user.setAccessToken(at);
		assertEquals("('oneid')", AbstractSecurityUtils.getUserFilters(user));
	}

	@Test
	void testGetUserFilters_multipleLogin() {
		User user = new User();
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		user.setAccessToken(at);

		AccessToken at2 = new AccessToken();
		at2.setProvider(AuthProvider.DROPBOX);
		at2.setId("dropbox");
		user.setAccessToken(at2);
		assertEquals("('oneid', 'dropbox')", AbstractSecurityUtils.getUserFilters(user));
	}

	@Test
	void testGetUserFiltersQs_multiple() {
		User user = new User();
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		user.setAccessToken(at);

		AccessToken at2 = new AccessToken();
		at2.setProvider(AuthProvider.DROPBOX);
		at2.setId("dropbox");
		user.setAccessToken(at2);
		Collection<String> values = AbstractSecurityUtils.getUserFiltersQs(user);
		assertEquals(2, values.size());
		assertTrue(values.contains("oneid"));
		assertTrue(values.contains("dropbox"));
	}

	@Test
	void testGetSimpleQuery_smssUsers() {
		UnitTestSecurityAuthUtils.createUser("admin", true);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));

		List<Map<String, Object>> map = AbstractSecurityUtils.getSimpleQuery(qs);

		assertEquals(1, map.size());
		Map<String, Object> user = map.getFirst();
		assertEquals("NATIVE", user.get("type"));
	}

	@Test
	void testValidEmail_emailNull() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validEmail(null, true));
		assertEquals("null is not a valid email address. ", e.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "blah", "hey@blah" })
	void testValidEmail_emailMalformed(String test) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validEmail(test, true));
		assertEquals(test + " is not a valid email address. ", e.getMessage());
	}

	@Test
	void testValidEmail_userExists() {
		User user = UnitTestSecurityAuthUtils.createUser("one", true);
		String email = user.getPrimaryLoginToken().getEmail();
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validEmail(email, true));
		assertEquals("This email already exists. Please login. ", e.getMessage());
	}

	@Test
	void testValidEmail_validEmail() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("one", true);

		AbstractSecurityUtils.validEmail("email@test.com", true);
	}

	@ParameterizedTest
	@NullAndEmptySource
	void testValidPassword_nullAndEmpty(String password) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validPassword("oneId", AuthProvider.NATIVE, password));
		assertEquals("Password cannot be empty. ", e.getMessage());
	}

	@Test
	void testValidPassord_reusingOldPassword() {
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		SecurityNativeUserUtils.addNativeUser(at, "Test123!");

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validPassword("oneid", AuthProvider.NATIVE, "Test123!"));

		assertEquals("Cannot reuse old password. ", e.getMessage());
	}

	@Test
	void testValidPassord_valid() throws Exception {
		AccessToken at = UnitTestSecurityAuthUtils.createAccessToken("one");
		SecurityNativeUserUtils.addNativeUser(at, "Test123!");
		AbstractSecurityUtils.validPassword("oneid", AuthProvider.NATIVE, "Foobar123!");
	}

	@Test
	void testFormatPhone_null() throws Exception {
		String phone = AbstractSecurityUtils.formatPhone(null);
		assertNull(phone);
	}

	@Test
	void testFormatPhone_empty() throws Exception {
		String phone = AbstractSecurityUtils.formatPhone("");
		assertEquals("", phone);
	}

	@ParameterizedTest
	@ValueSource(strings = { "hello", "333hj3h3333", "1092!93323" })
	void testFormatPhone_invalidCharacters(String source) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.formatPhone(source));
		assertEquals("Phone number " + source + " contains invalid characters. ", e.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "7777777", "1112223333444" })
	void testFormatPhone_wrongLengths(String source) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.formatPhone(source));

		assertEquals(source + " is not a valid phone number. ", e.getMessage());
	}

	@ParameterizedTest
	@NullAndEmptySource
	void testValidUsername_nullandempty(String source) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validUsername(source));
		assertEquals("Username cannot be empty. ", e.getMessage());
	}

	@Test
	void testValidUsername_userExists() {
		User user = UnitTestSecurityAuthUtils.createUser("one", true);
		String username = user.getPrimaryLoginToken().getUsername();
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AbstractSecurityUtils.validUsername(username));
		assertEquals("Username already exists. ", e.getMessage());
	}

	@Test
	void testValidUsername_valid() {
		// no error thrown good
		AbstractSecurityUtils.validUsername("working");
	}

	@Test
	void testGenerateSalt_success() {
		String salt = AbstractSecurityUtils.generateSalt();
		assertNotNull(salt);
		assertFalse(salt.isEmpty());
	}

	@Test
	void testHash_successful() {
		String salt = AbstractSecurityUtils.generateSalt();
		String hashed = AbstractSecurityUtils.hash("password", salt);
		assertNotNull(hashed);
		assertFalse(hashed.isEmpty());
	}

	@Test
	void testCalculateEndDate() {
		ZonedDateTime now = ZonedDateTime.now();
		String nowString = now.toString();
		Timestamp ts = AbstractSecurityUtils.calculateEndDate(nowString);
		// probably figure out a better way to test the timestamp is in utc.
		assertNotNull(ts);
	}

	@Test
	void testEndDateIsExpired_null() throws Exception {
		assertFalse(AbstractSecurityUtils.endDateIsExpired(null));
	}

	@Test
	void testEndDateIsExpired_true() throws Exception {
		SemossDate semossDate = new SemossDate(LocalDateTime.now().minusDays(2), ZoneId.of("UTC"));
		assertTrue(AbstractSecurityUtils.endDateIsExpired(semossDate));
	}

	@Test
	void testEndDateIsExpired_false() throws Exception {
		SemossDate semossDate = new SemossDate(LocalDateTime.now().plusDays(2), ZoneId.of("UTC"));
		assertFalse(AbstractSecurityUtils.endDateIsExpired(semossDate));
	}

	@Test
	void preparedIdentityFiltersRetainOriginalValuesWithoutSqlSanitizers() {
		try (var sanitizerGuard = org.mockito.Mockito.mockStatic(Utility.class, invocation -> {
			String method = invocation.getMethod().getName();
			if (method.equals("inputSQLSanitizer") || method.equals("inputSanitizer")) {
				throw new AssertionError("Prepared queries must not call Utility." + method);
			}
			return invocation.callRealMethod();
		})) {
			String id = "O'Brien\\path";
			User user = new User();
			AccessToken token = new AccessToken();
			token.setId(id);
			token.setProvider(AuthProvider.MICROSOFT);
			user.setPrimaryLogin(AuthProvider.MICROSOFT);
			user.setAccessToken(token);
			assertEquals(List.of(id), AbstractSecurityUtils.getUserFilterValues(user));
			assertTrue(AbstractSecurityUtils.getUserFilterValues(null).isEmpty());
			assertTrue(AbstractSecurityUtils.getUserFilterValues(new User()).isEmpty());
			var creators = List.of(Pair.with(id, "provider'type"), Pair.with("other", "MICROSOFT"));
			var qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "id"));
			qs.addExplicitFilter(AbstractSecurityUtils.getPreparedCreatedByFilter("ENGINE__CREATEDBY",
					"ENGINE__CREATEDBYTYPE", creators));
			var compiled = new ParameterizedSqlInterpreter(securityDb).compile(qs);
			assertEquals(List.of(id, "provider'type", "other", "MICROSOFT"), compiled.parameters());
			assertFalse(compiled.sql().contains(id));
			assertThrows(IllegalArgumentException.class, () -> AbstractSecurityUtils
					.getPreparedCreatedByFilter("ENGINE__CREATEDBY", "ENGINE__CREATEDBYTYPE", List.of()));
			assertThrows(IllegalArgumentException.class, () -> AbstractSecurityUtils
					.getPreparedCreatedByFilter("ENGINE__CREATEDBY", "ENGINE__CREATEDBYTYPE", null));
		}
	}

}
