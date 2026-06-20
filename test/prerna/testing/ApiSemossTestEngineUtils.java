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
package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.notifications.NotificationDbUtils;
import prerna.prompt.PromptUtils;
import prerna.reactor.database.upload.rdbms.csv.RdbmsUploadTableDataReactor;
import prerna.reactor.database.upload.rdbms.excel.RdbmsUploadExcelDataReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.utility.TestExcelInputObject;
import prerna.testing.utility.TestExcelType;
import prerna.theme.AbstractThemeUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.SystemEngineRegistryTestExtension;
import prerna.util.Utility;

public class ApiSemossTestEngineUtils {

	private static Path ENGINES_CONFIG_FILE = Paths.get(ApiTestsSemossConstants.TEST_CONFIG_DIRECTORY.toString(),
			"engines.txt");
	private static List<String> CORE_DBS = null;

	private final static List<String> DO_NOT_CLEAR_LIST = Arrays.asList(Constants.INSIGHT_METAKEYS,
			Constants.PROJECT_METAKEYS, Constants.ENGINE_METAKEYS, Constants.PROMPT_METAKEYS);
	private static List<String> IDS_TO_AVOID = null;

	private static Logger classLogger = LogManager.getLogger(ApiSemossTestEngineUtils.class);

	// DBs to clear, tables to avoid
	private static final List<Pair<String, List<String>>> DB_TO_CLEAR = Arrays
			.asList(Pair.of(Constants.SECURITY_DB, Arrays.asList("PERMISSION")),
			// Pair.of(Constants.SCHEDULER_DB, new ArrayList<String>()), not initialized
//			Pair.of(Constants.THEMING_DB, Arrays.asList(new String[] {"BLOCKS_TABLE"})),
					Pair.of(Constants.USER_TRACKING_DB, Arrays.asList(new String[] {})),
					Pair.of(Constants.PROMPT_DB, Arrays.asList(new String[] {})));

	static void checkDatabasePropMapping() {
		assertTrue(SystemEngineRegistry.isLocalMasterDbLoaded(), "LocalMasterDb should be loaded");
		assertTrue(SystemEngineRegistry.isSecurityDbLoaded(), "SecurityDb should be loaded");
		assertTrue(SystemEngineRegistry.isSchedulerDbLoaded(), "SchedulerDb should be loaded");
		assertTrue(SystemEngineRegistry.isThemesDbLoaded(), "ThemesDb should be loaded");
		assertTrue(SystemEngineRegistry.isUserTrackingDbLoaded(), "UserTrackingDb should be loaded");
		assertTrue(SystemEngineRegistry.isPromptDbLoaded(), "PromptDB should be loaded");
		assertTrue(SystemEngineRegistry.isNotificationDbLoaded(), "NotificationDB should be loaded");
		assertTrue(SystemEngineRegistry.isAuditLogsDbLoaded(), "AuditLogDb should be loaded");
		assertTrue(SystemEngineRegistry.isModelInferenceLogsDbLoaded(), "ModelInferenceLogsDb should be loaded");
	}

	static void unloadDatabases() {
		SystemEngineRegistryTestExtension.resetAll();
	}

	public static void addDBStartupTasks(List<Callable<Void>> tasks) {
		tasks.add(() -> initializeLocalMaster());
		tasks.add(() -> initializeSecurity());
		tasks.add(() -> initializeScheduler());
		tasks.add(() -> initializeThemes());
		tasks.add(() -> initializeUserTracking());
		tasks.add(() -> initializePrompt());
		tasks.add(() -> initializeNotification());
		tasks.add(() -> initializeAuditLogs());
		tasks.add(() -> initializeModelInferenceLogs());
	}

	private static Void initializeModelInferenceLogs() throws Exception {
		doInitializeSemossDB(Constants.MODEL_INFERENCE_LOGS_DB, "database.mv.db");
		ModelInferenceLogsUtils.initModelInferenceLogsDatabase();
		return null;
	}

	private static Void initializeAuditLogs() throws Exception {
		doInitializeSemossDB(Constants.AUDIT_LOGS_DB, "database.mv.db");
		AuditLogsDbUtils.loadAuditLogsDatabase();
		return null;
	}

	private static Void initializeNotification() throws Exception {
		doInitializeSemossDB(Constants.NOTIFICATION_DB, "database.mv.db");
		NotificationDbUtils.loadNotificationDatabase();
		return null;
	}

	private static Void initializeLocalMaster() throws IOException, Exception {
		doInitializeSemossDB(Constants.LOCAL_MASTER_DB, "databaseNewMaster.mv.db");
		MasterDatabaseUtility.initLocalMaster();
		return null;
	}

	private static Void initializeSecurity() throws IOException, Exception {
		doInitializeSemossDB(Constants.SECURITY_DB, "database.mv.db");
		AbstractSecurityUtils.loadSecurityDatabase();
		return null;
	}

	private static Void initializeUserTracking() throws IOException, Exception {
		doInitializeSemossDB(Constants.USER_TRACKING_DB, "databaseNewUserTracking.mv.db");
		UserTrackingUtils.initUserTrackerDatabase();
		return null;
	}

	private static Void initializeThemes() throws Exception {
		doInitializeSemossDB(Constants.THEMING_DB, "database.mv.db");
		AbstractThemeUtils.loadThemingDatabase();
		return null;
	}

	private static Void initializeScheduler() throws Exception {
		doInitializeSemossDB(Constants.SCHEDULER_DB, "database.mv.db");
		// Full SchedulerDatabaseUtility.startServer() skipped intentionally in tests
		// (starts background scheduler threads); the engine is loaded and registered
		// via doInitializeSemossDB so callers can use
		// SystemEngineRegistry.getSchedulerDb().
		return null;
	}

	public static Void initializePrompt() throws Exception {
		try {
			doInitializeSemossDB(Constants.PROMPT_DB, "database.mv.db");
			PromptUtils.loadPromptDatabase();
		} catch (Exception e) {
			// Weird behavior, but NPE on first try, successful load on second
			// Can't debug because it works first try when debugging
			PromptUtils.loadPromptDatabase();
		}
		return null;
	}

	public static void createUser(String userUserName, String email, String type, boolean isAdmin) throws SQLException {
		Triple<String, String, String> cds = getTestDatabaseConnection(Constants.SECURITY_DB);

		try (Connection conn = DriverManager.getConnection(cds.getLeft(), cds.getMiddle(), cds.getRight())) {
			String userPassword = "TestTest8*";
			String salt = SecurityQueryUtils.generateSalt();
			String hashed = SecurityQueryUtils.hash(userPassword, salt);

			String name = userUserName.substring(0, 1);
			PreparedStatement ps = conn.prepareStatement("INSERT INTO SMSS_USER "
					+ "(NAME, EMAIL, TYPE, ID, PASSWORD, SALT, USERNAME, ADMIN, PUBLISHER, EXPORTER, DATECREATED, LASTLOGIN, LASTPASSWORDRESET, LOCKED, PHONE, PHONEEXTENSION, COUNTRYCODE)\r\n"
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, true, true, null, null, null, false, '', '', '')");
			int i = 1;
			ps.setString(i++, name);
			ps.setString(i++, email);
			ps.setString(i++, type);
			ps.setString(i++, userUserName);
			ps.setString(i++, hashed);
			ps.setString(i++, salt);
			ps.setString(i++, userUserName);
			ps.setBoolean(i++, isAdmin);
			ps.executeUpdate();

			conn.commit();
			ps.close();
		}
	}

	private static void doInitializeSemossDB(String name, String dbName) throws Exception {
		String smssPath = ApiTestsSemossConstants.TEST_DB_DIRECTORY + File.separator + name + ".smss";
		String db = ApiTestsSemossConstants.TEST_DB_DIRECTORY + File.separator + name + File.separator + dbName;

		String marker = IntegrationTestWorkspace.MARKER_PREFIX;
		assertTrue(smssPath.contains(marker),
				"Expected SMSS path to be inside the integration test workspace: " + smssPath);
		assertTrue(db.contains(marker), "Expected DB path to be inside the integration test workspace: " + db);

		if (Files.exists(Paths.get(db))) {
			Files.delete(Paths.get(db));
		}

		SystemEngineRegistryTestExtension.loadForTesting(smssPath);
	}

	public static void deleteAllDataAndAddUser() {
		for (Pair<String, List<String>> x : DB_TO_CLEAR) {
			Triple<String, String, String> connectionDetails = getTestDatabaseConnection(x.getLeft());
			connectAndClearDb(connectionDetails, x.getRight());
		}

		Triple<String, String, String> lmdConnDetails = getTestDatabaseConnection(Constants.LOCAL_MASTER_DB);
		connectAndClearLocalMaster(lmdConnDetails);

		Triple<String, String, String> themeConnDetails = getTestDatabaseConnection(Constants.THEMING_DB);
		connectAndClearThemeDb(themeConnDetails);

		try {
			createUser(ApiTestsSemossConstants.USER_NAME, ApiTestsSemossConstants.USER_EMAIL,
					AuthProvider.NATIVE.toString(), true);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Could not add Default Native Admin user");
		}
	}

	private static void connectAndClearThemeDb(Triple<String, String, String> connectionDetails) {
		PreparedStatement ps = null;
		Statement st = null;
		try (Connection conn = DriverManager.getConnection(connectionDetails.getLeft(), connectionDetails.getMiddle(),
				connectionDetails.getRight())) {
			assertTrue(connectionDetails.getLeft().contains(IntegrationTestWorkspace.MARKER_PREFIX),
					"Expected connection URL to be inside the integration test workspace: "
							+ connectionDetails.getLeft());

			ps = conn
					.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
			ps.execute();
			ResultSet rs = ps.getResultSet();
			List<String> al = new ArrayList<>();
			while (rs.next()) {
				al.add(rs.getString(1));
			}
			ps.close();

//			List<String> manualDelete = Arrays.asList(ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName());
//			al.removeAll(manualDelete);
			// delete * from databases
			st = conn.createStatement();
			for (String x : al) {
				st.addBatch("delete from " + x);
			}

//			manual delete statement
//			st.addBatch("delete from " + ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName());

			st.executeBatch();

		} catch (Exception e) {
			e.printStackTrace();
			fail("could not clear core dbs");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (st != null) {
				try {
					st.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void connectAndClearLocalMaster(Triple<String, String, String> connectionDetails) {
		PreparedStatement ps = null;
		Statement st = null;
		try (Connection conn = DriverManager.getConnection(connectionDetails.getLeft(), connectionDetails.getMiddle(),
				connectionDetails.getRight())) {
			assertTrue(connectionDetails.getLeft().contains(IntegrationTestWorkspace.MARKER_PREFIX),
					"Expected connection URL to be inside the integration test workspace: "
							+ connectionDetails.getLeft());

			ps = conn
					.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
			ps.execute();
			ResultSet rs = ps.getResultSet();
			List<String> al = new ArrayList<>();
			while (rs.next()) {
				al.add(rs.getString(1));
			}
			ps.close();

			List<String> manualDelete = Arrays.asList("ENGINE", "ENGINECONCEPT", "ENGINERELATION");
			al.removeAll(manualDelete);
			// delete * from databases
			st = conn.createStatement();
			for (String x : al) {
				st.addBatch("delete from " + x);
			}

			String idList = " NOT IN (";
			for (String x : IDS_TO_AVOID) {
				idList = idList + " '" + x + "',";
			}
			idList = idList.substring(0, idList.length() - 1) + ")";
			if (IDS_TO_AVOID.size() == 0) {
				st.addBatch("DELETE FROM ENGINE");
				st.addBatch("DELETE FROM ENGINECONCEPT");
				st.addBatch("DELETE FROM ENGINERELATION");
			} else {
				st.addBatch("DELETE FROM ENGINE WHERE ID" + idList);
				st.addBatch("DELETE FROM ENGINECONCEPT WHERE ENGINE" + idList);
				st.addBatch("DELETE FROM ENGINERELATION WHERE ENGINE" + idList);
			}

			st.executeBatch();

		} catch (Exception e) {
			e.printStackTrace();
			fail("could not clear core dbs");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (st != null) {
				try {
					st.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void connectAndClearDb(Triple<String, String, String> connectiondetails,
			List<String> ignoredtables) {
		PreparedStatement ps = null;
		Statement st = null;
		try (Connection conn = DriverManager.getConnection(connectiondetails.getLeft(), connectiondetails.getMiddle(),
				connectiondetails.getRight())) {
			assertTrue(connectiondetails.getLeft().contains(IntegrationTestWorkspace.MARKER_PREFIX),
					"Expected connection URL to be inside the integration test workspace: "
							+ connectiondetails.getLeft());

			ps = conn
					.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
			ps.execute();
			ResultSet rs = ps.getResultSet();
			List<String> al = new ArrayList<>();
			while (rs.next()) {
				al.add(rs.getString(1));
			}
			ps.close();

			al.removeAll(ignoredtables);
			// delete * from databases
			st = conn.createStatement();
			for (String x : al) {
				if (!DO_NOT_CLEAR_LIST.contains(x)) {
					st.addBatch("DELETE FROM " + x);
				}
			}
			st.executeBatch();

		} catch (Exception e) {
			e.printStackTrace();
			fail("Could not clear core dbs");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (st != null) {
				try {
					st.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static Triple<String, String, String> getTestDatabaseConnection(String db) {
		String dbPath = Paths.get(ApiTestsSemossConstants.TEST_DB_DIRECTORY, db + ".smss").toAbsolutePath().toString();
		Properties props = Utility.loadProperties(dbPath);
		classLogger.info("Test dbPath: {}", dbPath);
		classLogger.info("Props: {}", props);
		String connection = props.getProperty(Constants.CONNECTION_URL);
		classLogger.info("Connection String url: ", connection);
		connection = connection.replaceAll("@BaseFolder@",
				ApiTestsSemossConstants.TEST_BASE_DIRECTORY.replace('\\', '/'));
		connection = connection.replaceAll("@ENGINE@", db);
		assertTrue(connection.contains(IntegrationTestWorkspace.MARKER_PREFIX),
				"Expected connection URL to be inside the integration test workspace: " + connection);

		String username = props.getProperty(Constants.USERNAME);
		String password = props.getProperty(Constants.PASSWORD);
		return Triple.of(connection, username, password);
	}

	public static void clearNonCoreDBs() throws IOException {
		List<String> dbsToAvoid = getDBsToAvoid();
		File f = Paths.get(ApiTestsSemossConstants.TEST_DB_DIRECTORY).toFile();
		List<String> toDelete = new ArrayList<>();
		for (String s : f.list()) {
			boolean found = false;
			for (String c : dbsToAvoid) {
				if (s.toLowerCase().startsWith(c.toLowerCase())) {
					found = true;
					break;
				}
			}
			if (!found) {
				toDelete.add(s);
			}
		}

		for (String delete : toDelete) {
			Path p = Paths.get(ApiTestsSemossConstants.TEST_DB_DIRECTORY.toString(), delete);
			if (Files.isDirectory(p)) {
				Files.walk(p).sorted().map(Path::toFile).forEach(File::delete);
				if (Files.exists(p)) {
					try {
						Files.delete(p);
					} catch (IOException e) {
						// ignore
					}
				}
			} else {
				Files.delete(p);
			}
		}
	}

	private static List<String> getDBsToAvoid() throws IOException {
		if (CORE_DBS != null) {
			return CORE_DBS;
		}

		CORE_DBS = Files.readAllLines(ENGINES_CONFIG_FILE).stream().map(s -> s.trim()).filter(s -> !s.isEmpty())
				.collect(Collectors.toList());

		IDS_TO_AVOID = new ArrayList<>();
		for (String x : CORE_DBS) {
			if (x.contains("__")) {
				IDS_TO_AVOID.add(x.split("__")[1]);
			}
		}
		return CORE_DBS;
	}

	@SuppressWarnings("unchecked")
	public static String addTestRdbmsDatabase(String name, List<String> columns, List<String> dataTypes,
			Map<String, String> additionalDataTypes, List<List<String>> rowValues) {
		assertNotNull(name);
		assertNotNull(columns);
		assertNotNull(dataTypes);
		assertNotNull(rowValues);
		assertEquals(columns.size(), dataTypes.size(), "Column name count and dataType count have to match up");
		assertTrue(rowValues.size() > 0, "Input must contain table data");
		assertEquals(1, rowValues.stream().map(s -> s.size()).distinct().count(), "All row value lengths must match");
		assertEquals(rowValues.get(0).size(), columns.size(),
				"Data columns must have same size as column names and data types");

		Path path = Paths.get(ApiSemossTestInsightUtils.getInsightCache().toString(), name + ".csv");
		try {
			path = Files.createFile(path);
			List<String> lines = new ArrayList<>();
			lines.add(String.join(", ", columns));
			for (List<String> rv : rowValues) {
				lines.add(String.join(", ", rv));
			}
			Files.write(path, lines);
		} catch (Exception e) {
			fail(e.toString());
		}

		Map<String, String> dataType = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			dataType.put(columns.get(i), dataTypes.get(i));
		}

		Map<String, String> newHeaders = new HashMap<>();

		Map<String, String> descriptionMap = new HashMap<>();
		Map<String, String> logicalMap = new HashMap<>();
		String pixelCall = ApiSemossTestUtils.buildPixelCall(RdbmsUploadTableDataReactor.class, "database",
				Arrays.asList(name), "filePath", Arrays.asList("\\" + name + ".csv"), "delimiter", Arrays.asList(","),
				"dataTypeMap", Arrays.asList(dataType), "newHeaders", Arrays.asList(newHeaders), "additionalDataTypes",
				Arrays.asList(additionalDataTypes), "descriptionMap", Arrays.asList(descriptionMap), "logicalNamesMap",
				Arrays.asList(logicalMap), "existing", Arrays.asList(Boolean.FALSE));

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixelCall);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String engineId = (String) ret.get("database_id");
		return engineId;
	}

	@SuppressWarnings("unchecked")
	public static String addTestRdbmsDatabase(String name, List<String> tableNames, List<List<String>> columns,
			List<List<String>> dataTypes, List<Map<String, String>> additionalDataTypes,
			List<List<TestExcelInputObject>> rowValues) {

		Path path = Paths.get(ApiSemossTestInsightUtils.getInsightCache().toString(), name + ".xlsx");
		try (Workbook workbook = new XSSFWorkbook()) {

			for (int t = 0; t < tableNames.size(); t++) {
				String tableName = tableNames.get(t);
				Sheet sheet = workbook.createSheet(tableName);

				// Add column headers for the current sheet
				Row headerRow = sheet.createRow(0);
				List<String> currentColumns = columns.get(t);
				int colIdx = 0;
				for (String col : currentColumns) {
					Cell cell = headerRow.createCell(colIdx++);
					cell.setCellValue(col);
				}

				// Add row values for the current sheet
				List<TestExcelInputObject> currentRowValues = rowValues.get(t);
				int rowIdx = 1;
				for (TestExcelInputObject val : currentRowValues) {
					Row row = sheet.createRow(rowIdx++);
					colIdx = 0; // Reset column index for each row
					for (TestExcelInputObject cellValue : currentRowValues) {
						Cell cell = row.createCell(colIdx++);
						if (TestExcelType.BOOLEAN == cellValue.getType()) {
							cell.setCellValue(cellValue.getB());
						} else if (TestExcelType.INTEGER == cellValue.getType()) {
							cell.setCellValue(cellValue.getI());
						} else if (TestExcelType.DOUBLE == cellValue.getType()) {
							cell.setCellValue(cellValue.getD());
						} else if (TestExcelType.DATE == cellValue.getType()) {
							cell.setCellValue(cellValue.getLdt().toString()); // or format it as a date
						} else if (TestExcelType.STRING == cellValue.getType()) {
							cell.setCellValue(cellValue.getS());
						} else if (TestExcelType.NULL == cellValue.getType()) {
							cell.setCellValue(""); // Empty cell for null values
						}
					}
				}
			}

			// Write the output to the file
			try (FileOutputStream fileOut = new FileOutputStream(path.toFile())) {
				workbook.write(fileOut);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		// set this to empty because its super hard to figure out data structure
		// expected for range map
		// just let semoss calculate it
		Map<String, Map<String, Map<String, String>>> dataType = new HashMap<>();

		// Initialize other maps
		Map<String, Map<String, Map<String, String>>> newHeaders = new HashMap<>();
		Map<String, Map<String, Map<String, String>>> descriptionMap = new HashMap<>();
		Map<String, Map<String, Map<String, List<String>>>> logicalMap = new HashMap<>();

		// Construct pixelCall -- issues here
		String pixelCall = ApiSemossTestUtils.buildPixelCall(RdbmsUploadExcelDataReactor.class, "database",
				Arrays.asList(name), "filePath", Arrays.asList("\\" + name + ".xlsx"), "delimiter", Arrays.asList(","),
				"dataTypeMap", Arrays.asList(dataType), "newHeaders", Arrays.asList(newHeaders), "additionalDataTypes",
				Arrays.asList(additionalDataTypes), "descriptionMap", Arrays.asList(descriptionMap), "logicalNamesMap",
				Arrays.asList(logicalMap), "existing", Arrays.asList(Boolean.FALSE));

		// Process the pixel call -- string to map issue
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixelCall);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String engineId = (String) ret.get("database_id");
		return engineId;
	}

	public static String createBasicEngine() {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.BOOLEAN.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add("true");
		String engine = addTestRdbmsDatabase("test", columns, dtypes, adt, vals);
		return engine;
	}

}
