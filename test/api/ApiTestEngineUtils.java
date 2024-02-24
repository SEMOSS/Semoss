package api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
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
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.database.upload.rdbms.csv.RdbmsUploadTableDataReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.AbstractThemeUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ApiTestEngineUtils {
	
	private static Path ENGINES_CONFIG_FILE = Paths.get(ApiTests.TEST_CONFIG_DIRECTORY.toString(), "engines.txt");
	private static List<String> CORE_DBS = null;
	
	private static List<String> CURRENT_NAMES = new ArrayList<>();
	
	// DBs to clear, tables to avoid
	private static final List<Pair<String, List<String>>> DB_TO_CLEAR = Arrays.asList(
			Pair.of(Constants.LOCAL_MASTER_DB, Arrays.asList(new String[] {})),
			Pair.of(Constants.SECURITY_DB, Arrays.asList("PERMISSION")),
			//Pair.of(Constants.SCHEDULER_DB, new ArrayList<String>()), not initialized
			Pair.of(Constants.THEMING_DB, Arrays.asList(new String[] {})),
			Pair.of(Constants.USER_TRACKING_DB, Arrays.asList(new String[] {}))
			);
	

	
	static void checkDatabasePropMapping() {
		assertEquals(ApiTests.LMD_SMSS, ((String) DIHelper.getInstance().getEngineProperty(Constants.LOCAL_MASTER_DB + "_" + Constants.STORE)));
    	assertEquals(ApiTests.SECURITY_SMSS, ((String) DIHelper.getInstance().getEngineProperty(Constants.SECURITY_DB + "_" + Constants.STORE)));
    	assertEquals(ApiTests.SCHEDULER_SMSS, ((String) DIHelper.getInstance().getEngineProperty(Constants.SCHEDULER_DB + "_" + Constants.STORE)));
    	assertEquals(ApiTests.THEMES_SMSS, ((String) DIHelper.getInstance().getEngineProperty(Constants.THEMING_DB + "_" + Constants.STORE)));
    	assertEquals(ApiTests.UTDB_SMSS, ((String) DIHelper.getInstance().getEngineProperty(Constants.USER_TRACKING_DB + "_" + Constants.STORE)));
	}
	
	static void unloadDatabases() {
		DIHelper.getInstance().removeEngineProperty(Constants.LOCAL_MASTER_DB + "_" + Constants.STORE);
		DIHelper.getInstance().removeEngineProperty(Constants.SECURITY_DB + "_" + Constants.STORE);
		DIHelper.getInstance().removeEngineProperty(Constants.THEMING_DB + "_" + Constants.STORE);
		DIHelper.getInstance().removeEngineProperty(Constants.SCHEDULER_DB + "_" + Constants.STORE);
		DIHelper.getInstance().removeEngineProperty(Constants.USER_TRACKING_DB + "_" + Constants.STORE);
	}
	
	static void initalizeDatabases() throws Exception {
		initializeSemossDatabases();
	}

	private static void initializeSemossDatabases() throws Exception {
		doInitializeSemossDB(Constants.LOCAL_MASTER_DB, "databaseNewMaster.mv.db");
		MasterDatabaseUtility.initLocalMaster();
		
		doInitializeSemossDB(Constants.SECURITY_DB, "database.mv.db");
//		SecurityOwlCreator soc = new SecurityOwlCreator((RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB));
//		soc.remakeOwl();
		AbstractSecurityUtils.loadSecurityDatabase();
		
		doInitializeSemossDB(Constants.SCHEDULER_DB, "database.mv.db");
		// error when initializing
		// TODO: fix this later
		//SchedulerDatabaseUtility.startServer();
		
		doInitializeSemossDB(Constants.THEMING_DB, "database.mv.db");
		AbstractThemeUtils.loadThemingDatabase();
		
		doInitializeSemossDB(Constants.USER_TRACKING_DB, "databaseNewUserTracking.mv.db");
		UserTrackingUtils.initUserTrackerDatabase();
		
		
		createUser("ater", "Native", true);
	}

	public static void createUser(String userUserName, String type, boolean isAdmin) throws SQLException {
		Triple<String, String, String> cds = getTestDatabaseConnection(Constants.SECURITY_DB);
		
		try (Connection conn = DriverManager.getConnection(cds.getLeft(), cds.getMiddle(), cds.getRight())) {
			// TODO: change my email to something else
			String userPassword = "TestTest8*";
			String salt = SecurityQueryUtils.generateSalt();
			String hashed = SecurityQueryUtils.hash(userPassword, salt);
			
			String name = userUserName.substring(0, 1);
			String email = userUserName + "@" + userUserName + ".com";
			PreparedStatement ps = conn.prepareStatement("INSERT INTO SMSS_USER "
					+ "(NAME, EMAIL, \"TYPE\", ID, PASSWORD, SALT, USERNAME, ADMIN, PUBLISHER, EXPORTER, DATECREATED, LASTLOGIN, LASTPASSWORDRESET, LOCKED, PHONE, PHONEEXTENSION, COUNTRYCODE)\r\n"
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

	private static void doInitializeSemossDB(String name, String dbName) throws IOException {
		String smssPath = ApiTests.TEST_DB_DIRECTORY + File.separator + name + ".smss";
		String db = ApiTests.TEST_DB_DIRECTORY + File.separator + name + File.separator + dbName;
		
		if (Files.exists(Paths.get(db))) {
			Files.delete(Paths.get(db));
		}
		
		DIHelper.getInstance().setEngineProperty(name + "_" + Constants.STORE, smssPath);
	}
	
	
	public static void deleteAllDataAndAddUser() {
		for (Pair<String, List<String>> x : DB_TO_CLEAR) {
			Triple<String, String, String> connectionDetails = getTestDatabaseConnection(x.getLeft());
			connectAndClearDb(connectionDetails, x.getRight());
		}
		
		try {
			createUser("ater", "Native", true);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Could not add Default Native Admin user");
		}
	}
	
	private static void connectAndClearDb(Triple<String, String, String> connectionDetails, List<String> ignoredTables) {
		PreparedStatement ps = null;
		Statement st = null;
		try (Connection conn = DriverManager.getConnection(connectionDetails.getLeft(), connectionDetails.getMiddle(), 
				connectionDetails.getRight())) {
			assertTrue(connectionDetails.getLeft().contains("testfolder"));
			
			ps = conn.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
			ps.execute();
			ResultSet rs = ps.getResultSet();
			List<String> al = new ArrayList<>();
			while (rs.next()) {
				al.add(rs.getString(1));
			}
			ps.close();
			
			al.removeAll(ignoredTables);
			// delete * from databases
			st = conn.createStatement();
			for (String x : al) {
				st.addBatch("DELETE FROM " + x);
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
		String dbPath = Paths.get(ApiTests.TEST_DB_DIRECTORY, db + ".smss").toAbsolutePath().toString();
		Properties props = Utility.loadProperties(dbPath);
		String connection = props.getProperty(Constants.CONNECTION_URL);
		connection = connection.replaceAll("@BaseFolder@", ApiTests.TEST_BASE_DIRECTORY.replace('\\', '/'));
		connection = connection.replaceAll("@ENGINE@", db);

		String username = props.getProperty(Constants.USERNAME);
		String password = props.getProperty(Constants.PASSWORD);
		return Triple.of(connection, username, password);
	}
	
	public static void clearNonCoreDBs() throws IOException {
		List<String> dbsToAvoid = getDBsToAvoid();
		File f = Paths.get(ApiTests.TEST_DB_DIRECTORY).toFile();
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
			Path p = Paths.get(ApiTests.TEST_DB_DIRECTORY.toString(), delete);
			if (Files.isDirectory(p)) {
				Files.walk(p).sorted().map(Path::toFile).forEach(File::delete);
				if (Files.exists(p)) {
					Files.delete(p);
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
		return CORE_DBS;
	}

	@SuppressWarnings("unchecked")
	public static String addTestRdbmsDatabase(String name, List<String> columns, List<String> dataTypes, Map<String, String> additionalDataTypes, 
			List<List<String>> rowValues) {
		assertNotNull(name);
		assertNotNull(columns);
		assertNotNull(dataTypes);
		assertNotNull(rowValues);
		assertFalse("Database with named <" + name + "> already exists", CURRENT_NAMES.contains(name));
		assertEquals("Column name count and dataType count have to match up", columns.size(), dataTypes.size());
		assertTrue("Input must contain table data", rowValues.size() > 0);
		assertEquals("All row value lengths must match", 1, rowValues.stream().map(s -> s.size()).distinct().count());
		assertEquals("Data columns must have same size as column names and data types", rowValues.get(0).size(), columns.size());
		
		CURRENT_NAMES.add(name);
		
		Path path = Paths.get(ApiTests.TEST_INSIGHT_CACHE.toString(), name + ".csv");
		try {
			path = Files.createFile(path);
			List<String> lines = new ArrayList<>();
			lines.add(String.join(", ", columns));
			for (List<String> rv : rowValues) {
				lines.add(String.join(", ", rv));
			}
			Files.write(path, lines);
		} catch(Exception e) {
			fail(e.toString());
		}
		
		Map<String, String> dataType = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			dataType.put(columns.get(i), dataTypes.get(i));
		}
		Map<String, String> newHeaders = new HashMap<>();
		
		Map<String, String> descriptionMap = new HashMap<>();
		Map<String, String> logicalMap = new HashMap<>();
		String pixelCall = ApiTestUtils.buildPixelCall(RdbmsUploadTableDataReactor.class, 
				"database", Arrays.asList(name), 
				"filePath", Arrays.asList("\\" + name + ".csv"), 
				"delimiter", Arrays.asList(","), 
				"dataTypeMap", Arrays.asList(dataType), 
				"newHeaders", Arrays.asList(newHeaders), 
				"additionalDataTypes", Arrays.asList(additionalDataTypes), 
				"descriptionMap", Arrays.asList(descriptionMap), 
				"logicalNamesMap", Arrays.asList(logicalMap), 
				"existing", Arrays.asList(Boolean.FALSE));

		NounMetadata nm = ApiTestUtils.processPixel(pixelCall);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String engineId = (String) ret.get("database_id");
		return engineId;
	}
	
}
