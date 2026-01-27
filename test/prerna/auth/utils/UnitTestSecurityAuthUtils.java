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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.ReadOnlyAccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class UnitTestSecurityAuthUtils {

	static List<String> clearSecurityDB(IRDBMSEngine securityDb, List<String> tables) throws SQLException {
		Statement statement = null;
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = securityDb.getConnection();
			assertTrue(connection.getMetaData().getURL().contains("junit"));

			if (tables != null) {
				ps = connection.prepareStatement(
						"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
				ps.execute();
				rs = ps.getResultSet();
				List<String> al = new ArrayList<>();
				while (rs.next()) {
					al.add(rs.getString(1));
				}

				al.remove("PERMISSION");
				al.remove("PASSWORD_RULES");
				al.remove("ENGINEMETAKEYS");
				tables = al;
			}

			statement = connection.createStatement();
			for (String x : tables) {
				statement.addBatch("DELETE FROM " + x);
			}
			statement.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			assert statement != null;
			statement.close();
			connection.close();
		}

		return tables;
	}

	static void dumpQuery(String query, RDBMSNativeEngine securityDb) {
		try {
			doDumpQuery(query, securityDb);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void doDumpQuery(String query, RDBMSNativeEngine securityDb) throws SQLException {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = securityDb.getConnection();
			assertTrue(connection.getMetaData().getURL().contains("junit"));
			ps = connection.prepareStatement(query);
			ps.execute();
			rs = ps.getResultSet();
			printResults(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			assert connection != null;
			connection.close();
		}
	}

	static void dumpTable(String tableName, RDBMSNativeEngine securityDb) {
		try {
			doDumpTable(tableName, securityDb);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void doDumpTable(String tableName, RDBMSNativeEngine securityDb) throws SQLException {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = securityDb.getConnection();
			assertTrue(connection.getMetaData().getURL().contains("junit"));
			ps = connection.prepareStatement("SELECT * FROM " + tableName);
			ps.execute();
			rs = ps.getResultSet();
			printResults(rs);

		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			assert connection != null;
			connection.close();
		}
	}

	private static void printResults(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int cols = rsmd.getColumnCount();
		List<String[]> rows = new ArrayList<>();
		String[] headers = new String[cols];
		int[] widths = new int[cols];

		for (int i = 1; i <= cols; i++) {
			headers[i - 1] = rsmd.getColumnName(i);
			widths[i - 1] = headers[i - 1].length();
		}

		while (rs.next()) {
			String[] row = new String[cols];
			for (int i = 1; i <= cols; i++) {
				String val = rs.getString(i);
				if (val == null) {
					val = "NULL";
				}
				row[i - 1] = val;
				widths[i - 1] = Math.max(widths[i - 1], val.length());
			}
			rows.add(row);
		}

		StringBuilder border = new StringBuilder("+");
		for (int w : widths) {
			border.append(filler(w + 2, '-')).append("+");
		}
		System.out.println(border);
		System.out.print("|");
		for (int i = 0; i < cols; i++) {
			System.out.printf(" %-" + widths[i] + "s |", headers[i]);
		}
		System.out.println();
		System.out.println(border);

		for (String[] row : rows) {
			System.out.print("|");
			for (int i = 0; i < cols; i++) {
				System.out.printf(" %-" + widths[i] + "s |", row[i]);
			}
			System.out.println();
		}
		System.out.println(border);
	}

	static String filler(int count, char filler) {
		char[] arr = new char[count];
		Arrays.fill(arr, filler);
		return new String(arr);
	}

	static AccessToken createAccessToken(String prefix) {
		return doCreateAccessToken(prefix + "id", prefix + "name", prefix + "id", prefix + "@test.com");
	}

	private static AccessToken doCreateAccessToken(String id, String name, String username, String email) {
		AccessToken at = new AccessToken();
		at.setName(name);
		at.setId(id);
		at.setUsername(username);
		at.setEmail(email);
		at.setProvider(AuthProvider.NATIVE);
		return at;
	}

	static User createUser(String prefix, boolean admin) {
		return doCreateUser(prefix + "id", prefix + "name", prefix + "id", prefix + "@test.com", admin);
	}

	static User createAdminAddedUser(String prefix, boolean admin) {
		return doCreateUser(prefix + "@test.com", "ADMIN_ADDED_USER", "ADMIN_ADDED_USER", prefix + "@test.com", admin);
	}

	static User doCreateUser(String id, String name, String username, String email, boolean admin) {
		User user = new User();
		user.setPrimaryLogin(AuthProvider.NATIVE);
		AccessToken at = doCreateAccessToken(id, name, username, email);
		user.setAccessToken(at);

		assertTrue(SecurityUpdateUtils.registerUser(at.getId(), at.getName(), at.getEmail(), "Test123!",
				AuthProvider.NATIVE.getLabel(), "5555555555", "001", "US", admin, false, false, null, null, null,
				null));

		return user;
	}

	static void createProject(String id, String name, User user) {
		createProject(id, name, user, false);
	}

	static void createProject(String id, String name, User user, boolean hasPortal) {
		String userId = user.getPrimaryLoginToken().getId();
		SecurityProjectUtils.addProject(id, name, "APP", null, hasPortal, null, false, user);
		SecurityProjectUtils.addProjectOwner(user, id, userId);
	}

	static void createEngine(String id, String name, User user) {
		String userId = user.getPrimaryLoginToken().getId();
		SecurityEngineUtils.addEngine(id, name, IEngine.CATALOG_TYPE.DATABASE, null, null, false, user);
		SecurityEngineUtils.addEngineOwner(id, userId);
	}

	static void createEngineGlobal(String id, String name, User user) {
		String userId = user.getPrimaryLoginToken().getId();
		SecurityEngineUtils.addEngine(id, name, IEngine.CATALOG_TYPE.DATABASE, null, null, true, user);
		SecurityEngineUtils.addEngineOwner(id, userId);
	}

	static void createEngine(String id, String name, IEngine.CATALOG_TYPE catalogType, User user) {
		String userId = user.getPrimaryLoginToken().getId();
		SecurityEngineUtils.addEngine(id, name, catalogType, null, null, false, user);
		SecurityEngineUtils.addEngineOwner(id, userId);
	}

	static void createEngineWithSubtype(String id, String name, User user, String subtype) {
		String userId = user.getPrimaryLoginToken().getId();
		SecurityEngineUtils.addEngine(id, name, IEngine.CATALOG_TYPE.DATABASE, subtype, null, false, user);
		SecurityEngineUtils.addEngineOwner(id, userId);
	}

	static void addPermissionsToUserForEngine(User user, String userId, String engineId, String permission) {
		List<Map<String, Object>> permissions = List.of(Map.of("userid", userId, "permission", permission));
		try {
			SecurityEngineUtils.addEngineUserPermissions(user, engineId, permissions);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	static void addPermissionsToUserForProject(User user, String pid, String uid, String permission)
			throws IllegalAccessException {
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		List<Map<String, String>> permissions = List.of(Map.of("userid", uid, "permission", permission));
		SecurityProjectUtils.addProjectUserPermissions(user, pid, permissions, endDate);
	}

	static void createGroup(User user, String groupId, String groupType) {
		try {
			AdminSecurityGroupUtils.getInstance(user).addGroup(user, groupId, groupType, "short description");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	static void addUserToGroupAsUser(User user, String groupId) throws Exception {
		String userId = user.getPrimaryLoginToken().getId();
		String userType = user.getPrimaryLoginToken().getProvider().getLabel();
		addUserToGroup(user, groupId, userId, userType);
	}

	static void addUserToGroup(User user, String groupId, String userId, String userType) throws Exception {
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		AdminSecurityGroupUtils.getInstance(user).addUserToGroup(user, groupId, userId, userType, endDate);
	}

	static void addUserTokenToGroup(User user, String groupid, String groupType) {
		ReadOnlyAccessToken at = (ReadOnlyAccessToken) user.getAccessToken(AuthProvider.NATIVE);
		AccessToken newAt = AccessToken.copyToken(at);
		Collection<String> existingUserGroups = newAt.getUserGroups();
		existingUserGroups.add(groupid);
		newAt.setUserGroups(new HashSet<>(existingUserGroups));
		newAt.setUserGroupType(groupType);
		user.setAccessToken(newAt);
	}

	static void setupImageDir(Path semossDir, String... emptyImages) {
		Path stock = semossDir.resolve("images").resolve("stock");

		try {
			Files.createDirectories(stock);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		Path colorLogo = stock.resolve("color-logo.png");
		try {
			if (!Files.exists(colorLogo)) {
				Files.createFile(colorLogo);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		for (String image : emptyImages) {
			Path stockImage = stock.resolve(image);
			try {
				if (!Files.exists(stockImage)) {
					Files.createFile(stockImage);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void createInsight(String projectId, String insightId, String insightName, String layout) {
		SecurityInsightUtils.addInsight(projectId, insightId, insightName, false, layout, false, 0, null, null, false,
				null, null);
	}

	static Path createSmssFileFromProps(Properties securityProps, String folder, String filename) throws IOException {
		Path secSmss = Paths.get(folder + File.separator + filename);
		try (BufferedWriter bufferedWriter = new BufferedWriter(
				new OutputStreamWriter(Files.newOutputStream(secSmss)))) {
			for (Map.Entry<Object, Object> entry : securityProps.entrySet()) {
				String key = (String) entry.getKey();
				String value = (String) entry.getValue();
				bufferedWriter.write(key + "=" + value);
				bufferedWriter.newLine();
			}
		}
		return secSmss;
	}

	static Path createSmssFileFromProps(Properties securityProps, Path folder, String filename) {
		Path secSmss = Paths.get(folder + File.separator + filename);
		if (Files.exists(secSmss)) {
			try {
				Files.delete(secSmss);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		try (BufferedWriter bufferedWriter = new BufferedWriter(
				new OutputStreamWriter(Files.newOutputStream(secSmss)))) {
			for (Map.Entry<Object, Object> entry : securityProps.entrySet()) {
				String key = (String) entry.getKey();
				String value = (String) entry.getValue();
				bufferedWriter.write(key + "=" + value);
				bufferedWriter.newLine();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return secSmss;
	}

	static Properties getDefaultDBProperties(String dbName) {
		Properties securityProps = getDefaultDatabaseProperties();
		securityProps.setProperty(Constants.ENGINE, dbName);
		securityProps.setProperty(Constants.OWL, dbName + "_OWL.OWL");

		return securityProps;
	}

	private static Properties getDefaultDatabaseProperties() {
		Properties props = new Properties();
		props.setProperty(Constants.ENGINE_TYPE, "prerna.engine.impl.rdbms.H2EmbeddedServerEngine");
		props.setProperty(Constants.RDBMS_TYPE, "H2_DB");
		props.setProperty("DATABASE", "");
		props.setProperty("SCHEMA", "PUBLIC");
		props.setProperty("DRIVER", "org.h2.Driver");
		props.setProperty(Constants.USERNAME, "sa");
		props.setProperty(Constants.PASSWORD, "");
		props.setProperty(Constants.CONNECTION_URL, "jdbc:h2:nio:@BaseFolder@/db/@ENGINE@/database");
		props.setProperty(Constants.DATABASE_ZONEID, "UTC");
		return props;
	}

	public static void addEngineStoreProp(String testdb, Path smssFile) {
		DIHelper.getInstance().setEngineProperty(testdb + "_" + Constants.STORE, smssFile.toAbsolutePath().toString());
	}

	public static void removeEngineStoreProp(String testdb) {
		DIHelper.getInstance().removeEngineProperty(testdb + "_" + Constants.STORE);
	}

	static Properties getDefaultOpenAiProperties(String engineId) {
		Properties props = getDefaultOpenAiProperties();
		props.setProperty(Constants.ENGINE, engineId);

		return props;
	}

	private static Properties getDefaultOpenAiProperties() {
		Properties props = new Properties();

		props.setProperty(Constants.ENGINE_TYPE, "prerna.engine.impl.model.OpenAiEngine");

		return props;
	}
}
