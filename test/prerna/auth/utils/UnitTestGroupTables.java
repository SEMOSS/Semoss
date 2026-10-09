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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IRDBMSEngine;

/**
 * Reads and seeds the custom group tables (GROUPMANAGERS, CUSTOMGROUPASSIGNMENT
 * and SMSS_USER) with plain JDBC, so a test can set up rows without going
 * through the code it checks.
 */
final class UnitTestGroupTables {

	private static final String INSERT_MANAGER = "INSERT INTO GROUPMANAGERS (GROUPID, USERID, TYPE, DATEADDED, "
			+ "PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES (?,?,?,?,?,?)";
	private static final String INSERT_MEMBER = "INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID, USERID, TYPE, DATEADDED) "
			+ "VALUES (?,?,?,?)";
	private static final String INSERT_USER = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE) VALUES (?,?,?,?,?)";
	private static final String SELECT_MANAGERS = "SELECT USERID, TYPE FROM GROUPMANAGERS WHERE GROUPID=? ORDER BY USERID, TYPE";
	private static final String SELECT_MEMBERS = "SELECT USERID, TYPE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=? "
			+ "ORDER BY USERID, TYPE";

	private UnitTestGroupTables() {

	}

	static void insertManager(IRDBMSEngine securityDb, String groupId, String userId, String type, Timestamp dateAdded)
			throws SQLException {
		update(securityDb, INSERT_MANAGER, groupId, userId, type, dateAdded, "seed", "NATIVE");
	}

	static void insertManager(IRDBMSEngine securityDb, String groupId, String userId, String type) throws SQLException {
		insertManager(securityDb, groupId, userId, type, Timestamp.valueOf(LocalDateTime.now()));
	}

	static void insertMember(IRDBMSEngine securityDb, String groupId, String userId, String type) throws SQLException {
		update(securityDb, INSERT_MEMBER, groupId, userId, type, Timestamp.valueOf(LocalDateTime.now()));
	}

	/**
	 * Adds a user row directly, which allows the same id under more than one login
	 * type.
	 */
	static void insertUser(IRDBMSEngine securityDb, String id, String type) throws SQLException {
		update(securityDb, INSERT_USER, id, id + "name", id, id + "@test.com", type);
	}

	/**
	 * Adds many users in one batch, each also made a member of the group when the
	 * group id is not null.
	 */
	static void insertUsers(IRDBMSEngine securityDb, String idPrefix, int count, String type, String memberOfGroupId)
			throws SQLException {
		Timestamp now = Timestamp.valueOf(LocalDateTime.now());
		try (Connection connection = securityDb.getConnection();
				PreparedStatement users = connection.prepareStatement(INSERT_USER);
				PreparedStatement members = connection.prepareStatement(INSERT_MEMBER)) {
			for (int i = 0; i < count; i++) {
				String id = String.format("%s%03d", idPrefix, i);
				bind(users, id, id + "name", id, id + "@test.com", type);
				users.addBatch();
				if (memberOfGroupId != null) {
					bind(members, memberOfGroupId, id, type, now);
					members.addBatch();
				}
			}
			users.executeBatch();
			if (memberOfGroupId != null) {
				members.executeBatch();
			}
			commit(connection);
		}
	}

	/**
	 * @return the group's managers as {@code userid:type}, sorted
	 */
	static List<String> managers(IRDBMSEngine securityDb, String groupId) throws SQLException {
		return idAndTypes(securityDb, SELECT_MANAGERS, groupId);
	}

	/**
	 * @return the group's members as {@code userid:type}, sorted
	 */
	static List<String> members(IRDBMSEngine securityDb, String groupId) throws SQLException {
		return idAndTypes(securityDb, SELECT_MEMBERS, groupId);
	}

	/**
	 * @return the first column of the first row, or null when there are no rows
	 */
	static Object queryValue(IRDBMSEngine securityDb, String sql, Object... params) throws SQLException {
		try (Connection connection = securityDb.getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {
			bind(statement, params);
			try (ResultSet rs = statement.executeQuery()) {
				return rs.next() ? rs.getObject(1) : null;
			}
		}
	}

	private static List<String> idAndTypes(IRDBMSEngine securityDb, String sql, String groupId) throws SQLException {
		List<String> rows = new ArrayList<>();
		try (Connection connection = securityDb.getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {
			bind(statement, groupId);
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					rows.add(rs.getString(1) + ":" + rs.getString(2));
				}
			}
		}
		return rows;
	}

	private static void update(IRDBMSEngine securityDb, String sql, Object... params) throws SQLException {
		try (Connection connection = securityDb.getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {
			bind(statement, params);
			statement.executeUpdate();
			commit(connection);
		}
	}

	private static void bind(PreparedStatement statement, Object... params) throws SQLException {
		for (int i = 0; i < params.length; i++) {
			statement.setObject(i + 1, params[i]);
		}
	}

	private static void commit(Connection connection) throws SQLException {
		if (!connection.getAutoCommit()) {
			connection.commit();
		}
	}
}
