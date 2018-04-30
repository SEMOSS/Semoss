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
package prerna.auth;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.google.gson.internal.StringMap;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public class UserPermissionsMasterDB {
	private RDBMSNativeEngine securityDB;
	
	public UserPermissionsMasterDB() {
		securityDB = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * 
	 * @param userName	String representing the name of the user to add
	 */
	public Boolean addUser(User newUser) {
		ISelectWrapper sjsw = Utility.processQuery(securityDB, "SELECT NAME FROM USER WHERE ID='" + newUser.getId() + "';");
		if(!sjsw.hasNext()) {			
			String query = "INSERT INTO User (id, name, email, type, admin) VALUES ('" + newUser.getId() + "', '"+ newUser.getName() + "', '" + newUser.getEmail() + "', '" + newUser.getLoginType() + "', '" + newUser.isAdmin() + "');";
			securityDB.insertData(query);
			securityDB.commit();
		}
		
		return true;
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * 
	 * @param userName	String representing the name of the user to add
	 */
	public Boolean isUserAdmin(String userId) {
		String query = "SELECT ADMIN FROM USER WHERE ID='" + userId + "';";
		ArrayList<String[]> ret = runQuery(query);
		if(!ret.isEmpty()) {
			return Boolean.parseBoolean(ret.get(0)[0]);
		}
		
		return false;
	}
	
	/**
	 * Adds user as owner for a given engine, giving him/her all permissions.
	 * 
	 * @param engineName	Name of engine user is being added as owner for
	 * @param userId		ID of user being made owner
	 * @return				true or false for successful addition
	 */
	public Boolean addEngineAndOwner(String engineName, String userId) {
		//Add the engine to the ENGINE table
		String query = "INSERT INTO Engine VALUES (NULL, '" + engineName + "');";
		Statement stmt = securityDB.execUpdateAndRetrieveStatement(query, false);
		int id = -1;
		ResultSet rs = null;
		try {
			rs = stmt.getGeneratedKeys();
			while (rs.next()) 
			{
			   id = rs.getInt(1);
			   if(id < 1) {
				   return false;
			   }
			}
		} catch(SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		//Add the user to the permissions table as the owner for the engine
		query = "INSERT INTO EnginePermission VALUES (NULL, " + id + ", '" + userId + "', " + EnginePermission.OWNER.getId() + ");";
		Statement stmt2 = securityDB.execUpdateAndRetrieveStatement(query, true);
		if(stmt2 != null) {
			securityDB.commit();
			return true;
		}

		return false;
	}
	
	public boolean addEngineAccessRequest(String engineName, String userRequestedBy) {
		//Get the owner for a given DB and create the access request
		ArrayList<String[]> ret = getEngineIdAndOwner(engineName);
		if(ret != null && !ret.isEmpty()) {
			String engineId = ret.get(0)[0];
			String ownerUserId = ret.get(0)[1];
			String query = "INSERT INTO AccessRequest VALUES (NULL, '" + userRequestedBy + "', '" + ownerUserId + "', " + engineId + ", " + EnginePermission.READ_ONLY.getId() + ");";
			Statement stmt = securityDB.execUpdateAndRetrieveStatement(query, true);
			if(stmt != null) {
				securityDB.commit();
				return true;
			}
		}
		return false;
	}
	
	public ArrayList<EngineAccessRequest> getEngineAccessRequestsForUser(String userId) {
		ArrayList<EngineAccessRequest> requests = new ArrayList<EngineAccessRequest>();
		
		String query = "SELECT a.ID AS MyID, u.NAME AS Username, e.NAME AS Engine FROM AccessRequest a, User u, Engine e WHERE a.SUBMITTEDTO='" + userId + "' AND a.SUBMITTEDBY=u.ID AND a.ENGINE=e.ID;";
		ArrayList<String[]> ret = runQuery(query);
		for(String[] row : ret) {
			requests.add(new EngineAccessRequest(row[0], row[1], row[2]));
		}
		
		return requests;
	}
	
	public ArrayList<String> getEngineAccessRequestsByUser(String userId) {
		ArrayList<String> requests = new ArrayList<String>();
		
		String query = "SELECT DISTINCT Engine.NAME AS ENGINENAME FROM AccessRequest, Engine WHERE AccessRequest.SUBMITTEDBY='" + userId + "' AND AccessRequest.ENGINE=Engine.ID;";
		ArrayList<String[]> ret = runQuery(query);
		for(String[] row : ret) {
			requests.add(row[0]);
		}
		
		return requests;
	}
	
	public Boolean processEngineAccessRequest(String requestId, String approvingUserId, String[] permissions) {
		String userId = "", engine = "";
		String query = "SELECT ENGINE, SUBMITTEDBY, PERMISSION FROM AccessRequest WHERE SUBMITTEDTO='" + approvingUserId + "' AND ID=" + requestId + ";";
		ArrayList<String[]> ret = runQuery(query);
		
		//Add the permission(s) and delete the access request
		if(permissions.length > 0) {
			for(String[] row : ret) {
				query = "INSERT INTO EnginePermission VALUES (NULL, " + row[0] + ", " + row[1] + ", " + row[2] + ");";
				securityDB.execUpdateAndRetrieveStatement(query, true);
			}
		}
		
		query = "DELETE FROM AccessRequest WHERE ID=" + requestId + ";";
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		return true;
	}
	
	/**
	 * Returns a list of all engines that a given user is listed as an owner for.
	 * 
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public ArrayList<String> getUserOwnedEngines(String userId) {
		ArrayList<String> engines = new ArrayList<String>();
		
		ArrayList<String[]> ret = runQuery("SELECT DISTINCT Engine.NAME FROM Engine, EnginePermission, Permission "
				+ "WHERE EnginePermission.USER='" + userId + "' AND Permission.ID=" + EnginePermission.OWNER.getId() + " AND EnginePermission.PERMISSION=Permission.ID AND Engine.ID=EnginePermission.ENGINE");
		for(String[] row : ret) {
			engines.add(row[0]);
		}
		
		return engines;
	}
	
	/**
	 * Returns a list of all engines that a given user can see (has been given any level of permission).
	 * 
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public HashSet<String> getUserAccessibleEngines(String userId) {
		HashSet<String> engines = new HashSet<String>();
		ArrayList<String[]> ret = new ArrayList<String[]>();
		
		String query = "";
		if(isUserAdmin(userId)) {
			query = "SELECT DISTINCT Engine.name FROM Engine";
			ret = runQuery(query);
		} else {
			query = "SELECT DISTINCT Engine.name FROM Engine, EnginePermission WHERE EnginePermission.USER='" + userId + "' AND Engine.ID=EnginePermission.ENGINE;";
			ret = runQuery(query);
			
			query = "SELECT DISTINCT e.NAME AS ENGINENAME FROM Engine e, User u, GroupEnginePermission gep, GroupMembers gm, Permission p "
					+ "WHERE u.ID='" + userId + "' AND gm.MEMBERID=u.ID AND gm.GROUPID=gep.GROUPID AND gep.PERMISSION=p.ID";
			ret.addAll(runQuery(query));
		}
		
		for(String[] row : ret) {
			engines.add(row[0]);
		}
		
		return engines;
	}
	
//	/**
//	 * Returns list of permissions that a given user has for a given engine.
//	 * 
//	 * @param engine	Engine for which user has permissions
//	 * @param userId	ID of the user
//	 * @return			Table of Permission->true/false for that specific permission
//	 */
//	public Hashtable<String, Boolean> getUserPermissionsForEngine(String engine, String userId) {
//		ISelectWrapper sjsw = Utility.processQuery(securityDB, MasterDatabaseQueries.GET_PERMISSIONS_FOR_ENGINE_QUERY.replace("@ENGINE_NAME@", engine).replace("@USER_ID@", userId));
//		String[] names = sjsw.getVariables();
//		Hashtable<String, Boolean> permissions = new Hashtable<String, Boolean>();
//		while(sjsw.hasNext()) {
//			ISelectStatement sjss = sjsw.next();
//			for(int i = 0; i < names.length; i++) {
//				permissions.put(names[i], Boolean.parseBoolean(sjss.getVar(names[i]).toString()));
//			}
//		}
//		
//		return permissions;
//	}
	
	/**
	 * Returns all engines for which a given user has a given set of permissions.
	 * 
	 * @param user			ID of the user
	 * @param permissions	Array of permissions needed
	 * @return				List of engine names
	 */
	public ArrayList<String> getEnginesForUserAndPermissions(String user, EnginePermission[] permissions) {
		ArrayList<String> engines = new ArrayList<String>();
		String query = "SELECT DISTINCT e.NAME FROM Engine e, EnginePermission ep, Permission p WHERE ep.USER='" + user + "' AND e.ID=ep.ENGINE AND ep.PERMISSION=p.ID AND (p.NAME='";
		
		if(permissions.length > 0) {
			query = query + permissions[0].getPermission();
		}
		for(int i = 1; i < permissions.length; i++) {
			query = query.concat("' OR p.NAME='" + permissions[i].getPermission());
		}
		
		query = query + "');";
		
		ArrayList<String[]> ret = runQuery(query);
		for(String[] row : ret) {
			engines.add(row[0]);
		}
		
		return engines;
	}
	
	/**
	 * Returns a list of owners for a given engine.
	 * 
	 * @param engineName	Name of engine being queried
	 * @return				List of user IDs noted as engine owners
	 */
	public ArrayList<String[]> getEngineIdAndOwner(String engineName) {
		return runQuery("SELECT DISTINCT EnginePermission.ENGINE, EnginePermission.USER FROM Engine, EnginePermission, Permission WHERE Engine.NAME='" + engineName + "' AND Engine.ID=EnginePermission.ENGINE AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId());
	}
	
	/**
	 * Deletes a given database, its associated owner's group, and all permissions relationships.
	 * 
	 * @param user			ID of the user
	 * @param engineName	Name of the engine to be deleted
	 * @return				true/false for successful deletion
	 */
	public Boolean deleteEngine(String user, String engineName) {
		String query = "DELETE Engine, EnginePermission FROM Engine INNER JOIN EnginePermission ON Engine.ID=EnginePermission.ENGINE WHERE Engine.NAME='" + engineName 
				+ "' AND EnginePermission.USER='" + user + "' AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + ";";
		securityDB.execUpdateAndRetrieveStatement(query, true);
		
		query = "DELETE FROM InsightExecution WHERE InsightExecution.DATABASE='" + engineName + "';";
		securityDB.execUpdateAndRetrieveStatement(query, true);
		
		securityDB.commit();
		
		return true;
	}
	
	public StringMap<ArrayList<StringMap<String>>> getGroupsAndMembersForUser(String userId) {
		String query = "SELECT DISTINCT ug.NAME AS GroupName, u.ID AS ID, u.NAME AS MEMBERNAME, u.EMAIL AS EMAIL FROM UserGroup ug, User u, GroupMembers gm WHERE ug.OWNER='" + userId + "' AND ug.ID=gm.GROUPID AND gm.MEMBERID=u.ID;";
		
		ArrayList<String[]> groups = runQuery(query);
		StringMap<ArrayList<StringMap<String>>> ret = new StringMap<ArrayList<StringMap<String>>>();
		
		for(String[] group : groups) {
			String groupName = group[0];
			StringMap<String> user = new StringMap<String>();
			user.put("id", group[1]);
			user.put("name", group[2]);
			user.put("email", group[3]);
			
			if(ret.get(groupName) == null) {
				ArrayList<StringMap<String>> newGroup = new ArrayList<StringMap<String>>();
				newGroup.add(user);
				ret.put(groupName, newGroup);
			} else {
				ret.get(groupName).add(user);
			}
		}
		
		return ret;
	}
	
	public ArrayList<String[]> getAllEnginesOwnedByUser(String userId) {
		String query = "SELECT DISTINCT Engine.NAME AS EngineName FROM Engine, User, UserGroup, GroupMembers, Permission, EnginePermission, GroupEnginePermission "
				+ "WHERE User.ID='" + userId + "' "
					+ "AND ((User.ID=EnginePermission.USER AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + ") "
						+ "OR (UserGroup.ID=GroupEnginePermission.GROUPID AND GroupEnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + " "
							+ "AND UserGroup.ID=GroupMembers.GROUPID AND GroupMembers.MEMBERID='" + userId + "') )";
		
		return runQuery(query);
	}
	
	public HashMap<String, ArrayList<StringMap<String>>> getAllPermissionsGrantedByEngine(String userId, String engineName) {
		String query = "SELECT DISTINCT ug.NAME AS GROUPNAME, p.NAME AS PERMISSIONNAME FROM User u, UserGroup ug, Engine e, EnginePermission ep, GroupEnginePermission gep, Permission p "
				+ "WHERE ug.ID=gep.GROUPID "
					+ "AND e.NAME='" + engineName + "' "
					+ "AND gep.ENGINE=e.ID "
					+ "AND gep.PERMISSION=p.ID "
					+ "AND ep.ENGINE=e.ID AND ep.PERMISSION=" + EnginePermission.OWNER.getId() + " AND ep.USER='" + userId + "';";
		
		ArrayList<StringMap<String>> groups = new ArrayList<StringMap<String>>();
		for(String[] groupPermissions : runQuery(query)) {
			StringMap<String> map = new StringMap<String>();
			map.put("name", groupPermissions[0]);
			map.put("permission", groupPermissions[1]);
			groups.add(map);
		}
		
		
		ArrayList<String> engines = getUserOwnedEngines(userId);
		query = "SELECT DISTINCT u.ID AS ID, u.NAME AS USERNAME, p.NAME AS PERMISSIONNAME, u.EMAIL AS EMAIL FROM User u, Engine e, EnginePermission ep, Permission p "
				+ "WHERE u.ID=ep.USER "
					+ "AND ep.ENGINE=e.ID "
					+ "AND ep.PERMISSION=p.ID "
					+ "AND e.NAME IN ('";
		
		for(int i = 0; i < engines.size(); i++) {
			if(i != engines.size()-1) {
				query += engines.get(i) + "', '";
			} else {
				query += engines.get(i);
			}
		}
		query += "');";
		
		ArrayList<StringMap<String>> users = new ArrayList<StringMap<String>>();
		for(String[] userPermissions : runQuery(query)) {
			StringMap<String> map = new StringMap<String>();
			map.put("id", userPermissions[0]);
			map.put("name", userPermissions[1]);
			map.put("permission", userPermissions[2]);
			map.put("email", userPermissions[3]);
			users.add(map);
		}
		
		HashMap<String, ArrayList<StringMap<String>>> ret = new HashMap<String, ArrayList<StringMap<String>>>();
		ret.put("groups", groups);
		ret.put("users", users);
		
		return ret;
	}
	
	public ArrayList<StringMap<String>> searchForUser(String searchTerm) {
		String query = "SELECT DISTINCT User.ID AS ID, User.NAME AS NAME, User.EMAIL AS EMAIL FROM User WHERE UPPER(User.NAME) LIKE UPPER('%" + searchTerm + "%') OR UPPER(User.EMAIL) LIKE UPPER('%" + searchTerm + "%');";
		ArrayList<StringMap<String>> users = new ArrayList<StringMap<String>>();
		
		for(String[] s : runQuery(query)) {
			StringMap<String> map = new StringMap<String>();
			map.put("id", s[0]);
			map.put("name", s[1]);
			map.put("email", s[2]);
			users.add(map);
		}
		
		return users;
	}
	
	public ArrayList<StringMap<String>> getAllEnginesAndPermissionsForUser(String userId) {
		ArrayList<String[]> ret = new ArrayList<String[]>();
		String query = "SELECT DISTINCT e.NAME AS ENGINENAME, u.NAME AS OWNER, p.NAME AS PERMISSION FROM Engine e, User u, EnginePermission ep, Permission p "
				+ "WHERE u.ID='" + userId + "' AND ep.USER=u.ID AND ep.PERMISSION=p.ID";
		
		ret = runQuery(query);
		
		query = "SELECT DISTINCT e.NAME AS ENGINENAME, u.NAME AS OWNER, p.NAME AS PERMISSIONNAME FROM Engine e, User u, GroupEnginePermission gep, GroupMembers gm, Permission p "
				+ "WHERE u.ID='" + userId + "' AND gm.MEMBERID=u.ID "
					+ "AND gm.GROUPID=gep.GROUPID AND gep.PERMISSION=p.ID";
		
		ret.addAll(runQuery(query));
		
		ArrayList<StringMap<String>> list = new ArrayList<StringMap<String>>();
		for(String[] eng : ret) {
			StringMap<String> map = new StringMap<String>();
			map.put("name", eng[0]);
			map.put("owner", eng[1]);
			map.put("permission", eng[2]);
			list.add(map);
		}
		
		return list;
	}
	
	public Boolean addGroup(String userId, String groupName, ArrayList<String> users) {
		String query = "INSERT INTO UserGroup VALUES (NULL, '" + groupName + "', '" + userId + "');";
		Statement stmt = securityDB.execUpdateAndRetrieveStatement(query, false);
		int id = -1;
		ResultSet rs = null;
		try {
			rs = stmt.getGeneratedKeys();
			while (rs.next()) {
			   id = rs.getInt(1);
			   if(id < 1) {
				   return false;
			   }
			}
		} catch(SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		for(String user : users) {
			query = "INSERT INTO GroupMembers VALUES (" + id + ", '" + user + "');";
			securityDB.insertData(query);
		}
		
		securityDB.commit();
		
		return true;
	}
	
	public Boolean removeGroup(String userId, String groupName) {
		String query = "DELETE FROM GroupEnginePermission WHERE GroupEnginePermission.GROUPID IN (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.NAME='group1' AND UserGroup.OWNER='" + userId + "'); ";
		query += "DELETE FROM GroupMembers WHERE GroupMembers.GROUPID IN (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.NAME='" + groupName + "' AND UserGroup.OWNER='" + userId + "'); ";
		query += "DELETE FROM UserGroup WHERE UserGroup.NAME='" + groupName + "' AND UserGroup.OWNER='" + userId + "';";
		
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		return true;
	}
	
	public Boolean addUserToGroup(String userId, String groupName, String userIdToAdd) {
		String query = "INSERT INTO GroupMembers VALUES ((SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.NAME='" + groupName + "' AND UserGroup.OWNER='" + userId + "'), '" + userIdToAdd + "');";
		
		securityDB.insertData(query);
		securityDB.commit();
		
		return true;
	}
	
	public Boolean removeUserFromGroup(String userId, String groupName, String userToRemove) {
		String query = "DELETE FROM GroupMembers WHERE GroupMembers.MEMBERID='" + userToRemove + "' AND GroupMembers.GROUPID IN "
				+ "(SELECT DISTINCT UserGroup.ID AS GROUPID FROM UserGroup WHERE UserGroup.OWNER='" + userId + "');";
		
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		return true;
	}
	
	public Boolean setPermissionsForGroup(String userId, String groupName, String engineName, EnginePermission[] permissions) {
		for(int i = 0; i < permissions.length; i++) {
			String query = "INSERT INTO GROUPENGINEPERMISSION VALUES ("
					+ "(SELECT DISTINCT Engine.ID FROM Engine, EnginePermission WHERE Engine.NAME='" + engineName + "' AND EnginePermission.USER='" + userId + "' "
							+ "AND EnginePermission.ENGINE=Engine.ID AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + "), "
					+ "(SELECT DISTINCT UserGroup.ID FROM UserGroup WHERE UserGroup.NAME='" + groupName + "' AND UserGroup.OWNER='" + userId + "'), "
					+ permissions[i].getId() + ")";
			
			securityDB.insertData(query);
		}
		securityDB.commit();
		
		return true;
	}
	
	public Boolean removeAllPermissionsForGroup(String userId, String groupName, String engineName) {
		String query = "DELETE FROM GroupEnginePermission "
				+ "WHERE GroupEnginePermission.ENGINE IN "
					+ "(SELECT DISTINCT Engine.ID FROM Engine, EnginePermission WHERE Engine.NAME='" + engineName + "' AND EnginePermission.ENGINE=Engine.ID "
						+ "AND EnginePermission.USER='" + userId + "' AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + ") "
				+ "AND GroupEnginePermission.GROUPID IN "
					+ "(SELECT DISTINCT UserGroup.ID FROM UserGroup WHERE UserGroup.NAME='" + groupName + "' AND UserGroup.OWNER='" + userId + "');";
		
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		return true;
	}
	
	public Boolean setPermissionsForUser(String userId, String engineName, String userToAdd, EnginePermission[] permissions) {
		for(int i = 0; i < permissions.length; i++) {
			String query = "INSERT INTO EnginePermission VALUES (NULL, (SELECT DISTINCT Engine.ID FROM Engine, EnginePermission WHERE Engine.NAME='" + engineName + "' AND EnginePermission.ENGINE=Engine.ID "
							+ "AND EnginePermission.USER='" + userId + "' AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + "), '" + userToAdd + "', " + permissions[i].getId() + ");";
			
			securityDB.insertData(query);
		}
		securityDB.commit();
		
		return true;
	}
	
	public Boolean removeAllPermissionsForUser(String userId, String engineName, String userToRemove) {
		String query = "DELETE FROM EnginePermission "
				+ "WHERE EnginePermission.ENGINE IN "
					+ "(SELECT DISTINCT Engine.ID FROM Engine, EnginePermission WHERE Engine.NAME='" + engineName + "' AND EnginePermission.ENGINE=Engine.ID "
						+ "AND EnginePermission.USER='" + userId + "' AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + ") "
				+ "AND EnginePermission.USER='" + userToRemove + "';";
		
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		return true;
	}
	
	public StringMap<ArrayList<String>> getInsightPermissionsForUser(String userId) {
		StringMap<ArrayList<String>> ret = new StringMap<ArrayList<String>>();
		String query = "SELECT E.NAME AS DBNAME, UIP.INSIGHTID AS INSIGHTID FROM UserInsightPermission UIP INNER JOIN Engine E ON E.ID=UIP.ENGINEID "
				+ " WHERE UIP.USERID='" + userId + "' ORDER BY E.NAME;";
		
		ArrayList<String[]> results = runQuery(query);
		ArrayList<String> insightIDs = new ArrayList<String>();
		String currDB = "";
		for(String[] row : results) {
			if(!currDB.equals(row[0])) {
				if(insightIDs.isEmpty()) {
					ret.put(currDB, insightIDs);
				}
				currDB = row[0];
			}
			insightIDs.add(row[1]);
		}
		ret.put(currDB, insightIDs);
		
		return ret;
	}
	
	public ArrayList<StringMap<String>> getUserPermissionsForInsight(String databaseName, String insightId) {
		ArrayList<StringMap<String>> ret = new ArrayList<StringMap<String>>();
		String query = "SELECT U.ID AS USERID, U.NAME AS USERNAME FROM UserInsightPermission UIP INNER JOIN Engine E ON E.ID=UIP.ENGINEID "
				+ " INNER JOIN User U ON UIP.USERID=U.ID WHERE E.NAME='" + databaseName + "' AND UIP.INSIGHTID='" + insightId + "' ORDER BY U.NAME;";
		
		ArrayList<String[]> results = runQuery(query);
		for(String[] userInfo : results) {
			StringMap<String> user = new StringMap<String>();
			user.put("id", userInfo[0]);
			user.put("name", userInfo[1]);
			ret.add(user);
		}
		
		return ret;
	}
	
	public Boolean addInsightPermissionsForUser(String loggedInUser, String userId, String engineName, String insightId) {
		String query = "SELECT e.ID AS ID FROM Engine e ";
		if(!isUserAdmin(loggedInUser)) {
			query += "INNER JOIN EnginePermission ep ON ep.ENGINE=e.ID WHERE ep.USER='" + loggedInUser + "' AND ep.PERMISSION=" 
					+ EnginePermission.OWNER.getId() + " AND e.NAME='" + engineName + "';";
		} else {
			query += " WHERE e.NAME='" + engineName + "' ";
		}
		
		String engineId = "";
		ArrayList<String[]> ret = runQuery(query);
		if(!ret.isEmpty()) {
			engineId = ret.get(0)[0];
			
			query = "INSERT INTO UserInsightPermission VALUES ('" + userId + "', " + engineId + ", '" + insightId + "');";
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
			
			return true;
		}
		
		return false;
	}
	
	public Boolean removeInsightPermissionsForUser(String loggedInUser, String userId, String engineName, String insightId) {
		String query = "SELECT e.ID AS ID FROM Engine e ";
		if(!isUserAdmin(loggedInUser)) {
			query += "INNER JOIN EnginePermission ep ON ep.ENGINE=e.ID WHERE ep.USER='" + loggedInUser + "' AND ep.PERMISSION=" 
					+ EnginePermission.OWNER.getId() + " AND e.NAME='" + engineName + "';";
		} else {
			query += " WHERE e.NAME='" + engineName + "' ";
		}
		
		String engineId = "";
		ArrayList<String[]> ret = runQuery(query);
		if(!ret.isEmpty()) {
			engineId = ret.get(0)[0];
			
			query = "DELETE FROM UserInsightPermission WHERE USERID='" + userId + "' AND ENGINEID=" + engineId + " AND INSIGHTID='" + insightId + "';";
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * 
	 */
	public Boolean createSeed(String seedName, String databaseName, String tableName, String columnName, Object RLSValue, String RLSJavaCode, String userId) {
		String query = "SELECT E.ID AS ENGINEID FROM Engine E WHERE E.NAME='" + databaseName + "';";
		ArrayList<String[]> results = runQuery(query);
		String databaseID = "";
		if(results != null && !results.isEmpty()) {
			databaseID = results.get(0)[0];
		}
		
		query = "INSERT INTO Seed VALUES (NULL, '" + seedName + "', " + databaseID + ", '" + tableName + "', '" + columnName + "', ";
		
		if(RLSValue != null) {
			query += "'" + RLSValue + "', " + "NULL, '" + userId + "');";
		} else if(RLSJavaCode != null && !RLSJavaCode.isEmpty()) {
			query += "NULL, '" + RLSJavaCode + "', '" + userId + "');";
		} else {
			query += "NULL, NULL, '" + userId + "');";
		}
		
		securityDB.insertData(query);
		
		return true;
	}
	
	public Boolean deleteSeed(String seedName, String userId) {
		String query = "DELETE FROM UserSeedPermission WHERE UserSeedPermission.SEEDID IN (SELECT s.ID AS SEEDID FROM Seed s WHERE s.OWNER='" + userId + "' AND s.NAME='" + seedName + "');";
		securityDB.execUpdateAndRetrieveStatement(query, true);
		
		query = "DELETE FROM Seed WHERE name='" + seedName + "' AND owner='" + userId + "';";
		securityDB.execUpdateAndRetrieveStatement(query, true);
		
		securityDB.commit();
		
		return true;
	}
	
	public Boolean addUserToSeed(String userId, String seedName, String loggedInUser) {
		if(!isUserAdmin(loggedInUser)) {
			return false;
		}
		
		String query = "SELECT S.ID AS SEEDID FROM Seed S WHERE S.NAME='" + seedName + "';";
		ArrayList<String[]> results = runQuery(query);
		
		for(String[] s : results) {	
			query = "INSERT INTO UserSeedPermission VALUES ('" + userId + "', " + s[0] + ");";
			
			try {
				securityDB.insertData(query);
			} catch(Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}
	
	public Boolean deleteUserFromSeed(String userId, String seedName, String loggedInUser) {
		if(!isUserAdmin(loggedInUser)) {
			return false;
		}
		
		String query = "SELECT S.ID AS SEEDID FROM Seed S WHERE S.NAME='" + seedName + "';";
		ArrayList<String[]> results = runQuery(query);
		
		for(String[] s : results) {
			query = "DELETE FROM UserSeedPermission WHERE userID='" + userId + "' AND seedID=" + s[0] + ";";
			
			if(!isUserAdmin(loggedInUser)) {
				return false;
			}
			
			try {
				securityDB.insertData(query);
			} catch(Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}
	
	public HashMap<String, HashMap<String, ArrayList<String>>> getMetamodelSeedsForUser(String userId) {
		String query = "SELECT DISTINCT E.NAME AS DB, S.TABLENAME AS TAB, S.COLUMNNAME AS COL "
				+ "FROM Seed S "
				+ "INNER JOIN Engine E ON S.DATABASEID=E.ID "
				+ "INNER JOIN UserSeedPermission USP ON USP.SEEDID=S.ID "
				+ "INNER JOIN User U ON U.ID=USP.USERID AND U.ID='" + userId + "' "
				+ "WHERE S.RLSVALUE=NULL "
				+ "ORDER BY DB, TAB;";
		
		return getMetamodelSeedsFromQuery(query);
	}
	
	/**
	 * Return seeds for a given user.
	 * 
	 * @param userId	ID of the user used for search
	 * @param owner		boolean value if searching for seeds owned by user (true) or applied to user (false)
	 * @return
	 */
	public ArrayList<StringMap<Object>> getMetamodelSeedsForUser(String userId, boolean owner) {
		ArrayList<StringMap<Object>> ret = new ArrayList<StringMap<Object>>();
		
		//Get the seed names that this user can see
		String query = "SELECT DISTINCT S.NAME AS SEEDNAME, S.DATABASEID AS DBID FROM SEED S ";
		if(owner && !isUserAdmin(userId)) {
			query += "WHERE S.OWNER='" + userId + "' ";
		} else if(!owner) {
			query += "INNER JOIN UserSeedPermission USP ON USP.SEEDID=S.ID WHERE USP.USERID='" + userId + "' AND S.RLSVALUE=NULL;";
		}
		
		ArrayList<String[]> seedNames = runQuery(query);
		StringMap<String> seedNameList = new StringMap<String>();
		for(String[] seed : seedNames) {
			seedNameList.put(seed[0], seed[1]);
		}
		
		for(String seedName : seedNameList.keySet()) {
			StringMap<Object> seedInfo = new StringMap<Object>();
			HashMap rules = new HashMap();
			
			query = "SELECT DISTINCT E.NAME AS DB, S.TABLENAME AS TAB, S.COLUMNNAME AS COL "
					+ "FROM Seed S "
					+ "INNER JOIN Engine E ON S.DATABASEID=E.ID "
					+ "WHERE S.NAME='" + seedName + "' "
					+ "ORDER BY DB, TAB;";
			
			seedInfo.put("seedName", seedName);
			seedInfo.put("dbId", seedNameList.get(seedName));
			seedInfo.put("rules", getMetamodelSeedsFromQuery(query));
			ret.add(seedInfo);
		}
		
		return ret;
	}
	
	public HashMap<String, HashMap<String, ArrayList<String>>> getMetamodelSeedsFromQuery(String query) {
		ArrayList<String[]> results = runQuery(query);
		
		HashMap<String, HashMap<String, ArrayList<String>>> dbToTables = new HashMap<String, HashMap<String, ArrayList<String>>>();
		HashMap<String, ArrayList<String>> tableToCols = new HashMap<String, ArrayList<String>>();
		ArrayList<String> cols = new ArrayList<String>();
		
		String currDB = "";
		String currTable = "";
		
		for(String[] row : results) {
			String rowDB = row[0];
			String rowTable = row[1];
			if(currDB.equals(rowDB)) {
				if(!currTable.equals(rowTable)) {
					if(!cols.isEmpty()) {
						tableToCols.put(currTable, cols);
					}
					cols = new ArrayList<String>();
					currTable = rowTable;
				}
				
				cols.add(row[2]);
			} else {
				if(!cols.isEmpty()) {
					tableToCols.put(currTable, cols);
					dbToTables.put(currDB, tableToCols);
				}
				
				cols = new ArrayList<String>();
				tableToCols = new HashMap<String, ArrayList<String>>();
				
				cols.add(row[2]);
				currDB = rowDB;
				currTable = rowTable;
			}
		}
		
		if(!cols.isEmpty()) {
			tableToCols.put(currTable, cols);
			dbToTables.put(currDB, tableToCols);
			
		}
		
		return dbToTables;
	}
	
	public StringMap<StringMap<ArrayList>> getRowLevelSeedsForUserAndEngine(String userId, String engineName) {
		StringMap<StringMap<ArrayList>> ret = new StringMap<StringMap<ArrayList>>();
		String query = "SELECT s.TABLENAME AS TABLENAME, s.COLUMNNAME AS COLUMNNAME, s.RLSVALUE AS RLSVALUE FROM Seed s "
				+ "INNER JOIN UserSeedPermission usp ON usp.SEEDID=s.ID "
				+ "INNER JOIN Engine e ON e.ID=s.DATABASEID "
				+ "WHERE e.NAME='" + engineName + "' AND usp.USERID='" + userId + "' AND s.RLSVALUE IS NOT NULL ORDER BY TABLENAME, COLUMNNAME	;";
		
		ArrayList<String[]> results = runQuery(query);
		for(String[] row : results) {
			String table = row[0];
			String col = row[1];
			String rlsValue = row[2];
			
			if(ret.containsKey(table)) {
				if(ret.get(table).containsKey(col)) {
					ret.get(table).get(col).add(rlsValue);
				} else {
					ArrayList newList = new ArrayList();
					newList.add(rlsValue);
					ret.get(table).put(col, newList);
				}
			} else {
				StringMap<ArrayList> newColValues = new StringMap<ArrayList>();
				ArrayList newList = new ArrayList();
				newList.add(rlsValue);
				newColValues.put(col, newList);
				ret.put(table, newColValues);
			}
		}
		
		return ret;
	}
	
	public Boolean addSeedToGroup(String groupId, String seedId) {
		String query = "INSERT INTO GroupSeedPermission VALUES ('" + groupId + "', " + seedId + ");";
		
		securityDB.insertData(query);
		
		return true;
	}
	
	
	
//	public HashMap<String, HashMap<String, HashMap<String, ArrayList<Object>>>> getRLSSeedsForUser(String userId) {
//		String query = "";
//		
//		return null;
//	}
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * 
	 * @param query		Query to be executed to retrieve engine names
	 * @return			List of engine names
	 */
	private ArrayList<String[]> runQuery(String query) {
		System.out.println("Executing security query: " + query);
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		String[] names = sjsw.getDisplayVariables();
		ArrayList<String[]> ret = new ArrayList<String[]>();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String[] rowValues = new String[names.length];
			for(int i = 0; i < names.length; i++) {
				 rowValues[i] = sjss.getVar(names[i]).toString();
			}
			ret.add(rowValues);
		}
		
		return ret;
	}
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * 
	 * @param query		Query to be executed to retrieve engine names
	 * @return			List of engine names
	 */
	private List<Object[]> runQuery2(String query) {
		System.out.println("Executing security query: " + query);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		List<Object[]> ret = new ArrayList<Object[]>();
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			ret.add(row.getValues());
		}
		
		return ret;
	}
	
	public boolean isUserReadOnlyInsights(String userId, String engineName, String rdbmsId) {
		String query = "SELECT DISTINCT ENGINE.NAME, USERINSIGHTPERMISSION.INSIGHTID FROM USERINSIGHTPERMISSION INNER JOIN ENGINE ON USERINSIGHTPERMISSION.ENGINEID = ENGINE.ID WHERE USERINSIGHTPERMISSION.USERID = '" + userId +  "' "
				+ "AND ENGINE.NAME = '" + engineName + "' AND USERINSIGHTPERMISSION.INSIGHTID = '" + rdbmsId + "' AND PERMISSION='2'";
		
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		if(sjsw.hasNext()) {
			return true;
		}
		
		return false;
	}
	
	/*
	 * 
	 * USER TRACKING METHODS BEGIN HERE
	 * 
	 */
	
	public void trackInsightExecution(String user, String db, String insightId, String session) {
		insightId = insightId.split("_")[1];
		String query = "SELECT Count FROM InsightExecution WHERE USER='" + user + "' AND DATABASE='" + db + "' AND INSIGHT='" + insightId + "' AND SESSION='" + session + "';";
		try {
			ArrayList<String[]> ret = runQuery(query);
			
			if(ret != null && !ret.isEmpty()) {
				query = "UPDATE InsightExecution SET Count=Count+1, LastExecuted=CURRENT_TIMESTAMP WHERE USER='" + user + "' AND DATABASE='" + db + "' AND INSIGHT='" + insightId + "' AND SESSION='" + session + "';";
				securityDB.execUpdateAndRetrieveStatement(query, true);
			} else {
				query = "INSERT INTO InsightExecution (USER, DATABASE, INSIGHT, COUNT, LASTEXECUTED, SESSION) VALUES ('" + user + "', '" + db + "', '" + insightId + "', 1, CURRENT_TIMESTAMP, '" + session + "');";
				securityDB.insertData(query);
			}
			
			securityDB.commit();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Returns all insight tracking info for a given user, ordered by Last Executed descending. 
	 * 
	 * @param userId	ID of the user
	 * @return			Database Name, Insight ID, Count, Last Executed, Session ID (in that order)
	 */
	public ArrayList<String[]> getExecutedInsightsForUser(String userId) {
		String query = "SELECT DATABASE, INSIGHT, COUNT, LASTEXECUTED, SESSION FROM InsightExecution WHERE USER='" + userId + "' GROUP BY SESSION, DATABASE, INSIGHT ORDER BY LASTEXECUTED DESC;";
		
		return runQuery(query);
	}
	
	/**
	 * Returns top insights executed for a given user, or all users if no user ID is passed in.
	 * 
	 * @param userId	ID of the user
	 * @param limit		number of insights to return
	 * @return			Database Name, Insight ID, Total Execution Count (in that order)
	 */
	public ArrayList<String[]> getTopInsightsExecutedForUser(String userId, String limit) {
		String query = "";
		if(query != null && !query.isEmpty()) {
			query = "SELECT DATABASE, INSIGHT, SUM(COUNT) AS TOTAL FROM InsightExecution WHERE USER='" + userId + "' GROUP BY DATABASE, INSIGHT ORDER BY TOTAL DESC";
		} else {
			query = "SELECT DATABASE, INSIGHT, SUM(COUNT) AS TOTAL FROM InsightExecution GROUP BY DATABASE, INSIGHT ORDER BY TOTAL DESC";
		}
		
		if(limit != null && !limit.isEmpty()) {
			query += " LIMIT " + limit;
		}
		
		return runQuery(query);
	}
}