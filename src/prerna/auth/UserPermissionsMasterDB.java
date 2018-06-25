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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.mindrot.jbcrypt.BCrypt;

import com.google.gson.internal.StringMap;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UserPermissionsMasterDB {
	
	/*
	 * This class is used for low level operations on the database
	 * For higher level functions, please use UserPermissionUtility
	 */
	
	private RDBMSNativeEngine securityDB;
	
	public UserPermissionsMasterDB() {
		securityDB = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
	}
	
	public IEngine getEngine() {
		return securityDB;
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Initialize a local security engine for the UserPermissionsMasterDB object for testing purposes.
	 * @param dbPath Security database location.
	 */
	public UserPermissionsMasterDB(String dbPath) {
		TestUtilityMethods.loadDIHelper();
		securityDB = new RDBMSNativeEngine();
		securityDB.setEngineId("security");
		securityDB.openDB(dbPath);
		DIHelper.getInstance().setLocalProperty("security", securityDB);
	}
	
	/**
	 * Testing Here.
	 * @param args
	 */
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();
		String dbPath = DIHelper.getInstance().getProperty("BaseFolder") + "\\db\\security.smss";
		UserPermissionsMasterDB auth = new UserPermissionsMasterDB(dbPath);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding data
	 */
	
	/**
	 * Add an engine into the security database
	 * Default to set as not global
	 */
	public void addEngine(String engineId, String engineName, String engineType, String engineCost) {
		addEngine(engineId, engineName, engineType, engineCost, false);
	}
	
	public void addEngine(String engineId, String engineName, String engineType, String engineCost, boolean global) {
		String query = "INSERT INTO ENGINE (NAME, ID, TYPE, COST, GLOBAL) "
				+ "VALUES ('" + engineName + "', '" + engineId + "', '" + engineType + "', '" + engineCost + "', " + global + ")";
		securityDB.insertData(query);
		securityDB.commit();
	}
	
	/**
	 * Add an insight into the security db
	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 */
	public void addInsight(String engineId, String insightId, String insightName, boolean global) {
		String query = "INSERT INTO INSIGHT (ENGINEID, INSIGHTID, INSIGHTNAME, GLOBAL) "
				+ "VALUES ('" + engineId + "', '" + insightId + "', '" + insightName + "', " + global + ")";
		securityDB.insertData(query);
		securityDB.commit();
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying data
	 */
	
	/**
	 * Does the engine table contain the engine
	 * @param engineId
	 * @return
	 */
	public boolean containsEngine(String engineId) {
		String query = "SELECT ID FROM ENGINE WHERE ID='" + engineId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		try {
			if(wrapper.hasNext()) {
				return true;
			} else {
				return false;
			}
		} finally {
			wrapper.cleanUp();
		}
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public List<Map<String, String>> getUserDatabaseList(String userId) {
		String query = "SELECT ID, NAME, TYPE, COST FROM ENGINE, ENGINEPERMISSION WHERE "
				+ "ENGINE=ID AND (ENGINEPERMISSION.USER='" + userId + "' OR ENGINE.GLOBAL=TRUE) ORDER BY NAME";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get user engines + global engines 
	 * @param userId
	 * @return
	 */
	public List<String> getUserEngines(String userId) {
		String query = "SELECT ENGINE FROM ENGINEPERMISSION WHERE USER='" + userId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		List<String> engineList = flushToListString(wrapper);
		engineList.addAll(getGlobalEngineIds());
		return engineList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get global engines
	 * @return
	 */
	public Set<String> getGlobalEngineIds() {
		String query = "SELECT ID FROM ENGINE WHERE GLOBAL=TRUE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		return flushToSetString(wrapper, false);
	}

	/**
	 * Get user insights + global insights in engine
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public List<String> getUserInsightsForEngine(String userId, String engineId) {
		String query = "SELECT INSIGHTID FROM USERINSIGHTPERMISSION WHERE ENGINEID='" + engineId + "' AND USER='" + userId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		List<String> insightList = flushToListString(wrapper);
		insightList.addAll(getGlobalInsightIdsForEngine(engineId));
		return insightList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	public Set<String> getGlobalInsightIdsForEngine(String engineId) {
		String query = "SELECT INSIGHTID FROM INSIGHT WHERE ENGINEID='" + engineId + "' AND GLOBAL=TRUE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		return flushToSetString(wrapper, false);
	}
	
	/*
	 * Utility methods
	 */
	
	/**
	 * Utility method to flush result set into list
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	private List<String> flushToListString(IRawSelectWrapper wrapper) {
		List<String> values = new Vector<String>();
		while(wrapper.hasNext()) {
			values.add(wrapper.next().getValues()[0].toString());
		}
		return values;
	}
	
	/**
	 * Utility method to flush result set into set
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	private Set<String> flushToSetString(IRawSelectWrapper wrapper, boolean order) {
		Set<String> values = null;
		if(order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		while(wrapper.hasNext()) {
			values.add(wrapper.next().getValues()[0].toString());
		}
		return values;
	}
	
	private List<Map<String, String>> flushRsToMap(IRawSelectWrapper wrapper) {
		List<Map<String, String>> result = new Vector<Map<String, String>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			Map<String, String> map = new HashMap<String, String>();
			for(int i = 0; i < headers.length; i++) {
				map.put(headers[i], values[i].toString());
			}
			result.add(map);
		}
		return result;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Exposing the ability to get an insert prepared statement for improved performance
	 * @param args			Object[] where the first index is the table name
	 * 						and every other entry are the column names
	 * @return				PreparedStatement to perform a bulk insert
	 */
	public java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) {
		return this.securityDB.bulkInsertPreparedStatement(args);
	}	
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Check if a user (user name or email) exist in the security database
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public boolean checkUserExist(String username, String email){
		String query = "SELECT * FROM USER WHERE USERNAME = '?1' OR EMAIL = '?2'";
		query = query.replace("?1", username);
		query = query.replace("?2", email);
		
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		return sjsw.hasNext();
	}
	
	/**
	 * Verifies user information provided in the log in screen to allow or not 
	 * the entry in the application.
	 * @param user user name
	 * @param password
	 * @return true if user exist and password is correct otherwise false.
	 */
	public boolean logIn(String user, String password){
		StringMap<String> databaseUser = getUserFromDatabase(user);
		if(!databaseUser.isEmpty()){
			String typedHash = hash(password, databaseUser.get("SALT"));
			return databaseUser.get("PASSWORD").equals(typedHash);
		} else {
			 return false;
		}
	}
	
	/**
	 * Brings all the user basic information from the database.
	 * @param username 
	 * @return User retrieved from the database otherwise null.
	 */
	private StringMap<String> getUserFromDatabase(String username) {
		 StringMap<String> user = new StringMap<>();
		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT FROM USER WHERE USERNAME = '?1'";
		query = query.replace("?1", username);
		
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		String[] names = sjsw.getDisplayVariables();
		if(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			
			user.put(names[0], sjss.getVar(names[0]).toString());
			user.put(names[1], sjss.getVar(names[1]).toString());
			user.put(names[2], sjss.getVar(names[2]).toString());
			user.put(names[3], sjss.getVar(names[3]).toString());
			user.put(names[4], sjss.getVar(names[4]).toString());
			user.put(names[5], sjss.getVar(names[5]).toString());
			user.put(names[6], sjss.getVar(names[6]).toString());
			user.put(names[7], sjss.getVar(names[7]).toString());
			
			return user;
		}
		return user;
	}
	
	/**
	 * Current salt generation by BCrypt
	 * @return salt
	 */
	private String generateSalt(){
		return BCrypt.gensalt();
	}
	
	/**
	 * Create the password hash based on the password and salt provided.
	 * @param password
	 * @param salt
	 * @return hash
	 */
	private String hash(String password, String salt) {
        return BCrypt.hashpw(password, salt);
    }
	
	/**
	 * Check if the user is an admin
	 * 
	 * @param userId	String representing the id of the user to check
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
	public Boolean addEngineAndOwner(String engineID, String engineName, String userId) {
		//Add the engine to the ENGINE table
		//String engineID = UUID.randomUUID().toString();
		String query = "INSERT INTO Engine(NAME, ID) VALUES ('" + engineName + "','" + engineID + "');";
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
		query = "INSERT INTO EnginePermission (ID, USER, PERMISSION, ENGINE) VALUES (NULL, '" + userId + "', " + EnginePermission.OWNER.getId() + ", '" + engineID + "');";
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
	
	
	public ArrayList<StringMap<String>> getUserDatabases(String userId, boolean isAdmin){
		ArrayList<StringMap<String>> ret = new ArrayList<>();
		
		if(isAdmin && !isUserAdmin(userId)){
			throw new IllegalArgumentException("The user isn't an admin");
		}
		
		String query = "";
		ArrayList<String[]> engines = new ArrayList<>();
		
		if(!isAdmin){
			query = "SELECT e.id AS DB_ID, e.name AS DB_NAME, e.public AS PUBLIC, ep.permission AS DB_PERMISSION, ep.visibility AS VISIBILITY "
					+ "FROM ENGINEPERMISSION ep JOIN ENGINE e ON(ep.engine = e.id)  "
					+ "WHERE ep.user = '?1'";
			query = query.replace("?1", userId);
			
			engines = runQuery(query);
				  		
			query = "SELECT ge.engine AS DB_ID, e.name AS DB_NAME, e.public AS PUBLIC, ge.permission AS DB_PERMISSION, v.visibility AS VISIBILITY "
					+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION ge ON (ge.groupid = gm.groupid) JOIN ENGINE e ON (ge.engine = e.id) "
					+ "JOIN ENGINEGROUPMEMBERVISIBILITY v ON(gm.id = v.groupmembersid AND ge.id = groupenginepermissionid)  "
					+ "WHERE gm.memberid = '?1'";
			query = query.replace("?1", userId);
			engines.addAll(runQuery(query));
			
			engines.addAll(getPublicEngines(engines, null));
		} else {
			query = "SELECT e.id AS DB_ID, e.name AS DB_NAME, e.public AS PUBLIC "
					+ "FROM ENGINE e "
					+ "WHERE e.ID != '1'";
			
			engines = runQuery(query);	
		}
		
		for(String[] engine : engines) {
			StringMap<String> dbProp = new StringMap<>();
			
			dbProp.put("db_id", engine[0]);
			dbProp.put("db_name", engine[1]);
			dbProp.put("db_public", engine[2]);
			if(!isAdmin){
				if(engine[3] == null){
					dbProp.put("db_permission", EnginePermission.EDIT.getPermission());
				} else {
					dbProp.put("db_permission", EnginePermission.getPermissionValueById(engine[3]));
				}
				dbProp.put("db_visibility", engine[4]);
			}
			ret.add(dbProp);
		}
		return ret;
	}
	
	/**
	 * Get only the public engines that the user don't have a straight relationship established 
	 * @param engines other engines the user is related to
	 * @return public engines
	 */
	private List<String[]> getPublicEngines(ArrayList<String[]> engines, String userId) {
		
		String query = "";
		ArrayList<String> allEngines = new ArrayList<>();
		for(String[] engine : engines){
			allEngines.add(engine[0]);
		}
		
		if(userId != null){
			
			query = "SELECT e.ID FROM ENGINEPERMISSION ep JOIN ENGINE e ON (e.id = ep.engine) WHERE ep.user = '?1' AND ep.visibility = FALSE AND e.public =TRUE";
			query = query.replace("?1", userId);
			
			List<Object[]> publicHiddenEngines = runQuery2(query);
			
			query = "SELECT ENGINE_ID FROM (SELECT e.ID AS ENGINE_ID, gep.ID AS ID1, temp.AID AS ID2 FROM GROUPENGINEPERMISSION gep JOIN ENGINE e ON (gep.engine = e.id) JOIN (SELECT ID AS AID, GROUPID FROM GROUPMEMBERS WHERE MEMBERID = '?1') temp ON (gep.groupid = temp.GROUPID) WHERE ENGINE = '?1' AND e.PUBLIC = TRUE)  b JOIN ENGINEGROUPMEMBERVISIBILITY v ON (b.id1 = v.GROUPENGINEPERMISSIONID AND b.id2 = v.GROUPMEMBERSID) WHERE v.visibility = FALSE;";
			query = query.replace("?1", userId);
			publicHiddenEngines.addAll(runQuery2(query));
			
			for(Object[] p : publicHiddenEngines){
				String[] engine = Arrays.stream(p).map(Object::toString).
		                   toArray(String[]::new);
				allEngines.add(engine[0]);
			}
			
		}
		
		//TAP_Site_Data
		allEngines.add("1");
		query = "";
		
		query = "SELECT e.id AS ID, e.name AS NAME, e.public AS PUBLIC, 'Edit' AS PERMISSIONS, 'true' AS VISIBILITY FROM ENGINE e WHERE e.id NOT IN ?1 AND PUBLIC = TRUE";	
		
		query = query.replace("?1", "(" + convertArrayToDbString(allEngines, true) + ")");
		
		List<Object[]> res = runQuery2(query);
		List<String[]> resPrimitive = new ArrayList<>();
		for(Object[] r : res){
			String[] engine = Arrays.stream(r).map(Object::toString).
	                   toArray(String[]::new);
			resPrimitive.add(engine);
		}
		return resPrimitive;
	}

	public StringMap<ArrayList<StringMap<String>>> getDatabaseUsersAndGroups(String userId, String engineId, boolean isAdmin){
		
		StringMap<ArrayList<StringMap<String>>> ret = new StringMap<>();
		ret.put("groups", new ArrayList<>());
		ret.put("users", new ArrayList<>());
		
		if(isAdmin && !isUserAdmin(userId)){
			throw new IllegalArgumentException("This user is not an admin. ");
		}
		
		//TODO add check if user can access this engine as owner
		
		String query = "SELECT u.id as ID, u.name as NAME, ge.permission as PERMISSION "
				+ "FROM ENGINEPERMISSION ge JOIN User u ON (ge.user = u.id) "
				+ "WHERE ge.engine = '?1'";
		query = query.replace("?1", engineId);
		
		ArrayList<String[]> users = runQuery(query);
		
		for(String[] user : users) {
			StringMap<String> userInfo = new StringMap<>();
			
			userInfo.put("id", user[0]);
			userInfo.put("name", user[1]);
			userInfo.put("permission", EnginePermission.getPermissionValueById(user[2]));	
			
			ret.get("users").add(userInfo);
		}
		
		String groupQuery = "SELECT ug.id AS ID, ug.name as NAME, ge.permission AS PERMISSION "
				+ "FROM GROUPENGINEPERMISSION ge JOIN USERGROUP ug ON (ge.groupid = ug.id) "
				+ "WHERE ge.engine = '?1'";
		groupQuery = groupQuery.replace("?1", engineId);
		
		ArrayList<String[]> groups = runQuery(groupQuery);
		
		for(String[] group : groups) {
			StringMap<String> userInfo = new StringMap<>();
			
			userInfo.put("id", group[0]);
			userInfo.put("name", group[1]);
			userInfo.put("permission", EnginePermission.getPermissionValueById(group[2]));	
			
			ret.get("groups").add(userInfo);
		}
		
		return ret;
	}
	
	
	private void getGroupsWithoutMembers(ArrayList<HashMap<String, Object>> ret, String query, String userId){
		ArrayList<String[]> groupsWithoutMembers = runQuery(query);
		
		for(String[] groupsWithoutMember : groupsWithoutMembers) {
			String groupId = groupsWithoutMember[0];
			String groupName = groupsWithoutMember[1];
			
			HashMap<String, Object> groupObject = new HashMap<>();
			groupObject.put("group_id", groupId);
			groupObject.put("group_name", groupName);
			groupObject.put("group_users", new ArrayList<StringMap<String>>());
			
			ret.add(groupObject);
		}
	}
	
	private int indexGroup(ArrayList<HashMap<String, Object>> ret, String groupId){
		for (int i = 0; i < ret.size(); i++) {
			if(ret.get(i).get("group_id").equals(groupId)){
				return i;
			}
		}
		return -1;
	}
	
	private void getGroupsAndMembers(ArrayList<HashMap<String, Object>> ret, String query, String userId){
		ArrayList<String[]> groupsAndMembers = runQuery(query);
		
		for(String[] groupAndMember : groupsAndMembers) {
			String groupId = groupAndMember[0];
			String groupName = groupAndMember[1];
			
			StringMap<ArrayList<StringMap<String>>> groupUsersObject = new StringMap<>();
			
			StringMap<String> user = new StringMap<String>();
			user.put("id", groupAndMember[2]);
			user.put("name", groupAndMember[3]);
			user.put("email", groupAndMember[4]);
			
			int indexGroup = indexGroup(ret, groupId);
			
			if(indexGroup == -1) {
				ArrayList<StringMap<String>> newGroup = new ArrayList<StringMap<String>>();
				newGroup.add(user);
				HashMap<String, Object> groupObject = new HashMap<>();
				groupObject.put("group_id", groupId);
				groupObject.put("group_name", groupName);
				groupObject.put("group_users", newGroup);
				
				ret.add(groupObject);
			} else {
				ArrayList<StringMap<String>> updateGroup = (ArrayList<StringMap<String>>) ret.get(indexGroup).get("group_users");
				updateGroup.add(user);
			}
		}
	}
	
	/**
	 * Get all Groups and list of user for each group,
	 * if the user is admin returns all the groups.
	 * @param userId
	 * @return all groups and list of users for each group
	 */
	public ArrayList<HashMap<String, Object>> getGroupsAndMembersForUser(String userId) {
		
		ArrayList<HashMap<String, Object>> ret = new ArrayList<>();
		
//		if(isUserAdmin(userId)){
//			String query = "SELECT ug.ID AS GROUP_ID, ug.NAME AS GROUP_NAME FROM USERGROUP ug LEFT JOIN GROUPMEMBERS gm ON(ug.ID = gm.GROUPID) WHERE GROUPID IS NULL";
//			getGroupsWithoutMembers(ret, query, userId);
//			
//			query = "SELECT ug.ID AS GROUP_ID, ug.NAME AS GroupName, u.ID AS MEMBER_ID, u.NAME AS MEMBERNAME, u.EMAIL AS EMAIL FROM UserGroup ug JOIN GroupMembers gm ON(gm.groupid = ug.id) JOIN User u ON(gm.memberid = u.id)";
//			getGroupsAndMembers(ret, query, userId);
//		} else {
		String query = "SELECT ug.ID AS GROUP_ID, ug.NAME AS GROUP_NAME FROM USERGROUP ug LEFT JOIN GROUPMEMBERS gm ON(ug.ID = gm.GROUPID) WHERE GROUPID IS NULL AND ug.owner = '?1'";
		query = query.replace("?1", userId);
		getGroupsWithoutMembers(ret, query, userId);
		
		query = "SELECT ug.ID AS GROUP_ID, ug.NAME AS GroupName, u.ID AS MEMBER_ID, u.NAME AS MEMBERNAME, u.EMAIL AS EMAIL FROM UserGroup ug JOIN GroupMembers gm ON(gm.groupid = ug.id) JOIN User u ON(gm.memberid = u.id) WHERE ug.owner = '?1'";
		query = query.replace("?1", userId);
		getGroupsAndMembers(ret, query, userId);
//		}
		
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
		String query = "SELECT DISTINCT User.ID AS ID, User.NAME AS NAME, User.EMAIL AS EMAIL FROM User WHERE UPPER(User.NAME) LIKE UPPER('%" + searchTerm + "%') OR UPPER(User.EMAIL) LIKE UPPER('%" + searchTerm + "%') AND TYPE != 'anonymous';";
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
	
	/**
	 * Get all engines associated with the userId and the permission that the user has for the engine,
	 * @param userId
	 * @return List of "EngineName, UserName and Permission" for the specific user.
	 */
	public ArrayList<StringMap<String>> getAllEnginesAndPermissionsForUser(String userId) {
		ArrayList<String[]> ret = new ArrayList<String[]>();
		
		String query = "SELECT e.NAME AS ENGINENAME, u.NAME AS OWNER, p.NAME AS PERMISSION "
					 + "FROM Engine e JOIN EnginePermission ep ON (e.id = ep.engine) "
					 			   + "JOIN User u ON(ep.user = u.id) "
					 			   + "JOIN Permission p ON(ep.permission = p.id) "
	 			     + "WHERE ep.user='?1'";
		query = query.replace("?1", userId);
		
		ret = runQuery(query);
		
		query = "SELECT DISTINCT e.NAME AS ENGINENAME, u.NAME AS OWNER, p.NAME AS PERMISSIONNAME "
			  + "FROM Engine e JOIN GroupEnginePermission gep ON (e.id =gep.engine) "
							+ "JOIN GroupMembers gm ON(gep.groupid = gm.groupid) "
							+ "JOIN User u ON(gm.memberid = u.id) "
							+ "JOIN Permission p ON(gep.permission = p.id) "
			  + "WHERE u.ID='?1'";
		query = query.replace("?1", userId);
		
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
			query = "INSERT INTO GroupMembers(ID, GROUPID, MEMBERID) VALUES (NULL, " + id + ", '" + user + "');";
			securityDB.insertData(query);
		}
		
		securityDB.commit();
		
		//ADD VISIBILITY
		query = "INSERT INTO ENGINEGROUPMEMBERVISIBILITY (ID, GROUPENGINEPERMISSIONID, GROUPMEMBERSID, VISIBILITY) "
				+ "SELECT NULL AS ID, gep.ID AS GROUPENGINEPERMISSIONID, gm.ID ASGROUPMEMBERSID, TRUE AS VISIBILITY "
				+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION gep ON(gm.groupid = gep.groupid) "
				+ "WHERE gm.groupid = ?1 AND gm.memberid IN ?2 ";
		query = query.replace("?1", id + "");
		query = query.replace("?2", "(" + convertArrayToDbString(users, true) +")");
		securityDB.insertData(query);
		
		securityDB.commit();
		
		return true;
	}
	
	public Boolean removeGroup(String userId, String groupId) {
		
		String query;
		
		if(isUserAdmin(userId)){
			query = "DELETE FROM GroupEnginePermission WHERE GroupEnginePermission.GROUPID IN (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.ID='" + groupId + "'); ";
			query += "DELETE FROM GroupMembers WHERE GroupMembers.GROUPID IN (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.ID='" + groupId + "'); ";
			query += "DELETE FROM UserGroup WHERE UserGroup.ID='" + groupId + "';";
		} else {
			query = "DELETE FROM GroupEnginePermission WHERE GroupEnginePermission.GROUPID IN (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.ID='" + groupId + "' AND UserGroup.OWNER='" + userId + "'); ";
			query += "DELETE FROM GroupMembers WHERE GroupMembers.GROUPID IN (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.ID='" + groupId + "' AND UserGroup.OWNER='" + userId + "'); ";
			query += "DELETE FROM UserGroup WHERE UserGroup.ID='" + groupId + "' AND UserGroup.OWNER='" + userId + "';";
		}
		
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		return true;
	}
	
	public Boolean addUserToGroup(String userId, String groupId, String userIdToAdd) {
		String query;
		
		if(isUserAdmin(userId)){
			query = "INSERT INTO GroupMembers (ID, GROUPID, MEMBERID) VALUES (NULL, (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.ID='" + groupId + "'), '" + userIdToAdd + "');";
		} else {
			query = "INSERT INTO GroupMembers (ID, GROUPID, MEMBERID) VALUES (NULL, (SELECT UserGroup.ID FROM UserGroup WHERE UserGroup.ID='" + groupId + "' AND UserGroup.OWNER='" + userId + "'), '" + userIdToAdd + "');";
		}
		
		securityDB.insertData(query);
		securityDB.commit();
		
		//ADD VISIBILITY
		query = "INSERT INTO ENGINEGROUPMEMBERVISIBILITY (ID, GROUPENGINEPERMISSIONID, GROUPMEMBERSID, VISIBILITY) "
				+ "SELECT NULL AS ID, gep.ID AS GROUPENGINEPERMISSIONID, gm.ID ASGROUPMEMBERSID, TRUE AS VISIBILITY "
				+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION gep ON(gm.groupid = gep.groupid) "
				+ "WHERE gm.groupid = '?1' AND gm.memberid = '?2'";
		query = query.replace("?1", groupId);
		query = query.replace("?2", userIdToAdd);
		
		securityDB.insertData(query);
		securityDB.commit();
		
		return true;
	}
	
	private String convertArrayToDbString(ArrayList<String> list, boolean stringList){
		String listString = "";
		String quotes = "'";
		if(!stringList){
			quotes = "";	
		}
		for(String groupId : list){
			if(listString.isEmpty())
				listString += quotes + groupId + quotes;
			else 
				listString += ", " + quotes + groupId + quotes;
		}
		return listString;
	}
	
	public String isUserWithDatabasePermissionAlready(String userId, ArrayList<String> groupsId, ArrayList<String> usersId){
		String ret = "";
		
		String username = getUsernameByUserId(userId);
		
		if(usersId.contains(userId)){
			ret += "The user " + username + " already has a direct relationship with the database. ";
		}
		
		String query = "SELECT gm.groupid AS ID, ug.name AS NAME, u.name AS OWNER FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (gm.groupid = ug.id) JOIN User u ON(ug.owner = u.id) WHERE gm.groupid IN ?1 AND gm.memberid = '?2'";	
		query = query.replace("?1", "(" + convertArrayToDbString(groupsId, true) + ")");
		query = query.replace("?2", userId);
		
		ArrayList<String[]> result = runQuery(query);
		
		for(String[] row : result){
			String groupId = row[0];
			String groupName = row[1];
			String groupOwner = row[2];
			
			ret += "The user " + username + " is in " + groupName + " owned by " + groupOwner + ". "; 
			
		}
		
		return ret.isEmpty() ? "true" : ret;
	}
	
	private String getUsernameByUserId(String userId) {
		// TODO Auto-generated method stub
		String query = "SELECT NAME FROM USER WHERE ID = '?1'";
		query = query.replace("?1", userId);
		
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		String[] names = sjsw.getDisplayVariables();
		if(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			return sjss.getVar(names[0]).toString();
		}
		return null;
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
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * 
	 * @param query		Query to be executed to retrieve engine names
	 * @return			List of engine names
	 */
	private ArrayList<StringMap<String>> getSimpleQuery(String query) {
		System.out.println("Executing security query: " + query);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDB, query);
		ArrayList<StringMap<String>> ret = new ArrayList<>();
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			Object[] headers = row.getHeaders();
			Object[] values = row.getValues();
			StringMap<String> rowData = new StringMap<>();
			for(int idx = 0; idx < headers.length; idx++){
				if(headers[idx].toString().toLowerCase().equals("type") && values[idx].toString().equals("NATIVE")){
					rowData.put(headers[idx].toString().toLowerCase(), "Default");
				} else {
					rowData.put(headers[idx].toString().toLowerCase(), values[idx].toString());
				}
			}
			ret.add(rowData);
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

	/**
	 * Brings the user id from database.
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public String getUserId(String username) {
		// TODO Auto-generated method stub
		String query = "SELECT ID FROM USER WHERE USERNAME = '?1'";
		query = query.replace("?1", username);
		
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		String[] names = sjsw.getDisplayVariables();
		if(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			return sjss.getVar(names[0]).toString();
		}
		return null;
		
	}
	
	/**
	 * Brings the user name from database.
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public String getNameUser(String username) {
		// TODO Auto-generated method stub
		String query = "SELECT NAME FROM USER WHERE USERNAME = '?1'";
		query = query.replace("?1", username);
		
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		String[] names = sjsw.getDisplayVariables();
		if(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			return sjss.getVar(names[0]).toString();
		}
		return null;
		
	}
	
	/**
	 * Check if user is allowed to use a certain database (owner/edit permissions).
	 * @param userId
	 * @param engineName
	 * @return 
	 */
	public boolean isUserAllowedToUseEngine(String userId, String engineId){
		/*if(isUserAdmin(userId)){
			return true;
		}*/
		
		String query = "SELECT e.ID FROM ENGINE e WHERE e.name = '?3' AND e.public = TRUE ";
		List<Object[]> res = runQuery2(query);
		if(!res.isEmpty()){
			return true;
		}
		
		query = "SELECT e.ID FROM ENGINEPERMISSION ep "
				+ "JOIN ENGINE e ON(ep.engine = e.id) WHERE USER = '?1' AND e.id = '?2' AND (ep.permission = '1'  OR ep.permission = '3') "
				+ "UNION "
				+ "SELECT gep.engine "
				+ "FROM GROUPENGINEPERMISSION gep JOIN GROUPMEMBERS gm ON (gep.groupid = gm.groupid) JOIN engine e ON (e.id = gep.engine) "
				+ "WHERE e.id = '?2'  AND gm.memberid = '?1' AND (gep.PERMISSION = '1' OR  gep.PERMISSION = '3')";
		
		query = query.replace("?1", userId);
		query = query.replace("?2", engineId);
		
		res = runQuery2(query);
		return !res.isEmpty();
	}
	
	/**
	 * Get a list of the users o a certain group.
	 * @param groupId
	 * @return list of users.
	 */
	public ArrayList<String[]> getAllUserFromGroup(String groupId){
		String query = 	"SELECT gm.groupid AS GROUP_ID, ug.name AS GROUP_NAME, ug.owner AS GROUP_OWNER, gm.memberid AS USER_ID, u.name AS USER_NAME  FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (ug.id = gm.groupid) JOIN USER u ON (u.id = gm.memberid) WHERE gm.groupid = ?1";
		query = query.replace("?1", groupId);
		return runQuery(query);
	}
	
	/**
	 * Get a list of all the users of a list of groups
	 * @param groups
	 * @return list of users.
	 */
	public StringMap<ArrayList<String[]>> getAllUserFromGroups(ArrayList<String> groups){
		String query = 	"SELECT gm.groupid AS GROUP_ID, ug.name AS GROUP_NAME, ug.owner AS GROUP_OWNER, gm.memberid AS USER_ID, u.name AS USER_NAME  FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (ug.id = gm.groupid) JOIN USER u ON (u.id = gm.memberid) WHERE gm.groupid IN ?1";
		query = query.replace("?1", "(" + convertArrayToDbString(groups, false) + ")");
		List<Object[]> users = runQuery2(query);
		StringMap<ArrayList<String[]>> ret = new StringMap<>();
		for(Object[] userObject : users){
			String[] user = Arrays.stream(userObject).map(Object::toString).
	                   toArray(String[]::new);
			String userId = user[3];
			if(ret.get(userId) == null){
				ArrayList<String[]> userNodeList = new ArrayList<>();
				userNodeList.add(user);
				ret.put(userId, userNodeList);
			} else {
				ret.get(userId).add(user);
			}
		}
		return ret;
	}
	
	/**
	 * Check if the users from a group already have access to the database through another group.  
	 * @param usersFromGroup
	 * @param otherGroupsUserList
	 * @return
	 */
	private String isGroupWithInvalidUsersFromOtherGroups(ArrayList<String[]> usersFromGroup, StringMap<ArrayList<String[]>> otherGroupsUserList){
		String ret = "";
		for(String[] user : usersFromGroup){
			String userId = user[3];
			if(otherGroupsUserList.get(userId) != null){
				ArrayList<String[]> otherCoincidences = otherGroupsUserList.get(userId);
				for(String[] coincidence : otherCoincidences){
					String groupName = coincidence[1];
					String groupOwner = getUsernameByUserId(coincidence[2]);
					ret += "The user " + user[4] + " in " + user[1] + " already has access to the database through " + groupName + " owned by " + groupOwner + ". ";
				}
			}
		}
		return ret;
	}
	
	/**
	 * Check if the user from a group have already direct access to a database. 
	 * Checking over the parameter usersList
	 * @param usersFromGroup
	 * @param users
	 * @return
	 */
	private String isGroupWithInvalidUsersFromList(ArrayList<String[]> usersFromGroup, ArrayList<String> users){
		String ret = "";
		for(String[] user : usersFromGroup){
			if(users.contains(user[3])){
				ret += "The user " + user[4] + " in the group " + user[1] + " already has direct access to the database. ";
			}
		}
		return ret;
	}
	
	/**
	 * Check if the group to be added has already permissions to a database.
	 * @param groupId
	 * @param groups
	 * @param users
	 * @return
	 */
	public String isGroupUsersWithDatabasePermissionAlready(String groupId, ArrayList<String> groups,
			ArrayList<String> users) {
		
		String ret = "";
		// TODO Security check if the user logged in is the owner of the group being added (?)
		
		ArrayList<String[]> allUserFromGroup = getAllUserFromGroup(groupId);
		ret += isGroupWithInvalidUsersFromList(allUserFromGroup, users);
		StringMap<ArrayList<String[]>> allUsersFromOtherGroups = getAllUserFromGroups(groups);
		ret += isGroupWithInvalidUsersFromOtherGroups(allUserFromGroup, allUsersFromOtherGroups);
		
		return ret.isEmpty() ? "true" : ret;
	}
	
	/**
	 * Get all groups associated with a database. To that list adds groupsToAdd and remove groupsToRemove 
	 * @param engineId
	 * @param groupsToAdd
	 * @param groupsToRemove
	 * @return list of groups id.
	 */
	public ArrayList<String> getAllDbGroupsById(String engineId, ArrayList<String> groupsToAdd,
			ArrayList<String> groupsToRemove) {
		
		ArrayList<String> ret = new ArrayList<>();
		String query = "SELECT GROUPENGINEPERMISSION.GROUPID FROM GROUPENGINEPERMISSION WHERE GROUPENGINEPERMISSION.ENGINE = '?1'";
		query = query.replace("?1", engineId);
		ArrayList<String[]> rows = runQuery(query);
		
		for(String[] row : rows){
			if(row[0] != null && !row[0].isEmpty())
                ret.add(row[0]);
		}
		
		ret.removeAll(groupsToRemove);
		ret.addAll(groupsToAdd);
		return ret;
	}

	/***
	 * Get all users that have a direct relationship with a database to that list adds userToAdd and 
	 * removes usersToRemove
	 * @param engineId
	 * @param usersToAdd
	 * @param usersToRemove
	 * @return list of users id.
	 */
	public ArrayList<String> getAllDbUsersById(String engineId, ArrayList<String> usersToAdd,
			ArrayList<String> usersToRemove) {
		
		ArrayList<String> ret = new ArrayList<>();
		String query = "SELECT ENGINEPERMISSION.USER FROM ENGINEPERMISSION WHERE ENGINEPERMISSION.ENGINE = '?1'";
		query = query.replace("?1", engineId);
		ArrayList<String[]> rows = runQuery(query);
		
		for(String[] row : rows){
			ret.add(row[0]);
		}
		ret.removeAll(usersToRemove);
		ret.addAll(usersToAdd);
		return ret;
	}
	
	/**
	 * Check if the user is already associated from another group 
	 * to the databases that the group he wants to be part of is associated. 
	 * @param userId
	 * @param groupId
	 * @return
	 */
	public String isUserInAnotherDbGroup(String userId, String groupId) {
		
		String ret = "";
		String query = "SELECT gm.groupid as GROUPID, ug.name as GROUPNAME, u.name as OWNER, gr.enginename AS ENGINENAME "
				+ "FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (ug.id = gm.groupid) JOIN USER u ON(u.id = ug.owner) JOIN "
				+ "(SELECT ge.groupid as GROUPID, en.name AS ENGINENAME FROM GROUPENGINEPERMISSION ge JOIN "
				+ "(SELECT ge.engine AS ENGINE, e.name AS NAME "
				+ "FROM GROUPENGINEPERMISSION ge JOIN ENGINE e ON(ge.engine = e.id) WHERE ge.groupid = '?2') en ON (ge.engine = en.engine) WHERE ge.groupid != '?2') gr ON (gm.groupid = gr.groupid) "
				+ "WHERE gm.memberid = '?1'";
		
		query = query.replace("?1", userId);
		query = query.replace("?2", groupId);
		
		List<Object[]> rows = runQuery2(query);
		
		for(Object[] row : rows){
			String groupName = row[1].toString();
			String ownerName = row[2].toString();
			String engineName = row[3].toString();
			ret += "The user is already associated with the database " + engineName + " in the group " + groupName + " owned by " + ownerName + ". ";
		}
		
		return ret;
	}
	
	/**
	 * Check if a user already have a relationship with a database that the group already have access.
	 * @param userId
	 * @param groupId
	 * @return blank if there was no relationships otherwise a message explaining other databases associated with the user.
	 */
	public String isUserWithAccessToGroupDb(String userId, String groupId) {
		
		String ret = "";
		String query = "SELECT en.engine AS ENGINE, en.name AS ENGINENAME "
				+ "FROM ENGINEPERMISSION ep JOIN (SELECT ge.engine AS ENGINE, e.name AS NAME FROM GROUPENGINEPERMISSION ge JOIN ENGINE e ON(ge.engine = e.id) WHERE ge.groupid = '?1') en ON (ep.engine = en.engine) "
				+ "WHERE ep.user = '?2'";
		
		query = query.replace("?1", groupId);
		query = query.replace("?2", userId);
		
		List<Object[]> rows = runQuery2(query);
		
		for(Object[] row : rows){
			String engineName = row[1].toString();
			ret += "The user is already associated with the database " + engineName + ". ";
		}
		
		return ret;
	}
	
	/**
	 * Check if a user can be added or not to a group based on its database
	 * permission on another groups and the database permissions from the group
	 * it's going ot be added.
	 * @param userId
	 * @param groupId
	 * @return true if user can be added otherwise a message explainig why not.
	 */
	public String isUserAddedToGroupValid(String userId, String groupId){
		String ret = "";
		
		ret += isUserWithAccessToGroupDb(userId, groupId);
		ret += isUserInAnotherDbGroup(userId, groupId);
		
		return ret.isEmpty() ? "true" : ret;
	}
	
	/**
	 * Get all database users who aren't "Anonymous" type
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public ArrayList<StringMap<String>> getAllDbUsers(String userId) throws IllegalArgumentException{
		ArrayList<StringMap<String>> ret = new ArrayList<>();  
		if(isUserAdmin(userId)){
			String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM USER WHERE TYPE != 'anonymous'";
			ret = getSimpleQuery(query);
		} else {
			throw new IllegalArgumentException("The user can't access to this resource. ");
		}
		return ret;
	}
	
	/**
	 * Remove all permission a user has over a database.
	 * @param userRemove
	 * @param engineId
	 * @return true if action was performed otherwise false
	 */
	public boolean removeUserPermissionsbyDbId(String userRemove, String engineId){
		String query = "DELETE FROM ENGINEPERMISSION WHERE ENGINE = '?2' AND USER = '?1' AND PERMISSION != 1; "
				+ "DELETE FROM GROUPMEMBERS WHERE  GROUPID = (SELECT TOP 1 ge.groupid AS GROUPID FROM GROUPMEMBERS gm "
				+ "JOIN GROUPENGINEPERMISSION ge ON (gm.groupid = ge.groupid) WHERE gm.memberid = '?1' "
				+ "AND ge.engine = '?2' AND ge.PERMISSION != 1) AND MEMBERID = '?1'";
		
		query = query.replace("?1", userRemove);
		query = query.replace("?2", engineId);
		System.out.println("Executing security query: " + query);
		if(securityDB.execUpdateAndRetrieveStatement(query, true) != null){
			securityDB.commit();
		} else {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Update user information.
	 * @param adminId
	 * @param userInfo
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean editUser(String adminId, StringMap<String> userInfo) throws IllegalArgumentException{
        boolean first = true;
        String error = "";
        String userId = userInfo.remove("id");
        if(userId.equals(adminId) || isUserAdmin(adminId)){
        	String name = userInfo.get("name") != null ? userInfo.get("name") : "";
        	String email = userInfo.get("email") != null ? userInfo.get("email") : "";
            if(checkUserExist(name, email)){
                throw new IllegalArgumentException("The user name or email already exist.");
            }
            String password = userInfo.get("password");
            if(password != null && !password.isEmpty()){
                error += validPassword(password);
                if(error.isEmpty()){
                    String newSalt = generateSalt();
                    userInfo.put("password", hash(password, newSalt));
                    userInfo.put("salt", newSalt);
                }
            }
            if(email != null && !email.isEmpty()){
                error += validEmail(email);
            }
            if(!error.isEmpty()){
                throw new IllegalArgumentException(error);
            }
            String query = "UPDATE USER ";
            for( Entry<String, String> entry : userInfo.entrySet()){
                String key = entry.getKey();
                String value = entry.getValue();
                if(value != null && !value.isEmpty()){
                    if(first){
                        query += "SET " + key + " = '" + value + "'";
                        first = false;
                    } else {
                        query += ", " + key + " = '" + value + "'";
                    }
                }
            }
            query += " WHERE ID = '" + userId + "'";
            System.out.println("Executing security query: " + query);
            Statement stmt = securityDB.execUpdateAndRetrieveStatement(query, true);
            if(stmt != null){
                securityDB.commit();
            } else {
                throw new IllegalArgumentException("An unexpected error happen. Please try again.");
            }
        } else {
            throw new IllegalArgumentException("User is not allowed to perform this operation");
        }
        return true;
    }
	
	/**
	 * Remove all permissions from a group with a database.
	 * @param groupId
	 * @param engineId
	 * @return
	 */
	public Boolean removeAllPermissionsForGroup(String groupId, String engineId) {
		String query = "DELETE FROM GROUPENGINEPERMISSION WHERE ENGINE = '?1' AND GROUPID = '?2'";
		query = query.replace("?1", engineId);
		query = query.replace("?2", groupId);
		
		System.out.println("Executing security query: " + query);
		if(securityDB.execUpdateAndRetrieveStatement(query, true) != null){
			securityDB.commit();
		} else {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Remove all direct permissions of user with a database. 
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public Boolean removeAllPermissionsForUser(String userId, String engineId) {
		String query = "DELETE FROM ENGINEPERMISSION WHERE ENGINE = '?1' AND USER = '?2'";
		query = query.replace("?1", engineId);
		query = query.replace("?2", userId);
		
		System.out.println("Executing security query: " + query);
		if(securityDB.execUpdateAndRetrieveStatement(query, true) != null){
			securityDB.commit();
		} else {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Remove a user from a group. Only owner of a group can do it. 
	 * @param userId
	 * @param groupId
	 * @param userToRemove
	 * @return
	 */
	public Boolean removeUserFromGroup(String userId, String groupId, String userToRemove) {
        String query;
        
        /*if(isUserAdmin(userId)){
            query = "DELETE FROM GroupMembers WHERE GroupMembers.MEMBERID='" + userToRemove + "' AND GroupMembers.GROUPID = '" + groupId + "';";
        } else {*/
        query = "DELETE FROM GroupMembers WHERE GroupMembers.MEMBERID='" + userToRemove + "' AND GroupMembers.GROUPID IN "
                    + "(SELECT DISTINCT UserGroup.ID AS GROUPID FROM UserGroup WHERE UserGroup.OWNER='" + userId + "' AND UserGroup.ID = '" + groupId + "');";
        //}
        
        securityDB.execUpdateAndRetrieveStatement(query, true);
        securityDB.commit();
        
        return true;
    }
	
	/**
	 * Build the relationship between a group and a database.
	 * @param groupId
	 * @param engineId
	 * @param permission
	 * @return true
	 */
	public Boolean setPermissionsForGroup(String groupId, String engineId, EnginePermission permission) {
		
		String query = "INSERT INTO GROUPENGINEPERMISSION(ID, ENGINE, GROUPID, PERMISSION) VALUES (NULL,'?1', '?2', '?3')";
		query = query.replace("?1", engineId);
		query = query.replace("?2", groupId);
		query = query.replace("?3", permission.getId() + "");
		System.out.println("Executing security query: " + query);
		securityDB.insertData(query);
		securityDB.commit();
		
		//ADD VISIBILITY
		query = "INSERT INTO ENGINEGROUPMEMBERVISIBILITY (ID, GROUPENGINEPERMISSIONID, GROUPMEMBERSID, VISIBILITY)  "
				+ "SELECT NULL AS ID, ep.ID AS GROUPENGINEPERMISSIONID, gm.ID AS GROUPMEMBERS, TRUE AS VISIBILITY "
				+ "FROM GROUPENGINEPERMISSION ep JOIN GROUPMEMBERS gm ON (ep.groupid = gm.groupid) "
				+ "WHERE ep.groupid = '?1' AND ep.engine = '?2'";
		query = query.replace("?1", groupId);
		query = query.replace("?2", engineId);
		System.out.println("Executing security query: " + query);
		securityDB.insertData(query);
		securityDB.commit();
		
		return true;
	}
	
	public Boolean setPermissionsForUser(String engineId, String userToAdd, EnginePermission permission) {
		String query = "INSERT INTO ENGINEPERMISSION (ID, ENGINE, USER, PERMISSION) VALUES (NULL,'?1', '?2', '?3')";
		query = query.replace("?1", engineId);
		query = query.replace("?2", userToAdd);
		query = query.replace("?3", permission.getId() + "");
		System.out.println("Executing security query: " + query);
		securityDB.insertData(query);
		
		securityDB.commit();
		
		return true;
	}
	
	/**
	 * Change the user visibility (show/hide) for a database. Without removing its permissions.
	 * @param userId
	 * @param engineId
	 * @param visibility
	 */
	public void setDbVisibility(String userId, String engineId, String visibility){
		
		String query = "SELECT ID FROM ENGINEPERMISSION WHERE USER = '?1' AND ENGINE = '?2'";
		query = query.replace("?1", userId);
		query = query.replace("?2", engineId);
		ISelectWrapper sjsw = Utility.processQuery(securityDB, query);
		if(sjsw.hasNext()){
			query = "UPDATE ENGINEPERMISSION SET VISIBILITY = '?3' WHERE USER = '?1' AND ENGINE = '?2'";
			query = query.replace("?1", userId);
			query = query.replace("?2", engineId);
			query = query.replace("?3", visibility);
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
			return;
		}
		
		query = "SELECT gep.ID, temp.AID "
				+ "FROM GROUPENGINEPERMISSION gep JOIN (SELECT ID AS AID, GROUPID FROM GROUPMEMBERS WHERE MEMBERID = '?2') temp ON (gep.groupid = temp.GROUPID) "
				+ "WHERE ENGINE = '?1'";
		query = query.replace("?1", engineId);
		query = query.replace("?2", userId);
		
		List<Object[]> rows = runQuery2(query);
		
		for(Object[] row : rows){
			query = "UPDATE ENGINEGROUPMEMBERVISIBILITY SET VISIBILITY = '"+ visibility + 
					"' WHERE GROUPENGINEPERMISSIONID = '"+ row[0].toString() +"' AND GROUPMEMBERSID = '" + row[1].toString() +"'";
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
			return;
		}
		
		Boolean isVisible = Boolean.parseBoolean(visibility);
		
		if(!isVisible){
			query = "INSERT INTO ENGINEPERMISSION (ID, USER, ENGINE) VALUES (NULL, '?1', '?2')";
			query = query.replace("?1", userId);
			query = query.replace("?2", engineId);
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
		} else {
			query = "DELETE FROM ENGINEPERMISSION WHERE USER = '?1' AND ENGINE = '?2' AND PERMISSION IS NULL ";
			query = query.replace("?1", userId);
			query = query.replace("?2", engineId);
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
		}
	}
	
	/**
	 * Remove a database and all the permissions related to it.
	 * @param engineName
	 */
	public void removeDb(String engineId){
				
		//DELETE USERPERMISSIONS
		String query = "DELETE FROM ENGINEPERMISSION WHERE ENGINE = '?1'; DELETE FROM GROUPENGINEPERMISSION WHERE ENGINE = '?1'; DELETE FROM ENGINE WHERE ID = '?1'";
		query = query.replace("?1", engineId);
		
		System.out.println("Executing security query: " + query);
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
	}
	
	/**
	 * Returns a list of all engines that a given user can see (has been given any level of permission).
	 * 
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public HashSet<String> getUserVisibleEngines(String userId) {
		HashSet<String> engines = new HashSet<String>();
		
		//boolean isAdmin = isUserAdmin(userId);
		boolean isAdmin = false;
		
		String query = "SELECT e.id AS ID "
				+ "FROM ENGINEPERMISSION ep JOIN ENGINE e ON(ep.engine = e.id) ";
		
		if(!isAdmin){
			query = "SELECT e.id AS ID "
					+ "FROM ENGINEPERMISSION ep JOIN ENGINE e ON(ep.engine = e.id)  "
					+ "WHERE ep.user = '?1' AND ep.visibility = 'TRUE'";
			query = query.replace("?1", userId);
		}
		
		ArrayList<String[]> dbEngines = runQuery(query);
		
		query = "SELECT e.id AS ID "
			  + "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION ge ON (ge.groupid = gm.groupid) JOIN ENGINE e ON (ge.engine = e.id) ";
			  		
		if(!isAdmin){
			query = "SELECT e.id AS ID "
					+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION ge ON (ge.groupid = gm.groupid) JOIN ENGINE e ON (ge.engine = e.id) "
					+ "JOIN ENGINEGROUPMEMBERVISIBILITY v ON(gm.id = v.groupmembersid AND ge.id = groupenginepermissionid)  "
					+ "WHERE gm.memberid = '?1' AND v.visibility = 'TRUE'";
			query = query.replace("?1", userId);
		}
		
		dbEngines.addAll(runQuery(query));
		
		dbEngines.addAll(getPublicEngines(dbEngines, userId));
		
		for(String[] engine : dbEngines) {

			if(!engines.contains(engine[0])){
				engines.add(engine[0]);
			}
			
		}
		
		return engines;
		
	}
	
	/**
	 * Adds or Remove permission for users and groups to a
	 * certain database.
	 * @param userId
	 * @param isAdmin
	 * @param engineId
	 * @param groups
	 * @param users
	 */
	public void savePermissions(String userId, boolean isAdmin, String engineId, StringMap<ArrayList<StringMap<String>>> groups, StringMap<ArrayList<StringMap<String>>> users){
		
		ArrayList<StringMap<String>> groupsToAdd = groups.get("add");
		ArrayList<StringMap<String>> groupsToRemove = groups.get("remove");
		
		if(isAdmin && !isUserAdmin(userId)){
			throw new IllegalArgumentException("The user doesn't have the permissions to access this resource.");
		}
		
		if(!isAdmin && !isUserDatabaseOwner(userId, engineId)){
			throw new IllegalArgumentException("The user is not an owner of this database.");
		}
		
		for(StringMap<String> map : groupsToRemove) {
			removeAllPermissionsForGroup(map.get("id"), engineId);
		}
		
		for(StringMap<String> map : groupsToAdd) {
			String perm = map.get("permission");
			setPermissionsForGroup(map.get("id"), engineId, EnginePermission.getPermissionByValue(perm));
		}
		
		ArrayList<StringMap<String>> usersToAdd = users.get("add");
		ArrayList<StringMap<String>> usersToRemove = users.get("remove");
		
		for(StringMap<String> map : usersToRemove) {
			removeAllPermissionsForUser(map.get("id"), engineId);
		}
		
		for(StringMap<String> map : usersToAdd) {
			String perm = map.get("permission");
			setPermissionsForUser(engineId, map.get("id"), EnginePermission.getPermissionByValue(perm));
		}
		
	}
	
	/**
	 * Check if the user has owner permission of a certain database
	 * @param userId
	 * @param engineId
	 * @return true or false
	 */
	public boolean isUserDatabaseOwner(String userId, String engineId){
		String query = "SELECT ID FROM ENGINEPERMISSION WHERE USER = '?1' AND ENGINE = '?2' AND PERMISSION = '1' UNION SELECT gep.ID FROM GROUPENGINEPERMISSION gep JOIN GROUPMEMBERS gm ON (gep.groupid = gm.groupid) WHERE gep.engine = '?2'  AND gm.memberid = '?1' AND gep.PERMISSION = '1'";
		query = query.replace("?1", userId);
		query = query.replace("?2", engineId);
		List<Object[]> res = runQuery2(query);
		return !res.isEmpty();
	}
	
	/**
	 * Delete a user and all its relationships.
	 * @param userId
	 * @param userDelete
	 */
	public void deleteUser(String userId, String userDelete){
		if(isUserAdmin(userId)){
			List<String> groups = getGroupsOwnedByUser(userId);
			for(String groupId : groups){
				removeGroup(userId, groupId);
			}
			String query = "DELETE FROM ENGINEPERMISSION WHERE USER = '?1'; DELETE FROM GROUPMEMBERS WHERE MEMBERID = '?1'; DELETE FROM USER WHERE ID = '?1';";
			query = query.replace("?1", userDelete);
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
		} else {
			throw new IllegalArgumentException("This user can't perfom this action.");
		}
	}
	
	/**
	 * Get the id list of groups owned by an user
	 * @param userId
	 * @return List<String> with the id of the groups owned by an user
	 */
	public List<String> getGroupsOwnedByUser(String userId){
		String query = "SELECT ID FROM USERGROUP WHERE OWNER = '?1'";
		query = query.replace("?1", userId);
		ArrayList<String[]> res = runQuery(query);
		List<String> groupList = new ArrayList<>();
		for(String[] row : res){
			groupList.add(row[0]);
		}
		return groupList;
	}
	
	/**
	 * Change the the public visibility of a database
	 * @param userId
	 * @param engineId
	 * @param visibility
	 */
	public void setDbPublic(String userId, String engineId, String isP, boolean isAdmin){
		
		Boolean isPublic = Boolean.parseBoolean(isP);
		String query = "UPDATE ENGINE SET PUBLIC = '?1' WHERE ID = '?2'";
		
		if(isAdmin && !isUserAdmin(userId)){
			throw new IllegalArgumentException("The user doesn't have the permission to access this resource.");
		}
		
		if(!isAdmin && !isUserDatabaseOwner(userId, engineId)){
			throw new IllegalArgumentException("The user isn't owner of the database.");
		}

		query = query.replace("?1", isPublic.toString());
		query = query.replace("?2", engineId);
		securityDB.execUpdateAndRetrieveStatement(query, true);
		securityDB.commit();
		
		if(isPublic){
			query = "DELETE FROM ENGINEPERMISSION WHERE ENGINE = '?1' AND PERMISSION IS NULL";
			query = query.replace("?1", engineId);
			securityDB.execUpdateAndRetrieveStatement(query, true);
			securityDB.commit();
		}
	}
	
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////

	/*
	 * Native user info
	 */
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public Boolean addNativeUser(AccessToken newUser, String password) throws IllegalArgumentException{
		validInformation(newUser, password);
		boolean isNewUser = checkUserExist(newUser.getUsername(), newUser.getEmail());
		if(!isNewUser) {			
			String salt = generateSalt();
			String hashedPassword = (hash(password, salt));
			String query = "INSERT INTO User (id, name, username, email, type, admin, password, salt) VALUES ('" + newUser.getEmail() + "', '"+ newUser.getName() + "', '" + newUser.getUsername() + "', '" + newUser.getEmail() + "', '" + newUser.getProvider() + "', 'FALSE', "
					+ "'" + hashedPassword + "', '" + salt + "');";
			
			securityDB.insertData(query);
			securityDB.commit();
			return true;
		} else {
			return false;
		}
		
	}
	
	/**
	 * Basic validation of the user information before creating it.
	 * @param newUser
	 * @throws IllegalArgumentException
	 */
	private void validInformation(AccessToken newUser, String password) throws IllegalArgumentException {
		String error = "";
		if(newUser.getUsername().isEmpty()){
			error += "User name can not be empty. ";
		}

		error += validEmail(newUser.getEmail());
		error += validPassword(password);
		
		if(!error.isEmpty()){
			throw new IllegalArgumentException(error);
		}
	}
	
	private String validEmail(String email){
		if(!email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]{2,}$")){
			return  email + " is not a valid email address. ";
		}
		return "";
	}
	
	private String validPassword(String password){
		Pattern pattern = Pattern.compile("^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#$%^&*])(?=.{8,})");
        Matcher matcher = pattern.matcher(password);
		
		if(!matcher.lookingAt()){
			return "Password doesn't comply with the security policies.";
		}
		return "";
	}
	
}