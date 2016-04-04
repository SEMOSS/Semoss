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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.MasterDBHelper;
import prerna.nameserver.MasterDatabaseQueries;
import prerna.nameserver.MasterDatabaseURIs;
import prerna.nameserver.ModifyMasterDB;
import prerna.rdf.engine.wrappers.RDBMSSelectCheater;
import prerna.rdf.engine.wrappers.RDBMSSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UserPermissionsMasterDB {
	private RDBMSNativeEngine securityDB;
	
	public UserPermissionsMasterDB() {
		securityDB = (RDBMSNativeEngine) DIHelper.getInstance().getLocalProp(Constants.SECURITY_DB);
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * 
	 * @param userName	String representing the name of the user to add
	 */
	public Boolean addUser(User newUser) {
		ISelectWrapper sjsw = Utility.processQuery(securityDB, "SELECT NAME FROM USER WHERE ID='" + newUser.getId() + "';");
		if(!sjsw.hasNext()) {			
			String query = "INSERT INTO User (id, name, email, type) VALUES ('" + newUser.getId() + "', '"+ newUser.getName() + "', '" + newUser.getEmail() + "', '" + newUser.getLoginType() + "');";
			securityDB.insertData(query);
			securityDB.commit();
		}
		
		return true;
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
		try {
			ResultSet rs = stmt.getGeneratedKeys();
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
		
		String query = "SELECT AccessRequest.ID AS ID, User.NAME AS USERNAME, Engine.NAME AS ENGINENAME FROM AccessRequest, User, Engine WHERE AccessRequest.SUBMITTEDTO='" + userId + "' AND AccessRequest.SUBMITTEDBY=User.ID AND AccessRequest.ENGINE=Engine.ID;";
		ArrayList<String[]> ret = runQuery(query);
		for(String[] row : ret) {
			requests.add(new EngineAccessRequest(row[0], row[1], row[2]));
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
		
		ArrayList<String[]> ret = runQuery("SELECT e.NAME FROM Engine e, EnginePermission ep, Permission p WHERE ep.USER='" + userId + "' AND p.ID=" + EnginePermission.OWNER.getId() + " AND ep.PERMISSION=p.ID AND e.ID=ep.ENGINE");
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
	public ArrayList<String> getUserAccessibleEngines(String userId) {
		ArrayList<String> engines = new ArrayList<String>();
		
		ArrayList<String[]> ret = runQuery("SELECT Engine.name FROM Engine, EnginePermission WHERE EnginePermission.USER='" + userId + "' AND Engine.ID=EnginePermission.ENGINE;");
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
		String query = "SELECT e.NAME FROM Engine e, EnginePermission ep, Permission p WHERE ep.USER='" + user + "' AND e.ID=ep.ENGINE AND ep.PERMISSION=p.ID AND (p.NAME='";
		
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
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * 
	 * @param query		Query to be executed to retrieve engine names
	 * @return			List of engine names
	 */
	private ArrayList<String[]> runQuery(String query) {
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
	
	// USER TRACKING METHODS BEGIN HERE
	
	public void trackInsightExecution(String user, String db, String insightId, String session) {
		insightId = insightId.split(" ")[1];
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
}