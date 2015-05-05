/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.nameserver.MasterDBHelper;
import prerna.nameserver.MasterDatabaseQueries;
import prerna.nameserver.MasterDatabaseURIs;
import prerna.nameserver.ModifyMasterDB;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UserPermissionsMasterDB extends ModifyMasterDB {

	public UserPermissionsMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}
	public UserPermissionsMasterDB() {
		super();
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * 
	 * @param userName	String representing the name of the user to add
	 */
	public void addUser(User newUser) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_USER_QUERY.replace("@USER_ID@", newUser.getId()));
		if(!sjsw.hasNext()) {
			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + newUser.getId());
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + newUser.getId(), MasterDatabaseURIs.USER_NAME_PROP_URI, newUser.getName(), false);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + newUser.getId(), MasterDatabaseURIs.USER_EMAIL_PROP_URI, newUser.getEmail(), false);
		}
		
		masterEngine.commit();
		masterEngine.infer();
	}
	
	/**
	 * Adds user as owner for a given engine, giving him/her all permissions.
	 * 
	 * @param engineName	Name of engine user is being added as owner for
	 * @param userId		ID of user being made owner
	 * @return				true or false for successful addition
	 */
	public Boolean addEngineOwner(String engineName, String userId) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);
		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner");
		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ROLE_URI + "/" + "OwnerRole");
		for(EnginePermission ep : EnginePermission.values()) {
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ROLE_URI + "/OwnerRole", MasterDatabaseURIs.PROP_URI + "/" + ep.getPropertyName(), "true", false);
		}
		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.USERGROUP_URI + "/" + userId + "-OwnerGroup");
		
		MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner", MasterDatabaseURIs.ENGINE_ROLEGROUP_REL_URI + "/" + engineName + ":" + engineName+"-Owner");
		MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner", MasterDatabaseURIs.ROLE_URI + "/" + "OwnerRole", MasterDatabaseURIs.ENGINEROLEGROUP_ROLE_REL_URI + "/" + engineName + "-Owner" + ":" + "OwnerRole");
		MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner", MasterDatabaseURIs.USERGROUP_URI + "/" + userId + "-OwnerGroup", MasterDatabaseURIs.ROLEGROUP_USERGROUP_REL_URI + "/" + engineName + "-Owner" + ":" + "OwnerGroup");
		MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.USERGROUP_URI + "/" + userId + "-OwnerGroup", MasterDatabaseURIs.USER_BASE_URI + "/" + userId, MasterDatabaseURIs.USERGROUP_USER_REL_URI + "/" + userId + "-OwnerGroup" + ":" + userId);

		masterEngine.commit();
		masterEngine.infer();
		return true;
	}
	
	public boolean addEngineAccessRequest(String engineName, String userRequestedBy) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userRequestedBy + "-" + engineName);
		ArrayList<String> engineOwners = getEngineOwner(engineName);
		for(String s : engineOwners) {
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + s, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userRequestedBy + "-" + engineName, MasterDatabaseURIs.USER_ENGINE_ACCESSREQUEST_REL_URI + "/" + s + ":" + userRequestedBy + "-" + engineName);
		}
		
		MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userRequestedBy + "-" + engineName, MasterDatabaseURIs.ENGINE_NAME_REQUESTED_PROP_URI, engineName, false);
		MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userRequestedBy + "-" + engineName, MasterDatabaseURIs.ENGINE_ACCESS_REQUESTOR_PROP_URI, userRequestedBy, false);
		
		masterEngine.commit();
		masterEngine.infer();
		return true;
	}
	
	public ArrayList<EngineAccessRequest> getEngineAccessRequestsForUser(String userId) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		ArrayList<EngineAccessRequest> requests = new ArrayList<EngineAccessRequest>();
										
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.ENGINE_ACCESSREQUESTS_FOR_USER.replace("@USER_ID@", userId));
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String user = sjss.getVar(names[0]).toString();
			String engine = sjss.getVar(names[1]).toString();
			String engineAccessRequest = sjss.getVar(names[2]).toString();
			
			requests.add(new EngineAccessRequest(engineAccessRequest, user, engine));
		}
		
		return requests;
	}
	
	public Boolean processEngineAccessRequest(String requestId, String approvingUserId, String[] permissions) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		String userId = "", engine = "";
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_ENGINE_ACCESSREQUEST_USER.replace("@USER_ID@", approvingUserId).replace("@REQUEST_ID@", requestId));
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			userId = sjss.getVar(names[0]).toString();
			engine = sjss.getVar(names[1]).toString();
		}
		
		if(permissions.length != 0) {
			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engine + "-" + userId + "-EngineRoleGroup");
			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ROLE_URI + "/" + engine + "-" + userId + "-Role");
			for(String ep : permissions) {
				MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ROLE_URI + "/" + engine + "-" + userId + "-Role", MasterDatabaseURIs.PROP_URI + "/" + EnginePermission.getPropertyNameByPermissionName(ep), "true", false);
			}
			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.USERGROUP_URI + "/" + engine + "-" + userId + "-UserGroup");
			
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engine + "-" + userId + "-EngineRoleGroup", MasterDatabaseURIs.ENGINE_ROLEGROUP_REL_URI + "/" + engine + ":" + engine + "-" + userId + "-EngineRoleGroup");
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engine + "-" + userId + "-EngineRoleGroup", MasterDatabaseURIs.ROLE_URI + "/" + engine + "-" + userId + "-Role", MasterDatabaseURIs.ENGINEROLEGROUP_ROLE_REL_URI + "/" + engine + "-" + userId + "-EngineRoleGroup" + ":" + engine + "-" + userId + "-Role");
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engine + "-" + userId + "-EngineRoleGroup", MasterDatabaseURIs.USERGROUP_URI + "/" + engine + "-" + userId + "-UserGroup", MasterDatabaseURIs.ROLEGROUP_USERGROUP_REL_URI + "/" + engine + "-" + userId + "-EngineRoleGroup" + ":" + engine + "-" + userId + "-UserGroup");
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.USERGROUP_URI + "/" + engine + "-" + userId + "-UserGroup", MasterDatabaseURIs.USER_BASE_URI + "/" + userId, MasterDatabaseURIs.USERGROUP_USER_REL_URI + "/" + engine + "-" + userId + "-UserGroup" + ":" + userId);
		}
		
		MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userId + "-" + engine);
		ArrayList<String> engineOwners = getEngineOwner(engine);
		for(String s : engineOwners) {
			MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + s, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userId + "-" + engine, MasterDatabaseURIs.USER_ENGINE_ACCESSREQUEST_REL_URI + "/" + s + ":" + userId + "-" + engine);
		}
		
		MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userId + "-" + engine, MasterDatabaseURIs.ENGINE_NAME_REQUESTED_PROP_URI, engine, false);
		MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.ENGINE_ACCESSREQUEST_URI + "/" + userId + "-" + engine, MasterDatabaseURIs.ENGINE_ACCESS_REQUESTOR_PROP_URI, userId, false);
		
		masterEngine.commit();
		masterEngine.infer();
		return true;
	}
	
	/**
	 * Returns a list of all engines that a given user is listed as an owner for.
	 * 
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public ArrayList<String> getUserOwnedEngines(String userId) {
		return runQueryForSingleColumn(MasterDatabaseQueries.GET_USER_ENGINES_QUERY.replace("@USER_ID@", userId));
	}
	
	/**
	 * Returns a list of all engines that a given user can see (has been given any level of permission).
	 * 
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public ArrayList<String> getUserAccessibleEngines(String userId) {
		return runQueryForSingleColumn(MasterDatabaseQueries.GET_ACCESSIBLE_ENGINES_QUERY.replace("@USER_ID@", userId));
	}
	
	/**
	 * Returns list of permissions that a given user has for a given engine.
	 * 
	 * @param engine	Engine for which user has permissions
	 * @param userId	ID of the user
	 * @return			Table of Permission->true/false for that specific permission
	 */
	public Hashtable<String, Boolean> getUserPermissionsForEngine(String engine, String userId) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_PERMISSIONS_FOR_ENGINE_QUERY.replace("@ENGINE_NAME@", engine).replace("@USER_ID@", userId));
		String[] names = sjsw.getVariables();
		Hashtable<String, Boolean> permissions = new Hashtable<String, Boolean>();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			for(int i = 0; i < names.length; i++) {
				permissions.put(names[i], Boolean.parseBoolean(sjss.getVar(names[i]).toString()));
			}
		}
		
		return permissions;
	}
	
	/**
	 * Returns all engines for which a given user has a given set of permissions.
	 * 
	 * @param user			ID of the user
	 * @param permissions	Array of permissions needed
	 * @return				List of engine names
	 */
	public ArrayList<String> getEnginesForUserAndPermissions(String user, EnginePermission[] permissions) {
		String rolePermissionTriple = "{ ?role <" + MasterDatabaseURIs.PROP_URI + "/@PERMISSION-NAME@> 'true' } ";
		String permissionTriples = "";
		
		for(EnginePermission ep : permissions) {
			permissionTriples = permissionTriples.concat(rolePermissionTriple.replace("@PERMISSION-NAME@", ep.getPropertyName()));
		}
		
		return runQueryForSingleColumn(MasterDatabaseQueries.GET_ENGINES_BY_PERMISSIONS_QUERY.replace("@USER_ID@", user).replace("@PERMISSIONS@", permissionTriples));
	}
	
	/**
	 * Returns a list of owners for a given engine.
	 * 
	 * @param engineName	Name of engine being queried
	 * @return				List of user IDs noted as engine owners
	 */
	public ArrayList<String> getEngineOwner(String engineName) {
		return runQueryForSingleColumn(MasterDatabaseQueries.GET_ENGINE_OWNER_QUERY.replace("@ENGINE_NAME@", engineName));
	}
	
	/**
	 * Deletes a given database, its associated owner's group, and all permissions relationships.
	 * 
	 * @param user			ID of the user
	 * @param engineName	Name of the engine to be deleted
	 * @return				true/false for successful deletion
	 */
	public Boolean deleteEngine(String user, String engineName) {
		MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);
		MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner");
		MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner", MasterDatabaseURIs.ENGINE_ROLEGROUP_REL_URI + "/" + engineName + ":" + engineName+"-Owner");
		MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner", MasterDatabaseURIs.ROLE_URI + "/" + "OwnerRole", MasterDatabaseURIs.ENGINEROLEGROUP_ROLE_REL_URI + "/" + engineName + "-Owner" + ":" + "OwnerRole");
		MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.ENGINEROLEGROUP_URI + "/" + engineName + "-Owner", MasterDatabaseURIs.USERGROUP_URI + "/" + user + "-OwnerGroup", MasterDatabaseURIs.ROLEGROUP_USERGROUP_REL_URI + "/" + engineName + "-Owner" + ":" + "OwnerGroup");
		
		return true;
	}
	
	/**
	 * Returns a list of values given a SPARQL query with one column/variable.
	 * 
	 * @param query		Query to be executed to retrieve engine names
	 * @return			List of engine names
	 */
	private ArrayList<String> runQueryForSingleColumn(String query) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		ArrayList<String> userEngines = new ArrayList<String>();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String eng = sjss.getVar(names[0]).toString();

			if(eng != null && !eng.isEmpty()) {
				userEngines.add(eng);
			}
		}
		
		return userEngines;
	}
}