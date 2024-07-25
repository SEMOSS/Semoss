package prerna.auth.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class SecurityOwlCreator {

	private static List<String> conceptsRequired = new ArrayList<String>();
	static {
		conceptsRequired.add("ENGINE");
		conceptsRequired.add("ENGINEMETA");
		conceptsRequired.add("ENGINEMETAKEYS");
		conceptsRequired.add("ENGINEPERMISSION");
		conceptsRequired.add("WORKSPACEENGINE");
		conceptsRequired.add("ASSETENGINE");
		conceptsRequired.add("INSIGHT");
		conceptsRequired.add("INSIGHTMETA");
		conceptsRequired.add("INSIGHTMETAKEYS");
		conceptsRequired.add("INSIGHTFRAMES");
		conceptsRequired.add("USERINSIGHTPERMISSION");
		conceptsRequired.add("SMSS_USER");
		conceptsRequired.add("SMSS_USER_ACCESS_KEYS");
		conceptsRequired.add("PERMISSION");
		conceptsRequired.add("PROJECT");
		conceptsRequired.add("PROJECTPERMISSION");
		conceptsRequired.add("PROJECTMETA");
		conceptsRequired.add("PROJECTMETAKEYS");
		conceptsRequired.add("PROJECTDEPENDENCIES");
		conceptsRequired.add("PASSWORD_RULES");
		conceptsRequired.add("PASSWORD_HISTORY");
		conceptsRequired.add("PASSWORD_RESET");
		conceptsRequired.add("SESSION_SHARE");
		//conceptsRequired.add("DATABASEACCESSREQUEST");
		conceptsRequired.add("ENGINEACCESSREQUEST");
		conceptsRequired.add("PROJECTACCESSREQUEST");
		conceptsRequired.add("INSIGHTACCESSREQUEST");
		
		// new group tables
		conceptsRequired.add("SMSS_GROUP");
		conceptsRequired.add("CUSTOMGROUPASSIGNMENT");
		conceptsRequired.add("GROUPENGINEPERMISSION");
		conceptsRequired.add("GROUPPROJECTPERMISSION");
		conceptsRequired.add("GROUPINSIGHTPERMISSION");
		
		conceptsRequired.add(Constants.ENGINE_METAKEYS);
		conceptsRequired.add(Constants.PROJECT_METAKEYS);
		conceptsRequired.add(Constants.INSIGHT_METAKEYS);
		
		// trusted token security
		conceptsRequired.add("TOKEN");
		
		// prompts
		conceptsRequired.add("PROMPT");
		conceptsRequired.add("PROMPT_INPUT");
		conceptsRequired.add("PROMPT_VARIABLE");
		conceptsRequired.add(Constants.PROMPT_METAKEYS);
		conceptsRequired.add("PROMPTMETA");
		conceptsRequired.add("PROMPTPERMISSION");
	}
	
	private static List<String[]> relationshipsRequired = new ArrayList<String[]>();
	static {
		relationshipsRequired.add(new String[] {"ENGINE", "GROUPENGINEPERMISSION", "ENGINE.ENGINEID.GROUPENGINEPERMISSION.ENGINEID"});
		relationshipsRequired.add(new String[] {"PROJECT", "GROUPPROJECTPERMISSION", "PROJECT.PROJECTID.GROUPPROJECTPERMISSION.PROJECTID"});
		relationshipsRequired.add(new String[] {"INSIGHT", "GROUPINSIGHTPERMISSION", "INSIGHT.PROJECTID.GROUPINSIGHTPERMISSION.PROJECTID"});
		relationshipsRequired.add(new String[] {"INSIGHT", "GROUPINSIGHTPERMISSION", "INSIGHT.INSIGHTID.GROUPINSIGHTPERMISSION.INSIGHTID"});
	}
	
	private IDatabaseEngine securityDb;
	
	public SecurityOwlCreator(IDatabaseEngine securityDb) {
		this.securityDb = securityDb;
	}
	
	/**
	 * Determine if we need to remake the OWL
	 * @return
	 */
	public boolean needsRemake() {
		/*
		 * This is a very simple check
		 * Just looking at the tables
		 * Not doing anything with columns but should eventually do that
		 */
		
		List<String> cleanConcepts = new ArrayList<>();
		List<String> concepts = securityDb.getPhysicalConcepts();
		if(concepts.isEmpty()) {
			return true;
		}
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}
		
		if(!cleanConcepts.containsAll(conceptsRequired)) {
			return true;
		}
		
		{
			// dont need to keep adding a million things to this list
			// just need the latest change ...
			List<String> props = securityDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/"+Constants.ENGINE_METAKEYS);
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/"+Constants.ENGINE_METAKEYS+"/DEFAULTVALUES")) {
				return true;
			}
		}
		
		List<String[]> allRelationships = securityDb.getPhysicalRelationships();
		HAS_REQUIRED_REL_LOOP : for(String[] requiredRel : relationshipsRequired) {
			for(String[] existingRel : allRelationships) {
				String c1 = Utility.getInstanceName(existingRel[0]);
				String c2 = Utility.getInstanceName(existingRel[1]);
				String relName = Utility.getInstanceName(existingRel[2]);
	
				if(c1.equals(requiredRel[0])
						&& c2.equals(requiredRel[1])
						&& relName.equals(requiredRel[2])
						) {
					continue HAS_REQUIRED_REL_LOOP;
				}
			}
			
			// if we got here, the above didn't continue the loop so we dont have this rel
			// need to remake
			return true;
		}
		
		return false;
	}
	
	/**
	 * Remake the OWL 
	 * @throws Exception 
	 */
	public void remakeOwl() throws Exception {
		try(WriteOWLEngine owlEngine = securityDb.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			// write the new OWL
			writeNewOwl(owlEngine);
		}
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws Exception 
	 */
	private void writeNewOwl(WriteOWLEngine owler) throws Exception {
		// ENGINE
		owler.addConcept("ENGINE", null, null);
		owler.addProp("ENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINE", "ENGINENAME", "VARCHAR(255)");
		owler.addProp("ENGINE", "GLOBAL", "BOOLEAN");
		owler.addProp("ENGINE", "DISCOVERABLE", "BOOLEAN");
		owler.addProp("ENGINE", "ENGINETYPE", "VARCHAR(255)");
		owler.addProp("ENGINE", "ENGINESUBTYPE", "VARCHAR(255)");
		owler.addProp("ENGINE", "COST", "VARCHAR(255)");
		owler.addProp("ENGINE", "CREATEDBY", "VARCHAR(255)");
		owler.addProp("ENGINE", "CREATEDBYTYPE", "VARCHAR(255)");
		owler.addProp("ENGINE", "DATECREATED", "TIMESTAMP");

		// ENGINEMETA
		owler.addConcept("ENGINEMETA", null, null);
		owler.addProp("ENGINEMETA", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINEMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("ENGINEMETA", "METAVALUE", "CLOB");
		owler.addProp("ENGINEMETA", "METAORDER", "INT");

		// ENGINEPERMISSION
		owler.addConcept("ENGINEPERMISSION", null, null);
		owler.addProp("ENGINEPERMISSION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "PERMISSION", "INT");
		owler.addProp("ENGINEPERMISSION", "VISIBILITY", "BOOLEAN");
		owler.addProp("ENGINEPERMISSION", "FAVORITE", "BOOLEAN");
		owler.addProp("ENGINEPERMISSION", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "DATEADDED", "TIMESTAMP");
		owler.addProp("ENGINEPERMISSION", "ENDDATE", "TIMESTAMP");

		// PROJECT
		owler.addConcept("PROJECT", null, null);
		owler.addProp("PROJECT", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECT", "PROJECTNAME", "VARCHAR(255)");
		owler.addProp("PROJECT", "GLOBAL", "BOOLEAN");
		owler.addProp("PROJECT", "DISCOVERABLE", "BOOLEAN");
		owler.addProp("PROJECT", "TYPE", "VARCHAR(255)");
		owler.addProp("PROJECT", "COST", "VARCHAR(255)");
		owler.addProp("PROJECT", "CATALOGNAME", "VARCHAR(255)");
		owler.addProp("PROJECT", "HASPORTAL", "BOOLEAN");
		owler.addProp("PROJECT", "PORTALNAME", "VARCHAR(255)");
		owler.addProp("PROJECT", "PORTALPUBLISHED", "TIMESTAMP");
		owler.addProp("PROJECT", "PORTALPUBLISHEDUSER", "VARCHAR(255)");
		owler.addProp("PROJECT", "PORTALPUBLISHEDTYPE", "VARCHAR(255)");
		owler.addProp("PROJECT", "REACTORSCOMPILED", "TIMESTAMP");
		owler.addProp("PROJECT", "REACTORSCOMPILEDUSER", "VARCHAR(255)");
		owler.addProp("PROJECT", "REACTORSCOMPILEDTYPE", "VARCHAR(255)");
		owler.addProp("PROJECT", "CREATEDBY", "VARCHAR(255)");
		owler.addProp("PROJECT", "CREATEDBYTYPE", "VARCHAR(255)");
		owler.addProp("PROJECT", "DATECREATED", "TIMESTAMP");
		
		// PROJECTPERMISSION
		owler.addConcept("PROJECTPERMISSION", null, null);
		owler.addProp("PROJECTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECTPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("PROJECTPERMISSION", "PERMISSION", "INT");
		owler.addProp("PROJECTPERMISSION", "VISIBILITY", "BOOLEAN");
		owler.addProp("PROJECTPERMISSION", "FAVORITE", "BOOLEAN");
		owler.addProp("PROJECTPERMISSION", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("PROJECTPERMISSION", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");
		owler.addProp("PROJECTPERMISSION", "DATEADDED", "TIMESTAMP");
		owler.addProp("PROJECTPERMISSION", "ENDDATE", "TIMESTAMP");
		
		
		// PROJECTMETA
		owler.addConcept("PROJECTMETA", null, null);
		owler.addProp("PROJECTMETA", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECTMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("PROJECTMETA", "METAVALUE", "CLOB");
		owler.addProp("PROJECTMETA", "METAORDER", "INT");
		
		// PROJECTDEPENDENCIES
		owler.addConcept("PROJECTDEPENDENCIES", null, null);
		owler.addProp("PROJECTDEPENDENCIES", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECTDEPENDENCIES", "ENGINEID", "VARCHAR(255)");
		owler.addProp("PROJECTDEPENDENCIES", "USERID", "VARCHAR(255)");
		owler.addProp("PROJECTDEPENDENCIES", "TYPE", "VARCHAR(255)");
		owler.addProp("PROJECTDEPENDENCIES", "DATEADDED", "TIMESTAMP");

		// WORKSPACEENGINE
		owler.addConcept("WORKSPACEENGINE", null, null);
		owler.addProp("WORKSPACEENGINE", "PROJECTID", "VARCHAR(255)");
		owler.addProp("WORKSPACEENGINE", "USERID", "VARCHAR(255)");
		owler.addProp("WORKSPACEENGINE", "TYPE", "VARCHAR(255)");
		
		// ASSETENGINE
		owler.addConcept("ASSETENGINE", null, null);
		owler.addProp("ASSETENGINE", "PROJECTID", "VARCHAR(255)");
		owler.addProp("ASSETENGINE", "USERID", "VARCHAR(255)");
		owler.addProp("ASSETENGINE", "TYPE", "VARCHAR(255)");
		
		// INSIGHT
		owler.addConcept("INSIGHT", null, null);
		owler.addProp("INSIGHT", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHT", "PROJECTID", "VARCHAR(255)");
		owler.addProp("INSIGHT", "INSIGHTNAME", "VARCHAR(255)");
		owler.addProp("INSIGHT", "GLOBAL", "BOOLEAN");
		owler.addProp("INSIGHT", "EXECUTIONCOUNT", "BIGINT");
		owler.addProp("INSIGHT", "CREATEDON", "TIMESTAMP");
		owler.addProp("INSIGHT", "LASTMODIFIEDON", "TIMESTAMP");
		owler.addProp("INSIGHT", "LAYOUT", "VARCHAR(255)");
		owler.addProp("INSIGHT", "CACHEABLE", "BOOLEAN");
		owler.addProp("INSIGHT", "CACHEMINUTES", "INT");
		owler.addProp("INSIGHT", "CACHECRON", "VARCHAR(25)");
		owler.addProp("INSIGHT", "CACHEDON", "TIMESTAMP");
		owler.addProp("INSIGHT", "CACHEENCRYPT", "BOOLEAN");
		owler.addProp("INSIGHT", "RECIPE", "CLOB");
		owler.addProp("INSIGHT", "SCHEMANAME", "VARCHAR(255)");

		// USERINSIGHTPERMISSION
		owler.addConcept("USERINSIGHTPERMISSION", null, null);
		owler.addProp("USERINSIGHTPERMISSION", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "PERMISSION", "INT");
		owler.addProp("USERINSIGHTPERMISSION", "FAVORITE", "BOOLEAN");
		owler.addProp("USERINSIGHTPERMISSION", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "DATEADDED", "TIMESTAMP");
		owler.addProp("USERINSIGHTPERMISSION", "ENDDATE", "TIMESTAMP");

		// INSIGHTMETA
		owler.addConcept("INSIGHTMETA", null, null);
		owler.addProp("INSIGHTMETA", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "PROJECTID", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "METAVALUE", "CLOB");
		owler.addProp("INSIGHTMETA", "METAORDER", "INT");
		
		// INSIGHTFRAMES
		owler.addConcept("INSIGHTFRAMES", null, null);
		owler.addProp("INSIGHTFRAMES", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHTFRAMES", "PROJECTID", "VARCHAR(255)");
		owler.addProp("INSIGHTFRAMES", "TABLENAME", "VARCHAR(255)");
		owler.addProp("INSIGHTFRAMES", "TABLETYPE", "VARCHAR(255)");
		owler.addProp("INSIGHTFRAMES", "COLUMNNAME", "VARCHAR(255)");
		owler.addProp("INSIGHTFRAMES", "COLUMNTYPE", "VARCHAR(255)");
		owler.addProp("INSIGHTFRAMES", "ADDITIONALTYPE", "VARCHAR(255)");
		
		// SMSS_USER
		owler.addConcept("SMSS_USER", null, null);
		owler.addProp("SMSS_USER", "ID", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "NAME", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "EMAIL", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "TYPE", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "PASSWORD", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "SALT", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "USERNAME", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "ADMIN", "BOOLEAN");
		owler.addProp("SMSS_USER", "PUBLISHER", "BOOLEAN");
		owler.addProp("SMSS_USER", "EXPORTER", "BOOLEAN");
		owler.addProp("SMSS_USER", "DATECREATED", "TIMESTAMP");
		owler.addProp("SMSS_USER", "LASTLOGIN", "TIMESTAMP");
		owler.addProp("SMSS_USER", "LASTPASSWORDRESET", "TIMESTAMP");
		owler.addProp("SMSS_USER", "LOCKED", "BOOLEAN");
		owler.addProp("SMSS_USER", "PHONE", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "PHONEEXTENSION", "VARCHAR(255)");
		owler.addProp("SMSS_USER", "COUNTRYCODE", "VARCHAR(255)");
		
		// SMSS_USER_ACCESS_KEYS
		owler.addConcept("SMSS_USER_ACCESS_KEYS", null, null);
		// TODO: DELETE ID AFTER SOME TIME, REPLACED WITH USERID ... 2023-09-19
		owler.addProp("SMSS_USER_ACCESS_KEYS", "ID", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "USERID", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "TYPE", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "ACCESSKEY", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "SECRETKEY", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "SECRETSALT", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "DATECREATED", "TIMESTAMP");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "LASTUSED", "TIMESTAMP");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "TOKENNAME", "VARCHAR(255)");
		owler.addProp("SMSS_USER_ACCESS_KEYS", "TOKENDESCRIPTION", "VARCHAR(500)");

		// TOKEN
		owler.addConcept("TOKEN", null, null);
		owler.addProp("TOKEN", "IPADDR", "VARCHAR(255)");
		owler.addProp("TOKEN", "VAL", "VARCHAR(255)");
		owler.addProp("TOKEN", "DATEADDED", "TIMESTAMP");
		owler.addProp("TOKEN", "CLIENTID", "VARCHAR(255)");
		
		// PERMISSION
		owler.addConcept("PERMISSION", null, null);
		owler.addProp("PERMISSION", "ID", "INT");
		owler.addProp("PERMISSION", "NAME", "VARCHAR(255)");
		
		// PASSWORD_RULES
		owler.addConcept("PASSWORD_RULES", null, null);
		owler.addProp("PASSWORD_RULES", "PASS_LENGTH", "INT");
		owler.addProp("PASSWORD_RULES", "REQUIRE_UPPER", "BOOLEAN");
		owler.addProp("PASSWORD_RULES", "REQUIRE_LOWER", "BOOLEAN");
		owler.addProp("PASSWORD_RULES", "REQUIRE_NUMERIC", "BOOLEAN");
		owler.addProp("PASSWORD_RULES", "REQUIRE_SPECIAL", "BOOLEAN");
		owler.addProp("PASSWORD_RULES", "EXPIRATION_DAYS", "INT");
		owler.addProp("PASSWORD_RULES", "ADMIN_RESET_EXPIRATION", "BOOLEAN");
		owler.addProp("PASSWORD_RULES", "ALLOW_USER_PASS_CHANGE", "BOOLEAN");
		owler.addProp("PASSWORD_RULES", "PASS_REUSE_COUNT", "INT");
		owler.addProp("PASSWORD_RULES", "DAYS_TO_LOCK", "INT");
		owler.addProp("PASSWORD_RULES", "DAYS_TO_LOCK_WARNING", "INT");

		// PASSWORD_HISTORY
		owler.addConcept("PASSWORD_HISTORY", null, null);
		owler.addProp("PASSWORD_HISTORY", "ID", "VARCHAR(255)");
		owler.addProp("PASSWORD_HISTORY", "USERID", "VARCHAR(255)");
		owler.addProp("PASSWORD_HISTORY", "TYPE", "VARCHAR(255)");
		owler.addProp("PASSWORD_HISTORY", "PASSWORD", "VARCHAR(255)");
		owler.addProp("PASSWORD_HISTORY", "SALT", "VARCHAR(255)");
		owler.addProp("PASSWORD_HISTORY", "DATE_ADDED", "TIMESTAMP");
		
		// PASSWORD_RESET
		owler.addConcept("PASSWORD_RESET", null, null);
		owler.addProp("PASSWORD_RESET", "EMAIL", "VARCHAR(255)");
		owler.addProp("PASSWORD_RESET", "TYPE", "VARCHAR(255)");
		owler.addProp("PASSWORD_RESET", "TOKEN", "VARCHAR(255)");
		owler.addProp("PASSWORD_RESET", "DATE_ADDED", "TIMESTAMP");
		
		// SESSION_SHARE
		owler.addConcept("SESSION_SHARE", null, null);
		owler.addProp("SESSION_SHARE", "SHARE_VAL", "VARCHAR(255)");
		owler.addProp("SESSION_SHARE", "SESSION_VAL", "VARCHAR(255)");
		owler.addProp("SESSION_SHARE", "ROUTE_VAL", "VARCHAR(255)");
		owler.addProp("SESSION_SHARE", "DATE_ADDED", "TIMESTAMP");
		owler.addProp("SESSION_SHARE", "DATE_USED", "TIMESTAMP");
		owler.addProp("SESSION_SHARE", "USE_VALID", "BOOLEAN");
		owler.addProp("SESSION_SHARE", "USERID", "VARCHAR(255)");
		owler.addProp("SESSION_SHARE", "TYPE", "VARCHAR(255)");

		// ENGINEACCESSREQUEST
		owler.addConcept("ENGINEACCESSREQUEST", null, null);
		owler.addProp("ENGINEACCESSREQUEST", "ID", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "REQUEST_USERID", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "REQUEST_TYPE", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "REQUEST_TIMESTAMP", "TIMESTAMP");
		owler.addProp("ENGINEACCESSREQUEST", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "PERMISSION", "INT");
		owler.addProp("ENGINEACCESSREQUEST", "REQUEST_REASON", "CLOB");
		owler.addProp("ENGINEACCESSREQUEST", "APPROVER_USERID", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "APPROVER_TYPE", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "APPROVER_DECISION", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "APPROVER_TIMESTAMP", "TIMESTAMP");
		owler.addProp("ENGINEACCESSREQUEST", "SUBMITTED_BY_USERID", "VARCHAR(255)");
		owler.addProp("ENGINEACCESSREQUEST", "SUBMITTED_BY_TYPE", "VARCHAR(255)");
		
		// PROJECTACCESSREQUEST 
		owler.addConcept("PROJECTACCESSREQUEST", null, null);
		owler.addProp("PROJECTACCESSREQUEST", "ID", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "REQUEST_USERID", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "REQUEST_TYPE", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "REQUEST_TIMESTAMP", "TIMESTAMP");
		owler.addProp("PROJECTACCESSREQUEST", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "PERMISSION", "INT");
		owler.addProp("PROJECTACCESSREQUEST", "REQUEST_REASON", "CLOB");
		owler.addProp("PROJECTACCESSREQUEST", "APPROVER_USERID", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "APPROVER_TYPE", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "APPROVER_DECISION", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "APPROVER_TIMESTAMP", "TIMESTAMP");
		owler.addProp("PROJECTACCESSREQUEST", "SUBMITTED_BY_USERID", "VARCHAR(255)");
		owler.addProp("PROJECTACCESSREQUEST", "SUBMITTED_BY_TYPE", "VARCHAR(255)");
		
		// INSIGHTACCESSREQUEST 
		owler.addConcept("INSIGHTACCESSREQUEST", null, null);
		owler.addProp("INSIGHTACCESSREQUEST", "ID", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "REQUEST_USERID", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "REQUEST_TYPE", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "REQUEST_TIMESTAMP", "TIMESTAMP");
		owler.addProp("INSIGHTACCESSREQUEST", "PROJECTID", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "PERMISSION", "INT");
		owler.addProp("INSIGHTACCESSREQUEST", "REQUEST_REASON", "CLOB");
		owler.addProp("INSIGHTACCESSREQUEST", "APPROVER_USERID", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "APPROVER_TYPE", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "APPROVER_DECISION", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "APPROVER_TIMESTAMP", "TIMESTAMP");
		owler.addProp("INSIGHTACCESSREQUEST", "SUBMITTED_BY_USERID", "VARCHAR(255)");
		owler.addProp("INSIGHTACCESSREQUEST", "SUBMITTED_BY_TYPE", "VARCHAR(255)");
		
		// joins
		owler.addRelation("ENGINE", "ENGINEMETA", "ENGINE.ENGINEID.ENGINEMETA.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEPERMISSION", "ENGINE.ENGINEID.ENGINEPERMISSION.ENGINEID");
		owler.addRelation("ENGINE", "WORKSPACEENGINE", "ENGINE.ENGINEID.WORKSPACEENGINE.ENGINEID");
		
		owler.addRelation("PROJECT", "ASSETENGINE", "PROJECT.PROJECTID.ASSETENGINE.PROJECTID");
		owler.addRelation("PROJECT", "PROJECTMETA", "PROJECT.PROJECTID.PROJECTMETA.PROJECTID");
		owler.addRelation("PROJECT", "INSIGHT", "PROJECT.PROJECTID.INSIGHT.PROJECTID");
		owler.addRelation("PROJECT", "USERINSIGHTPERMISSION", "PROJECT.PROJECTID.USERINSIGHTPERMISSION.PROJECTID");
		owler.addRelation("PROJECT", "PROJECTPERMISSION", "PROJECT.PROJECTID.PROJECTPERMISSION.PROJECTID");

		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.INSIGHTID.USERINSIGHTPERMISSION.INSIGHTID");
		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.PROJECTID.USERINSIGHTPERMISSION.PROJECTID");

		owler.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "SMSS_USER.ID.USERINSIGHTPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "ENGINEPERMISSION", "SMSS_USER.ID.ENGINEPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "PROJECTPERMISSION", "SMSS_USER.ID.PROJECTPERMISSION.USERID");

		owler.addRelation("ENGINEPERMISSION", "PERMISSION", "ENGINEPERMISSION.PERMISSION.PERMISSION.ID");
		owler.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "USERINSIGHTPERMISSION.PERMISSION.PERMISSION.ID");
		owler.addRelation("PROJECTPERMISSION", "PERMISSION", "PROJECTPERMISSION.PERMISSION.PERMISSION.ID");
		
		owler.addRelation("INSIGHT", "INSIGHTMETA", "INSIGHT.INSIGHTID.INSIGHTMETA.INSIGHTID");
		owler.addRelation("INSIGHT", "INSIGHTMETA", "INSIGHT.PROJECTID.INSIGHTMETA.PROJECTID");
		
		owler.addRelation("INSIGHT", "INSIGHTFRAMES", "INSIGHT.INSIGHTID.INSIGHTFRAMES.INSIGHTID");
		owler.addRelation("INSIGHT", "INSIGHTFRAMES", "INSIGHT.PROJECTID.INSIGHTFRAMES.PROJECTID");
		
		// new group details
		// SMSS_GROUP
		owler.addConcept("SMSS_GROUP", null, null);
		owler.addProp("SMSS_GROUP", "ID", "VARCHAR(255)");
		owler.addProp("SMSS_GROUP", "TYPE", "VARCHAR(255)");
		owler.addProp("SMSS_GROUP", "DESCRIPTION", "CLOB");
		owler.addProp("SMSS_GROUP", "IS_CUSTOM_GROUP", "BOOLEAN");
		owler.addProp("SMSS_GROUP", "DATEADDED", "TIMESTAMP");
		owler.addProp("SMSS_GROUP", "USERID", "VARCHAR(255)");
		owler.addProp("SMSS_GROUP", "USERIDTYPE", "VARCHAR(255)");
		
		// CUSTOMGROUPASSIGNMENT
		owler.addConcept("CUSTOMGROUPASSIGNMENT", null, null);
		owler.addProp("CUSTOMGROUPASSIGNMENT", "GROUPID", "VARCHAR(255)");
		owler.addProp("CUSTOMGROUPASSIGNMENT", "USERID", "VARCHAR(255)");
		owler.addProp("CUSTOMGROUPASSIGNMENT", "TYPE", "VARCHAR(255)");
		owler.addProp("CUSTOMGROUPASSIGNMENT", "DATEADDED", "TIMESTAMP");
		owler.addProp("CUSTOMGROUPASSIGNMENT", "ENDDATE", "TIMESTAMP");
		owler.addProp("CUSTOMGROUPASSIGNMENT", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("CUSTOMGROUPASSIGNMENT", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");
		
		// GROUPENGINEPERMISSION
		owler.addConcept("GROUPENGINEPERMISSION", null, null);
		owler.addProp("GROUPENGINEPERMISSION", "ID", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "TYPE", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "PERMISSION", "INT");
		owler.addProp("GROUPENGINEPERMISSION", "DATEADDED", "TIMESTAMP");
		owler.addProp("GROUPENGINEPERMISSION", "ENDDATE", "TIMESTAMP");
		owler.addProp("GROUPENGINEPERMISSION", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");

		// GROUPPROJECTPERMISSION
		owler.addConcept("GROUPPROJECTPERMISSION", null, null);
		owler.addProp("GROUPPROJECTPERMISSION", "ID", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "TYPE", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "PERMISSION", "INT");
		owler.addProp("GROUPPROJECTPERMISSION", "DATEADDED", "TIMESTAMP");
		owler.addProp("GROUPPROJECTPERMISSION", "ENDDATE", "TIMESTAMP");
		owler.addProp("GROUPPROJECTPERMISSION", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");
		
		// GROUPPROJECTPERMISSION
		owler.addConcept("GROUPINSIGHTPERMISSION", null, null);
		owler.addProp("GROUPINSIGHTPERMISSION", "ID", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "TYPE", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "PERMISSION", "INT");
		owler.addProp("GROUPINSIGHTPERMISSION", "DATEADDED", "TIMESTAMP");
		owler.addProp("GROUPINSIGHTPERMISSION", "ENDDATE", "TIMESTAMP");
		owler.addProp("GROUPINSIGHTPERMISSION", "PERMISSIONGRANTEDBY", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "PERMISSIONGRANTEDBYTYPE", "VARCHAR(255)");
		
		// PROMPT
		owler.addConcept("PROMPT", null, null);
		owler.addProp("PROMPT", "ID", "VARCHAR(255)");
		owler.addProp("PROMPT", "TITLE", "VARCHAR(255)");
		owler.addProp("PROMPT", "CONTEXT", "CLOB");
		owler.addProp("PROMPT", "CONTEXT", "CLOB");
		owler.addProp("PROMPT", "CREATED_BY", "VARCHAR(255)");
		owler.addProp("PROMPT", "DATE_CREATED", "TIMESTAMP");
		owler.addProp("PROMPT", "IS_PUBLIC", "BOOLEAN");

		// PROMPTMETA
		owler.addConcept("PROMPTMETA", null, null);
		owler.addProp("PROMPTMETA", "PROMPT_ID", "VARCHAR(255)");
		owler.addProp("PROMPTMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("PROMPTMETA", "METAVALUE", "CLOB");
		owler.addProp("PROMPTMETA", "METAORDER", "INT");

		// PROMPTPERMISSION
		owler.addConcept("PROMPTPERMISSION", null, null);
		owler.addProp("PROMPTPERMISSION", "PROMPT_ID", "VARCHAR(255)");
		owler.addProp("PROMPTPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("PROMPTPERMISSION", "FAVORITE", "BOOLEAN");
		owler.addProp("PROMPTPERMISSION", "DATEADDED", "TIMESTAMP");

		owler.addConcept("PROMPT_INPUT", null, null);
		owler.addProp("PROMPT_INPUT", "ID", "VARCHAR(255)");
		owler.addProp("PROMPT_INPUT", "PROMPT_ID", "VARCHAR(255)");
		owler.addProp("PROMPT_INPUT", "INDEX", "INT");
		owler.addProp("PROMPT_INPUT", "KEY", "VARCHAR(255)");
		owler.addProp("PROMPT_INPUT", "DISPLAY", "VARCHAR(255)");
		owler.addProp("PROMPT_INPUT", "TYPE", "VARCHAR(255)");
		owler.addProp("PROMPT_INPUT", "IS_HIDDEN_PHRASE_INPUT_TOKEN", "BOOLEAN");
		owler.addProp("PROMPT_INPUT", "LINKED_INPUT_TOKEN", "VARCHAR(255)");

		owler.addConcept("PROMPT_VARIABLE", null, null);
		owler.addProp("PROMPT_VARIABLE", "ID", "VARCHAR(255)");
		owler.addProp("PROMPT_VARIABLE", "PROMPT_ID", "VARCHAR(255)");
		owler.addProp("PROMPT_VARIABLE", "PROMPT_INPUT_ID", "VARCHAR(255)");
		owler.addProp("PROMPT_VARIABLE", "TYPE", "VARCHAR(255)");
		owler.addProp("PROMPT_VARIABLE", "META", "VARCHAR(255)");
		owler.addProp("PROMPT_VARIABLE", "VALUE", "VARCHAR(255)");

		// "ENGINEMETAKEYS", "PROJECTMETAKEYS", "INSIGHTMETAKEYS, PROMPTMETAKEYS"
		List<String> metaKeyTableNames = Arrays.asList(Constants.ENGINE_METAKEYS, Constants.PROJECT_METAKEYS,
				Constants.INSIGHT_METAKEYS, Constants.PROMPT_METAKEYS);
		for(String tableName : metaKeyTableNames) {
			// all have the same columns and default values
			owler.addConcept(tableName, null, null);
			owler.addProp(tableName, "METAKEY", "VARCHAR(255)");
			owler.addProp(tableName, "SINGLEMULTI", "VARCHAR(255)");
			owler.addProp(tableName, "DISPLAYORDER", "INT");
			owler.addProp(tableName, "DISPLAYOPTIONS", "VARCHAR(255)");
			owler.addProp(tableName, "DEFAULTVALUES", "VARCHAR(500)");
		}

		owler.addRelation("SMSS_USER", "CUSTOMGROUPASSIGNMENT", "SMSS_USER.ID.CUSTOMGROUPASSIGNMENT.USERID");
		owler.addRelation("SMSS_GROUP", "CUSTOMGROUPASSIGNMENT", "SMSS_GROUP.ID.CUSTOMGROUPASSIGNMENT.GROUPID");

		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.ID.GROUPENGINEPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.TYPE.GROUPENGINEPERMISSION.TYPE");
		owler.addRelation("ENGINE", "GROUPENGINEPERMISSION", "ENGINE.ENGINEID.GROUPENGINEPERMISSION.ENGINEID");

		owler.addRelation("SMSS_GROUP", "GROUPPROJECTPERMISSION", "SMSS_GROUP.ID.GROUPPROJECTPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPPROJECTPERMISSION", "SMSS_GROUP.TYPE.GROUPPROJECTPERMISSION.TYPE");
		owler.addRelation("PROJECT", "GROUPPROJECTPERMISSION", "PROJECT.PROJECTID.GROUPPROJECTPERMISSION.PROJECTID");

		owler.addRelation("SMSS_GROUP", "GROUPINSIGHTPERMISSION", "SMSS_GROUP.ID.GROUPINSIGHTPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPINSIGHTPERMISSION", "SMSS_GROUP.TYPE.GROUPINSIGHTPERMISSION.TYPE");
		owler.addRelation("INSIGHT", "GROUPINSIGHTPERMISSION", "INSIGHT.PROJECTID.GROUPINSIGHTPERMISSION.PROJECTID");
		owler.addRelation("INSIGHT", "GROUPINSIGHTPERMISSION", "INSIGHT.INSIGHTID.GROUPINSIGHTPERMISSION.INSIGHTID");
		
		owler.commit();
		owler.export();
	}
	
}
