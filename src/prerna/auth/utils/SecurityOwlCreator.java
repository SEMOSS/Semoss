package prerna.auth.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;

public class SecurityOwlCreator {

	static private List<String> conceptsRequired = new Vector<String>();
	static {
		conceptsRequired.add("ENGINE");
		conceptsRequired.add("ENGINEMETA");
		conceptsRequired.add("ENGINEPERMISSION");
		conceptsRequired.add("WORKSPACEENGINE");
		conceptsRequired.add("ASSETENGINE");
		conceptsRequired.add("INSIGHT");
		conceptsRequired.add("INSIGHTMETA");
		conceptsRequired.add("USERINSIGHTPERMISSION");
		conceptsRequired.add("SMSS_USER");
		conceptsRequired.add("PERMISSION");
		conceptsRequired.add("PROJECT");
		conceptsRequired.add("PROJECTPERMISSION");
		conceptsRequired.add("PROJECTMETA");
		
		// new group tables
		conceptsRequired.add("SMSS_GROUP");
		conceptsRequired.add("GROUPENGINEPERMISSION");
		conceptsRequired.add("GROUPPROJECTPERMISSION");
		conceptsRequired.add("GROUPINSIGHTPERMISSION");
		
		// trusted token security
		conceptsRequired.add("TOKENS");
	}
	
	private IEngine securityDb;
	
	public SecurityOwlCreator(IEngine securityDb) {
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
		
		List<String> cleanConcepts = new Vector<String>();
		List<String> concepts = securityDb.getPhysicalConcepts();
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}
		
		boolean check1 = cleanConcepts.containsAll(conceptsRequired);
		if(check1) {
			List<String> props = securityDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/ENGINEMETA");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/METAORDER/ENGINEMETA")) {
				return true;
			}
			
			props = securityDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/USERINSIGHTPERMISSION");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/FAVORITE/USERINSIGHTPERMISSION")) {
				return true;
			}
			
			props = securityDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/ENGINEPERMISSION");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/FAVORITE/ENGINEPERMISSION")) {
				return true;
			}
			
			props = securityDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/INSIGHT");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/PROJECTID/INSIGHT")) {
				return true;
			}
			
			props = securityDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/SMSS_GROUP");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/DESCRIPTION/SMSS_GROUP")) {
				return true;
			}
		}
		return !check1;
	}
	
	/**
	 * Remake the OWL 
	 * @throws IOException
	 */
	public void remakeOwl() throws IOException {
		// get the existing engine and close it
		RDFFileSesameEngine baseEngine = securityDb.getBaseDataEngine();
		if(baseEngine != null) {
			baseEngine.closeDB();
		}
		
		// now delete the file if exists
		// and we will make a new
		String owlLocation = securityDb.getOWL();
		
		File f = new File(owlLocation);
		if(f.exists()) {
			f.delete();
		}
		
		// write the new OWL
		writeNewOwl(owlLocation);
		
		// now reload into security db
		securityDb.setOWL(owlLocation);
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws IOException
	 */
	private void writeNewOwl(String owlLocation) throws IOException {
		Owler owler = new Owler(owlLocation, ENGINE_TYPE.RDBMS);

		// ENGINE
		owler.addConcept("ENGINE", null, null);
		owler.addProp("ENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINE", "ENGINENAME", "VARCHAR(255)");
		owler.addProp("ENGINE", "GLOBAL", "BOOLEAN");
		owler.addProp("ENGINE", "TYPE", "VARCHAR(255)");
		owler.addProp("ENGINE", "COST", "VARCHAR(255)");
		
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

		//PROJECT
		owler.addConcept("PROJECT", null, null);
		owler.addProp("PROJECT", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECT", "PROJECTNAME", "VARCHAR(255)");
		owler.addProp("PROJECT", "GLOBAL", "BOOLEAN");
		owler.addProp("PROJECT", "TYPE", "VARCHAR(255)");
		owler.addProp("PROJECT", "COST", "VARCHAR(255)");
		
		//PROJECTPERMISSION
		owler.addConcept("PROJECTPERMISSION", null, null);
		owler.addProp("PROJECTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECTPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("PROJECTPERMISSION", "PERMISSION", "INT");
		owler.addProp("PROJECTPERMISSION", "VISIBILITY", "BOOLEAN");
		owler.addProp("PROJECTPERMISSION", "FAVORITE", "BOOLEAN");

		
		//PROJECTMETA
		owler.addConcept("PROJECTMETA", null, null);
		owler.addProp("PROJECTMETA", "PROJECTID", "VARCHAR(255)");
		owler.addProp("PROJECTMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("PROJECTMETA", "METAVALUE", "CLOB");
		owler.addProp("PROJECTMETA", "METAORDER", "INT");
		
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

		// USERINSIGHTPERMISSION
		owler.addConcept("USERINSIGHTPERMISSION", null, null);
		owler.addProp("USERINSIGHTPERMISSION", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "PERMISSION", "INT");
		owler.addProp("USERINSIGHTPERMISSION", "FAVORITE", "BOOLEAN");

		// INSIGHTMETA
		owler.addConcept("INSIGHTMETA", null, null);
		owler.addProp("INSIGHTMETA", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "PROJECTID", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "METAVALUE", "CLOB");
		owler.addProp("INSIGHTMETA", "METAORDER", "INT");
		
		// USER
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

		// PERMISSION
		owler.addConcept("PERMISSION", null, null);
		owler.addProp("PERMISSION", "ID", "INT");
		owler.addProp("PERMISSION", "NAME", "VARCHAR(255)");
		
		// joins
		owler.addRelation("ENGINE", "ENGINEMETA", "ENGINE.ENGINEID.ENGINEMETA.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEPERMISSION", "ENGINE.ENGINEID.ENGINEPERMISSION.ENGINEID");
		owler.addRelation("ENGINE", "WORKSPACEENGINE", "ENGINE.ENGINEID.WORKSPACEENGINE.ENGINEID");
		owler.addRelation("ENGINE", "ASSETENGINE", "ENGINE.ENGINEID.ASSETENGINE.ENGINEID");
		
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
		
		// new group details
		// SMSS_GROUP
		owler.addConcept("SMSS_GROUP", null, null);
		owler.addProp("SMSS_GROUP", "ID", "VARCHAR(255)");
		owler.addProp("SMSS_GROUP", "TYPE", "VARCHAR(255)");
		owler.addProp("SMSS_GROUP", "DESCRIPTION", "CLOB");

		// GROUPENGINEPERMISSION
		owler.addConcept("GROUPENGINEPERMISSION", null, null);
		owler.addProp("GROUPENGINEPERMISSION", "ID", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "TYPE", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("GROUPENGINEPERMISSION", "PERMISSION", "INT");

		// GROUPPROJECTPERMISSION
		owler.addConcept("GROUPPROJECTPERMISSION", null, null);
		owler.addProp("GROUPPROJECTPERMISSION", "ID", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "TYPE", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("GROUPPROJECTPERMISSION", "PERMISSION", "INT");
		
		// GROUPPROJECTPERMISSION
		owler.addConcept("GROUPINSIGHTPERMISSION", null, null);
		owler.addProp("GROUPINSIGHTPERMISSION", "ID", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "TYPE", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "PROJECTID", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("GROUPINSIGHTPERMISSION", "PERMISSION", "INT");
		
		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.ID.GROUPENGINEPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.TYPE.GROUPENGINEPERMISSION.TYPE");
		
		owler.addRelation("SMSS_GROUP", "GROUPPROJECTPERMISSION", "SMSS_GROUP.ID.GROUPPROJECTPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPPROJECTPERMISSION", "SMSS_GROUP.TYPE.GROUPPROJECTPERMISSION.TYPE");
		
		owler.addRelation("SMSS_GROUP", "GROUPINSIGHTPERMISSION", "SMSS_GROUP.ID.GROUPINSIGHTPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPINSIGHTPERMISSION", "SMSS_GROUP.TYPE.GROUPINSIGHTPERMISSION.TYPE");

		// TOKEN
		owler.addConcept("TOKEN", null, null);
		owler.addProp("TOKEN", "IPADDR", "VARCHAR(255)");
		owler.addProp("TOKEN", "VAL", "VARCHAR(255)");
		owler.addProp("TOKEN", "DATEADDED", "TIMESTAMP");
		
		owler.commit();
		owler.export();
	}
	
}
