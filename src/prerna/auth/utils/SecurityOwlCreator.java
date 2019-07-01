package prerna.auth.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.OWLER;
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
		conceptsRequired.add("USERINSIGHTPERMISSION");
		conceptsRequired.add("USER");
		conceptsRequired.add("PERMISSION");
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
		List<String> concepts = securityDb.getConcepts(true);
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}
		
		return !cleanConcepts.containsAll(conceptsRequired);
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
		OWLER owler = new OWLER(owlLocation, ENGINE_TYPE.RDBMS);

		// ENGINE
		owler.addConcept("ENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINE", "ENGINEID", "ENGINENAME", "VARCHAR(255)", null);
		owler.addProp("ENGINE", "ENGINEID", "GLOBAL", "BOOLEAN", null);
		owler.addProp("ENGINE", "ENGINEID", "TYPE", "VARCHAR(255)", null);
		owler.addProp("ENGINE", "ENGINEID", "COST", "VARCHAR(255)", null);
		
		// ENGINEMETA
		owler.addConcept("ENGINEMETA", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINEMETA", "ENGINEID", "KEY", "VARCHAR(255)", null);
		owler.addProp("ENGINEMETA", "ENGINEID", "VALUE", "VARCHAR(255)", null);

		// ENGINEPERMISSION
		owler.addConcept("ENGINEPERMISSION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "ENGINEID", "USERID", "VARCHAR(255)", null);
		owler.addProp("ENGINEPERMISSION", "ENGINEID", "PERMISSION", "INT", null);
		owler.addProp("ENGINEPERMISSION", "ENGINEID", "VISIBILITY", "BOOLEAN", null);

		// WORKSPACEENGINE
		owler.addConcept("WORKSPACEENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("WORKSPACEENGINE", "ENGINEID", "USERID", "VARCHAR(255)", null);
		owler.addProp("WORKSPACEENGINE", "ENGINEID", "TYPE", "VARCHAR(255)", null);
		
		// ASSETENGINE
		owler.addConcept("ASSETENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ASSETENGINE", "ENGINEID", "USERID", "VARCHAR(255)", null);
		owler.addProp("ASSETENGINE", "ENGINEID", "TYPE", "VARCHAR(255)", null);
		
		// INSIGHT
		owler.addConcept("INSIGHT", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHT", "INSIGHTID", "ENGINEID", "VARCHAR(255)", null);
		owler.addProp("INSIGHT", "INSIGHTID", "INSIGHTNAME", "VARCHAR(255)", null);
		owler.addProp("INSIGHT", "INSIGHTID", "GLOBAL", "BOOLEAN", null);
		owler.addProp("INSIGHT", "INSIGHTID", "EXECUTIONCOUNT", "BIGINT", null);
		owler.addProp("INSIGHT", "INSIGHTID", "CREATEDON", "TIMESTAMP", null);
		owler.addProp("INSIGHT", "INSIGHTID", "LASTMODIFIEDON", "TIMESTAMP", null);
		owler.addProp("INSIGHT", "INSIGHTID", "LAYOUT", "VARCHAR(255)", null);
		owler.addProp("INSIGHT", "INSIGHTID", "CACHEABLE", "BOOLEAN", null);

		// USERINSIGHTPERMISSION
		owler.addConcept("USERINSIGHTPERMISSION", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "INSIGHTID", "USERID", "VARCHAR(255)", null);
		owler.addProp("USERINSIGHTPERMISSION", "INSIGHTID", "ENGINEID", "VARCHAR(255)", null);
		owler.addProp("USERINSIGHTPERMISSION", "INSIGHTID", "PERMISSION", "INT", null);

		// USER
		owler.addConcept("USER", "ID", "VARCHAR(255)");
		owler.addProp("USER", "ID", "NAME", "VARCHAR(255)", null);
		owler.addProp("USER", "ID", "EMAIL", "VARCHAR(255)", null);
		owler.addProp("USER", "ID", "TYPE", "VARCHAR(255)", null);
		owler.addProp("USER", "ID", "PASSWORD", "VARCHAR(255)", null);
		owler.addProp("USER", "ID", "SALT", "VARCHAR(255)", null);
		owler.addProp("USER", "ID", "USERNAME", "VARCHAR(255)", null);
		owler.addProp("USER", "ID", "ADMIN", "BOOLEAN", null);
		owler.addProp("USER", "ID", "PUBLISHER", "BOOLEAN", null);

		// PERMISSION
		owler.addConcept("PERMISSION", "ID", "INT");
		owler.addProp("PERMISSION", "ID", "NAME", "VARCHAR(255)", null);
		
		// joins
		owler.addRelation("ENGINE", "ENGINEID", "ENGINEMETA", "ENGINEID", "ENGINE.ENGINEID.ENGINEMETA.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEID", "ENGINEPERMISSION", "ENGINEID", "ENGINE.ENGINEID.ENGINEPERMISSION.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEID", "WORKSPACEENGINE", "ENGINEID", "ENGINE.ENGINEID.WORKSPACEENGINE.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEID", "ASSETENGINE", "ENGINEID", "ENGINE.ENGINEID.ASSETENGINE.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEID", "INSIGHT", "INSIGHTID", "ENGINE.ENGINEID.INSIGHT.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEID", "USERINSIGHTPERMISSION", "INSIGHTID", "ENGINE.ENGINEID.USERINSIGHTPERMISSION.ENGINEID");

		owler.addRelation("INSIGHT", "INSIGHTID", "USERINSIGHTPERMISSION", "INSIGHTID", "INSIGHT.INSIGHTID.USERINSIGHTPERMISSION.INSIGHTID");
		
		owler.addRelation("USER", "ID", "USERINSIGHTPERMISSION", "INSIGHTID", "USER.ID.USERINSIGHTPERMISSION.USERID");
		owler.addRelation("USER", "ID", "ENGINEPERMISSION", "ENGINEID", "USER.ID.ENGINEPERMISSION.USERID");

		owler.addRelation("ENGINEPERMISSION", "ENGINEID", "PERMISSION", "ID", "ENGINEPERMISSION.PERMISSION.PERMISSION.ID");
		owler.addRelation("USERINSIGHTPERMISSION", "INSIGHTID", "PERMISSION", "ID", "ENGINEPERMISSION.PERMISSION.PERMISSION.ID");

		owler.commit();
		owler.export();
	}
	
}
