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
		List<String> concepts = securityDb.getPhysicalConcepts();
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
		owler.addProp("ENGINEMETA", "KEY", "VARCHAR(255)");
		owler.addProp("ENGINEMETA", "VALUE", "VARCHAR(255)");

		// ENGINEPERMISSION
		owler.addConcept("ENGINEPERMISSION", null, null);
		owler.addProp("ENGINEPERMISSION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "USERID", "VARCHAR(255)");
		owler.addProp("ENGINEPERMISSION", "PERMISSION", "INT");
		owler.addProp("ENGINEPERMISSION", "VISIBILITY", "BOOLEAN");

		// WORKSPACEENGINE
		owler.addConcept("WORKSPACEENGINE", null, null);
		owler.addProp("WORKSPACEENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("WORKSPACEENGINE", "USERID", "VARCHAR(255)");
		owler.addProp("WORKSPACEENGINE", "TYPE", "VARCHAR(255)");
		
		// ASSETENGINE
		owler.addConcept("ASSETENGINE", null, null);
		owler.addProp("ASSETENGINE", "ENGINEID", "VARCHAR(255)");
		owler.addProp("ASSETENGINE", "USERID", "VARCHAR(255)");
		owler.addProp("ASSETENGINE", "TYPE", "VARCHAR(255)");
		
		// INSIGHT
		owler.addConcept("INSIGHT", null, null);
		owler.addProp("INSIGHT", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHT", "ENGINEID", "VARCHAR(255)");
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
		owler.addProp("USERINSIGHTPERMISSION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("USERINSIGHTPERMISSION", "PERMISSION", "INT");

		// INSIGHTMETA
		owler.addConcept("INSIGHTMETA", null, null);
		owler.addProp("INSIGHTMETA", "INSIGHTID", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "ENGINEID", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "METAKEY", "VARCHAR(255)");
		owler.addProp("INSIGHTMETA", "METAVALUE", "CLOB");
		owler.addProp("INSIGHTMETA", "METAORDER", "INT");
		
		// USER
		owler.addConcept("USER", null, null);
		owler.addProp("USER", "ID", "VARCHAR(255)");
		owler.addProp("USER", "NAME", "VARCHAR(255)");
		owler.addProp("USER", "EMAIL", "VARCHAR(255)");
		owler.addProp("USER", "TYPE", "VARCHAR(255)");
		owler.addProp("USER", "PASSWORD", "VARCHAR(255)");
		owler.addProp("USER", "SALT", "VARCHAR(255)");
		owler.addProp("USER", "USERNAME", "VARCHAR(255)");
		owler.addProp("USER", "ADMIN", "BOOLEAN");
		owler.addProp("USER", "PUBLISHER", "BOOLEAN");

		// PERMISSION
		owler.addConcept("PERMISSION", null, null);
		owler.addProp("PERMISSION", "ID", "INT");
		owler.addProp("PERMISSION", "NAME", "VARCHAR(255)");
		
		// joins
		owler.addRelation("ENGINE", "ENGINEMETA", "ENGINE.ENGINEID.ENGINEMETA.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEPERMISSION", "ENGINE.ENGINEID.ENGINEPERMISSION.ENGINEID");
		owler.addRelation("ENGINE", "WORKSPACEENGINE", "ENGINE.ENGINEID.WORKSPACEENGINE.ENGINEID");
		owler.addRelation("ENGINE", "ASSETENGINE", "ENGINE.ENGINEID.ASSETENGINE.ENGINEID");
		owler.addRelation("ENGINE", "INSIGHT", "ENGINE.ENGINEID.INSIGHT.ENGINEID");
		owler.addRelation("ENGINE", "USERINSIGHTPERMISSION", "ENGINE.ENGINEID.USERINSIGHTPERMISSION.ENGINEID");

		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.INSIGHTID.USERINSIGHTPERMISSION.INSIGHTID");
		
		owler.addRelation("USER", "USERINSIGHTPERMISSION", "USER.ID.USERINSIGHTPERMISSION.USERID");
		owler.addRelation("USER", "ENGINEPERMISSION", "USER.ID.ENGINEPERMISSION.USERID");

		owler.addRelation("ENGINEPERMISSION", "PERMISSION", "ENGINEPERMISSION.PERMISSION.PERMISSION.ID");
		owler.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "USERINSIGHTPERMISSION.PERMISSION.PERMISSION.ID");

		owler.addRelation("INSIGHT", "INSIGHTMETA", "INSIGHT.INSIGHTID.INSIGHTMETA.INSIGHTID");
		owler.addRelation("INSIGHT", "INSIGHTMETA", "INSIGHT.ENGINEID.INSIGHTMETA.ENGINEID");

		owler.commit();
		owler.export();
	}
	
}
