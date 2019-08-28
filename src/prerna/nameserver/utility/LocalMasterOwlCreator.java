package prerna.nameserver.utility;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;

public class LocalMasterOwlCreator {

	static private List<String> conceptsRequired = new Vector<String>();
	static {
		conceptsRequired.add("BITLY");
		conceptsRequired.add("CONCEPT");
		conceptsRequired.add("CONCEPTMETADATA");
		conceptsRequired.add("ENGINE");
		conceptsRequired.add("ENGINECONCEPT");
		conceptsRequired.add("ENGINERELATION");
		conceptsRequired.add("KVSTORE");
		conceptsRequired.add("RELATION");
	}
	
	private IEngine localMasterDb;
	
	public LocalMasterOwlCreator(IEngine localMasterDb) {
		this.localMasterDb = localMasterDb;
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
		List<String> concepts = localMasterDb.getPhysicalConcepts();
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
		RDFFileSesameEngine baseEngine = localMasterDb.getBaseDataEngine();
		if(baseEngine != null) {
			baseEngine.closeDB();
		}
		
		// now delete the file if exists
		// and we will make a new
		String owlLocation = localMasterDb.getOWL();
		
		File f = new File(owlLocation);
		if(f.exists()) {
			f.delete();
		}
		
		// write the new OWL
		writeNewOwl(owlLocation);
		
		// now reload into security db
		localMasterDb.setOWL(owlLocation);
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws IOException
	 */
	private void writeNewOwl(String owlLocation) throws IOException {
		Owler owler = new Owler(owlLocation, ENGINE_TYPE.RDBMS);

		// BITLY
		owler.addConcept("BITLY", null, null);
		owler.addProp("BITLY", "EMBED", "VARCHAR(20000)");
		owler.addProp("BITLY", "FANCY", "VARCHAR(800)");
		
		// CONCEPT
		owler.addConcept("CONCEPT", null, null);
		owler.addProp("CONCEPT", "LOCALCONCEPTID", "VARCHAR(255)");
		owler.addProp("CONCEPT", "CONCEPTUALNAME", "VARCHAR(255)");
		owler.addProp("CONCEPT", "LOGICALNAME", "VARCHAR(255)");
		owler.addProp("CONCEPT", "DOMAINNAME", "VARCHAR(255)");
		owler.addProp("CONCEPT", "GLOBALID", "VARCHAR(255)");

		// CONCEPTMETADATA
		owler.addConcept("CONCEPTMETADATA", null, null);
		owler.addProp("CONCEPTMETADATA", "PHYSICALNAMEID", "VARCHAR(255)");
		owler.addProp("CONCEPTMETADATA", "KEY", "VARCHAR(255)");
		owler.addProp("CONCEPTMETADATA", "VALUE", "VARCHAR(255)");

		// ENGINE
		owler.addConcept("ENGINE", null, null);
		owler.addProp("ENGINE", "ID", "VARCHAR(255)");
		owler.addProp("ENGINE", "ENGINENAME", "VARCHAR(255)");
		owler.addProp("ENGINE", "MODIFIEDDATE", "TIMESTAMP");
		owler.addProp("ENGINE", "TYPE", "VARCHAR(255)");

		// ENGINECONCEPT
		owler.addConcept("ENGINECONCEPT", null, null);
		owler.addProp("ENGINECONCEPT", "ENGINE", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PARENTSEMOSSNAME", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "SEMOSSNAME", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PARENTPHYSICALNAME", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PARENTPHYSICALNAMEID", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PHYSICALNAME", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PHYSICALNAMEID", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PARENTLOCALCONCEPTID", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PK", "BOOLEAN");
		owler.addProp("ENGINECONCEPT", "IGNORE_DATA", "BOOLEAN");
		owler.addProp("ENGINECONCEPT", "ORIGINAL_TYPE", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "PROPERTY_TYPE", "VARCHAR(255)");
		owler.addProp("ENGINECONCEPT", "ADDITIONAL_TYPE", "VARCHAR(255)");
		
		// ENGINERELATION
		owler.addConcept("ENGINERELATION", null, null);
		owler.addProp("ENGINERELATION", "RELATIONID", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "ENGINE", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "INSTANCERELATIONID", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "SOURCECONCEPTID", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "TARGETCONCEPTID", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "SOURCEPROPERTY", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "TARGETPROPERTY", "VARCHAR(255)");
		owler.addProp("ENGINERELATION", "RELATIONNAME", "VARCHAR(255)");

		// KVSTORE
		owler.addConcept("KVSTORE", null, null);
		owler.addProp("KVSTORE", "K", "VARCHAR(800)");
		owler.addProp("KVSTORE", "V", "VARCHAR(800)");

		// RELATION
		owler.addConcept("RELATION", null, null);
		owler.addProp("RELATION", "ID", "VARCHAR(255)");
		owler.addProp("RELATION", "SOURCEID", "VARCHAR(255)");
		owler.addProp("RELATION", "TARGETID", "VARCHAR(255)");
		owler.addProp("RELATION", "GLOBALID", "VARCHAR(255)");

		// joins
		owler.addRelation("ENGINE", "ENGINECONCEPT", "ENGINE.ID.ENGINECONCEPT.ENGINE");
		owler.addRelation("ENGINE", "ENGINERELATION", "ENGINE.ID.ENGINERELATION.ENGINE");

		owler.addRelation("ENGINECONCEPT", "CONCEPT", "ENGINECONCEPT.LOCALCONCEPTID.CONCEPT.LOCALCONCEPTID");
		owler.addRelation("ENGINECONCEPT", "ENGINERELATION", "ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.SOURCECONCEPTID");
		owler.addRelation("ENGINECONCEPT", "ENGINERELATION", "ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.TARGETCONCEPTID");

		owler.addRelation("ENGINERELATION", "RELATION", "ENGINERELATION.RELATIONID.RELATION.ID");

		owler.addRelation("CONCEPT", "RELATION", "CONCEPT.LOCALCONCEPTID.RELATION.SOURCEID");
		owler.addRelation("CONCEPT", "RELATION", "CONCEPT.LOCALCONCEPTID.RELATION.TARGETID");
		
		owler.commit();
		owler.export();
	}
	
}
