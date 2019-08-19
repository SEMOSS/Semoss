package prerna.nameserver.utility;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Owler;
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
		List<String> concepts = localMasterDb.getConcepts(true);
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
		owler.addConcept("BITLY", "EMBED", "VARCHAR(20000)");
		owler.addProp("BITLY", "EMBED", "FANCY", "VARCHAR(800)", null);
		
		// CONCEPT
		owler.addConcept("CONCEPT", "LOCALCONCEPTID", "VARCHAR(800)");
		owler.addProp("CONCEPT", "LOCALCONCEPTID", "CONCEPTUALNAME", "VARCHAR(800)", null);
		owler.addProp("CONCEPT", "LOCALCONCEPTID", "LOGICALNAME", "VARCHAR(800)", null);
		owler.addProp("CONCEPT", "LOCALCONCEPTID", "DOMAINNAME", "VARCHAR(800)", null);
		owler.addProp("CONCEPT", "LOCALCONCEPTID", "GLOBALID", "VARCHAR(800)", null);

		// CONCEPTMETADATA
		owler.addConcept("CONCEPTMETADATA", "PHYSICALNAMEID", "VARCHAR(800)");
		owler.addProp("CONCEPTMETADATA", "PHYSICALNAMEID", "KEY", "VARCHAR(800)", null);
		owler.addProp("CONCEPTMETADATA", "PHYSICALNAMEID", "VALUE", "VARCHAR(800)", null);

		// ENGINE
		owler.addConcept("ENGINE", "ID", "VARCHAR(800)");
		owler.addProp("ENGINE", "ID", "ENGINENAME", "VARCHAR(800)", null);
		owler.addProp("ENGINE", "ID", "MODIFIEDDATE", "TIMESTAMP", null);
		owler.addProp("ENGINE", "ID", "TYPE", "VARCHAR(800)", null);

		// ENGINECONCEPT
		owler.addConcept("ENGINECONCEPT", "LOCALCONCEPTID", "VARCHAR(800)");
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "ENGINE", "VARCHAR(800)", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "PHYSICALNAME", "VARCHAR(800)", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "PARENTPHYSICALID", "VARCHAR(800)", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "PHYSICALNAMEID", "VARCHAR(800)", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "PK", "BOOLEAN", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "PROPERTY", "BOOLEAN", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "ORIGINAL_TYPE", "VARCHAR(800)", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "PROPERTY_TYPE", "VARCHAR(800)", null);
		owler.addProp("ENGINECONCEPT", "LOCALCONCEPTID", "ADDITIONAL_TYPE", "VARCHAR(800)", null);
		
		// ENGINERELATION
		owler.addConcept("ENGINERELATION", "RELATIONID", "VARCHAR(800)");
		owler.addProp("ENGINERELATION", "RELATIONID", "ENGINE", "VARCHAR(800)", null);
		owler.addProp("ENGINERELATION", "RELATIONID", "INSTANCERELATIONID", "VARCHAR(800)", null);
		owler.addProp("ENGINERELATION", "RELATIONID", "SOURCECONCEPTID", "VARCHAR(800)", null);
		owler.addProp("ENGINERELATION", "RELATIONID", "TARGETCONCEPTID", "VARCHAR(800)", null);
		owler.addProp("ENGINERELATION", "RELATIONID", "SOURCEPROPERTY", "VARCHAR(800)", null);
		owler.addProp("ENGINERELATION", "RELATIONID", "TARGETPROPERTY", "VARCHAR(800)", null);
		owler.addProp("ENGINERELATION", "RELATIONID", "RELATIONNAME", "VARCHAR(800)", null);

		// KVSTORE
		owler.addConcept("KVSTORE", "K", "VARCHAR(800)");
		owler.addProp("KVSTORE", "K", "V", "VARCHAR(800)", null);

		// RELATION
		owler.addConcept("RELATION", "ID", "VARCHAR(800)");
		owler.addProp("RELATION", "ID", "SOURCEID", "VARCHAR(800)", null);
		owler.addProp("RELATION", "ID", "TARGETID", "VARCHAR(800)", null);
		owler.addProp("RELATION", "ID", "GLOBALID", "VARCHAR(800)", null);

		// joins
		owler.addRelation("ENGINE", "ID", "ENGINECONCEPT", "ENGINE", "ENGINE.ID.ENGINECONCEPT.ENGINE");
		owler.addRelation("ENGINE", "ID", "ENGINERELATION", "ENGINE", "ENGINE.ID.ENGINERELATION.ENGINE");

		owler.addRelation("ENGINECONCEPT", "LOCALCONCEPTID", "CONCEPT", "LOCALCONCEPTID", "ENGINECONCEPT.LOCALCONCEPTID.CONCEPT.LOCALCONCEPTID");
		owler.addRelation("ENGINECONCEPT", "LOCALCONCEPTID", "ENGINERELATION", "RELATIONID", "ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.SOURCECONCEPTID");
		owler.addRelation("ENGINECONCEPT", "LOCALCONCEPTID", "ENGINERELATION", "RELATIONID", "ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.TARGETCONCEPTID");

		owler.addRelation("ENGINERELATION", "RELATIONID", "RELATION", "ID", "ENGINERELATION.RELATIONID.RELATION.ID");

		owler.addRelation("CONCEPT", "LOCALCONCEPTID", "RELATION", "ID", "CONCEPT.LOCALCONCEPTID.RELATION.SOURCEID");
		owler.addRelation("CONCEPT", "LOCALCONCEPTID", "RELATION", "ID", "CONCEPT.LOCALCONCEPTID.RELATION.TARGETID");
		
		owler.commit();
		owler.export();
	}
	
}
