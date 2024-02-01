package prerna.nameserver.utility;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;

public class LocalMasterOwlCreator {

	static private List<String> conceptsRequired = new ArrayList<String>();
	static {
		conceptsRequired.add("BITLY");
		conceptsRequired.add("CONCEPT");
		conceptsRequired.add("CONCEPTMETADATA");
		conceptsRequired.add("ENGINE");
		conceptsRequired.add("ENGINECONCEPT");
		conceptsRequired.add("ENGINERELATION");
		conceptsRequired.add("KVSTORE");
		conceptsRequired.add("RELATION");
		conceptsRequired.add("METAMODELPOSITION");
	}
	
	private IDatabaseEngine localMasterDb;
	
	public LocalMasterOwlCreator(IDatabaseEngine localMasterDb) {
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
		
		List<String> cleanConcepts = new ArrayList<String>();
		List<String> concepts = localMasterDb.getPhysicalConcepts();
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
			List<String> props = localMasterDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/CONCEPTMETADATA");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/METAKEY/CONCEPTMETADATA")) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Remake the OWL 
	 * @throws Exception 
	 */
	public void remakeOwl() throws Exception {
		try(WriteOWLEngine owlEngine = localMasterDb.getOWLEngineFactory().getWriteOWL()) {
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
		owler.addProp("CONCEPTMETADATA", "METAKEY", "VARCHAR(800)");
		owler.addProp("CONCEPTMETADATA", "METAVALUE", "CLOB");

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
		
		
		owler.addConcept("METAMODELPOSITION", null, null);
		owler.addProp("METAMODELPOSITION", "ENGINEID", "VARCHAR(255)");
		owler.addProp("METAMODELPOSITION", "TABLENAME", "VARCHAR(255)");
		owler.addProp("METAMODELPOSITION", "XPOS", "FLOAT");
		owler.addProp("METAMODELPOSITION", "YPOS", "FLOAT");
		
		owler.addRelation("METAMODELPOSITION", "ENGINE", "METAMODELPOSITION.ENGINEID.ENGINE.ID");
		
		owler.commit();
		owler.export();
	}
	
}
