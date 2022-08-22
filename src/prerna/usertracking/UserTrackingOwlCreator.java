package prerna.usertracking;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang3.tuple.Pair;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;

public class UserTrackingOwlCreator {
	
	private List<Pair<String, String>> columnNamesAndTypes = Arrays.asList(
			Pair.of("SESSIONID", "VARCHAR(255)"),
			Pair.of("USERID", "VARCHAR(255)"),
			Pair.of("TYPE", "VARCHAR(255)"),
			Pair.of("CREATED_ON", "TIMESTAMP"),
			Pair.of("ENDED_ON", "TIMESTAMP"),
			Pair.of("IP_ADDR", "VARCHAR(255)"),
			Pair.of("IP_LAT", "VARCHAR(255)"),
			Pair.of("IP_LONG", "VARCHAR(255)"),
			Pair.of("IP_COUNTRY", "VARCHAR(255)"),
			Pair.of("IP_STATE", "VARCHAR(255)"),
			Pair.of("IP_CITY", "VARCHAR(255)")			
		);
	
	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new Vector<String>();
	static {
		conceptsRequired.add("USER_TRACKING");
	}
	
	private IEngine sessionDb;
	
	public UserTrackingOwlCreator(IEngine sessionDb) {
		this.sessionDb = sessionDb;
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
		List<String> concepts = sessionDb.getPhysicalConcepts();
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}
		
		boolean check1 = cleanConcepts.containsAll(conceptsRequired);
		if(check1) {
			List<String> props = sessionDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/USER_TRACKING");
			
			props = sessionDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/USER_TRACKING");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/DESCRIPTION/USER_TRACKING")) {
				return true;
			}
			
		}
		return true;
	}
		
	
	/**
	 * Remake the OWL 
	 * @throws IOException
	 */
	public void remakeOwl() throws IOException {
		// get the existing engine and close it
		RDFFileSesameEngine baseEngine = sessionDb.getBaseDataEngine();
		if(baseEngine != null) {
			baseEngine.closeDB();
		}
		
		// now delete the file if exists
		// and we will make a new
		String owlLocation = sessionDb.getOWL();
		
		File f = new File(owlLocation);
		if(f.exists()) {
			f.delete();
		}
		
		// write the new OWL
		writeNewOwl(owlLocation);
		
		// now reload into security db
		sessionDb.setOWL(owlLocation);
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws IOException
	 */
	private void writeNewOwl(String owlLocation) throws IOException {
		Owler owler = new Owler(owlLocation, ENGINE_TYPE.RDBMS);

		// ENGINE
		owler.addConcept("USER_TRACKING", null, null);
		
		for (Pair<String, String> x : columnNamesAndTypes) {	
			owler.addProp("USER_TRACKING", x.getLeft(), x.getRight());
		}
		
		owler.commit();
		owler.export();
	}
	
	public List<Pair<String, String>> getColumnNamesAndTypes() {
		return columnNamesAndTypes;
	}
	
}
