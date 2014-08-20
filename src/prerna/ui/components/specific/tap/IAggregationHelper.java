package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;

public interface IAggregationHelper {

	String semossConceptBaseURI = "http://semoss.org/ontologies/Concept/";
	String semossRelationBaseURI = "http://semoss.org/ontologies/Relation/";
	String semossPropertyBaseURI = "http://semoss.org/ontologies/Relation/Contains/";
	
	HashMap<String, HashMap<String, Object>> dataHash = new HashMap<String, HashMap<String, Object>>();
	HashMap<String, HashMap<String, Object>> removeDataHash = new HashMap<String, HashMap<String, Object>>();

	public HashMap<String, Set<String>> allRelations = new HashMap<String, Set<String>>();
	public HashMap<String, Set<String>> allConcepts = new HashMap<String, Set<String>>();
	
	// processing and aggregating methods
	
	public void processData(IEngine engine, HashMap<String, HashMap<String, Object>> data);
	
	public void deleteData(IEngine engine, HashMap<String, HashMap<String, Object>> data);
	
	public void processNewConcepts(IEngine engine, String newConceptType);
	
	public void processNewRelationships(IEngine engine, String newRelationshipType);
	
	public void processNewConceptsAtInstanceLevel(IEngine engine, String subject, String object);
	
	public void processNewRelationshipsAtInstanceLevel(IEngine engine, String subject, String object); 
	
	public void addToDataHash(Object[] returnTriple);
	
	public void addToDeleteHash(Object[] returnTriple);
	
	public void addToAllConcepts(String uri);
	
	public void addToAllRelationships(String uri);
	
	public void writeToOWL(IEngine engine);
	
	// utility methods
	public Object[] processSumValues(String sub, String prop, Object value);
	
	public Object[] processConcatString(String sub, String prop, Object value); 
	
	public Object[] processMaxMinDouble(String sub, String prop, Object value, boolean max);
	
	public Object[] processMinMaxDate(String sub, String prop, Object value, Boolean latest);
	



	

}
