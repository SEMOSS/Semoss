package prerna.ui.components.specific.tap;

import java.util.Hashtable;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;

public interface IAggregationHelper {

	String semossConceptBaseURI = "http://semoss.org/ontologies/Concept/";
	String semossRelationBaseURI = "http://semoss.org/ontologies/Relation/";
	String semossPropertyBaseURI = "http://semoss.org/ontologies/Relation/Contains/";
	
	Hashtable<String, Hashtable<String, Object>> dataHash = new Hashtable<String, Hashtable<String, Object>>();
	Hashtable<String, Hashtable<String, Object>> removeDataHash = new Hashtable<String, Hashtable<String, Object>>();

	public Hashtable<String, Set<String>> allRelations = new Hashtable<String, Set<String>>();
	public Hashtable<String, Set<String>> allConcepts = new Hashtable<String, Set<String>>();
	
	// processing and aggregating methods
	public SesameJenaSelectWrapper processQuery(IEngine engine, String query);
	
	public void processData(IEngine engine, Hashtable<String, Hashtable<String, Object>> data);
	
	public void deleteData(IEngine engine, Hashtable<String, Hashtable<String, Object>> data);
	
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
