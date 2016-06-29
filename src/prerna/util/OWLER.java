package prerna.util;

import java.io.IOException;
import java.util.Hashtable;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IEngine;
import prerna.poi.main.BaseDatabaseCreator;

public class OWLER {

	private IEngine.ENGINE_TYPE type = null;
	
	public static String BASE_URI = "http://semoss.org/ontologies/";
	public static final String DEFAULT_NODE_CLASS = "Concept";
	public static final String DEFAULT_RELATION_CLASS = "Relation";
	public static final String DEFAULT_PROP_CLASS = "Relation/Contains";
	public static final String SUBPROPERTY_URI = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
	public static final String SUBCLASS_URI = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
	public static final String CLASS_URI = "http://www.w3.org/2000/01/rdf-schema#Class";
	public static final String DEFAULT_PROPERTY_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	public static final String SEMOSS_URI = BASE_URI;
	public static final String CLASS = "_CLASS";
	public static final String TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public static final String DEFAULT_PROPERTY_CLASS = "Relation/Contains";
	public static final String PROP_DATATYPE_PREDICATE = OWL.DatatypeProperty.toString();
	public static final String DATATYPE_PREDICATE = RDFS.CLASS.toString();
	
	public static String CONCEPT_URI = BASE_URI + DEFAULT_NODE_CLASS + "/";
	public static String RELATION_URI =  BASE_URI + DEFAULT_PROPERTY_CLASS + "/";
	
	// hashtable of concepts
	private Hashtable<String, String> conceptHash = new Hashtable<String, String>();
	// hashtable of relationships
	private Hashtable<String, String> relationHash = new Hashtable<String, String>();
	// hashtable of properties
	private Hashtable<String, String> propHash = new Hashtable<String, String>();

	public boolean addBase = false;
	protected BaseDatabaseCreator engine = null;
	protected String fileName = null;
	
	// I need something here to add to the base relationship
	// i.e. semoss/concept is a type of class
	
	public OWLER(String fileName, IEngine.ENGINE_TYPE type) {
		this.fileName = fileName;
		this.type = type;
		
		engine = new BaseDatabaseCreator(fileName);
		String baseSubject = BASE_URI + DEFAULT_NODE_CLASS ;
		String predicate = RDF.TYPE.stringValue();
		
		String baseRelation = BASE_URI + DEFAULT_RELATION_CLASS;

		engine.addToBaseEngine(baseSubject, predicate, CLASS_URI);
		engine.addToBaseEngine(baseRelation, predicate, DEFAULT_PROPERTY_URI);
	}
	
	public OWLER(IEngine existingEngine, String fileName, IEngine.ENGINE_TYPE type) {
		this.fileName = fileName;
		this.type = type;
		engine = new BaseDatabaseCreator(existingEngine, fileName);
	}
	
	public void closeOwl() {
		engine.closeBaseEng();
	}
	
	public void commit() {
		engine.commit();
	}
	
	public void export() throws IOException {
		engine.exportBaseEng(true);
	}
	
	public BaseDatabaseCreator getEngine() {
		return engine;
	}

	public void setEngine(BaseDatabaseCreator engine) {
		this.engine = engine;
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	// a class that has been LONG pending
	// add concepts / tables
	// add relations / foreign keys
	// add properties
	// finally get the owl as a String
	// woo hoo how exciting
		
	// adds a concept to the owl
	/*
	 * STEP 1
	 * <rdf:Description rdf:about="http://semoss.org/ontologies/Concept/Director/Director">
	<rdfs:subClassOf rdf:resource="http://semoss.org/ontologies/Concept"/>
	 *STEP 2
	 and then concept is a type of class
	 
	 */
	
	public String addConcept(String tableName, String colName, String baseURI, String dataType) {
		if(!conceptHash.containsKey(tableName + colName)) {
			// add this to the base URI
			// make this as a type of semoss class
			// make the class as a subclass of RDF Class
			String object = baseURI + DEFAULT_NODE_CLASS ;
			
			// get the subject
			String subject = object;
			// if it is rdbms, we need to account when the tablen ame and col name are not the same
			if(type.equals(IEngine.ENGINE_TYPE.RDBMS)) {
				// if the col is null or empty, assume table name and col name are the same
				if(colName == null || colName.isEmpty()) {
					subject += tableName + "/" + tableName;
				} else {
					subject += colName + "/" + tableName;
				}
			// here, it must be RDF
			// just add the table name
			} else {
				subject += "/" + tableName;
			}
			
			String typeObject = "TYPE:" + dataType;
			// all concepts in a rdf database are strings as they are uri's
			engine.addToBaseEngine(subject, DATATYPE_PREDICATE, typeObject);
			
			// and I will store this statement
			// STEP 1
			String predicate = RDFS.SUBCLASSOF.stringValue();
			engine.addToBaseEngine(subject, predicate, object);

			conceptHash.put(tableName + colName, subject);

			// STEP 2
			// also add this as base
			// I will handle this as a separate call at the end for now
		}		
		return conceptHash.get(tableName + colName);
	}
	
	public String addConcept(String concept, String col, String dataType) {
		return addConcept(concept, col, SEMOSS_URI, dataType);
	}
	
	public String addConcept(String concept, String dataType) {
		return addConcept(concept, "", SEMOSS_URI, dataType);
	}
	
	public String addConcept(String concept) {
		return addConcept(concept, "", SEMOSS_URI, "STRING");
	}
	
	// creates a relation
	// I need to also put where the affinity is etc.
	/* STEP 1
	 * <rdf:Description rdf:about="http://semoss.org/ontologies/Relation/Title.Title.Nominated.Title_FK">
	<rdfs:subPropertyOf rdf:resource="http://semoss.org/ontologies/Relation"/>
	
	*  STEP 2
	*	<rdf:Description rdf:about="http://semoss.org/ontologies/Concept/Title/Title">
		<Title.Title.Director.Title_FK xmlns="http://semoss.org/ontologies/Relation/" rdf:resource="http://semoss.org/ontologies/Concept/Director/Director"/>

	 */
	
	public String addRelation(String fromConcept, String toConcept, String predicate)
	{
		if(!relationHash.containsKey(fromConcept + toConcept + predicate)) {
			// the from is always where the foreign key is if predicate is null
			
			// add to the base URI
			// make this as subproperty of semoss relation
			// make this relation subproperty of RDF Relation
			String fromConceptURI = addConcept(fromConcept);
			String toConceptURI = addConcept(toConcept);
			
			if(predicate == null)
			{
				// create predicate
				// I will by default assume this is for SQL
				// * sheesh *
				predicate = fromConcept + "." + fromConcept + "." + toConcept + "." + fromConcept + "_FK";
			}
			
			String predicateObject = SEMOSS_URI + "Relation";
			String predicateSubject = predicateObject + "/" + predicate;
			String predicatePredicate = RDFS.SUBPROPERTYOF.stringValue();
			
			//STEP 1
			engine.addToBaseEngine(predicateSubject, predicatePredicate, predicateObject);
			
			// STEP 2
			engine.addToBaseEngine(fromConceptURI, predicateSubject, toConceptURI); // add the base relation as well
			
			relationHash.put(fromConcept + toConcept + predicate, predicateSubject);
		}
		
		return relationHash.get(fromConcept + toConcept + predicate);
	}
	
	public void addSubclass(String childType, String parentType) {
		String childURI = addConcept(childType);
		String parentURI = addConcept(parentType);
		engine.addToBaseEngine(childURI, SUBCLASS_URI, parentURI);
	}
	
	public void addCustomBaseURI(String customBaseURI) {
		engine.addToBaseEngine("SEMOSS:ENGINE_METADATA", "CONTAINS:BASE_URI", customBaseURI+"/"+DEFAULT_NODE_CLASS+"/");
	}
	
	public String getOwlAsString()
	{
		String owl = null;
		
		try {
			owl = engine.exportBaseEngAsString(true);
			System.out.println("OWL.. " + owl);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return owl;
	}

	// creates a property
	
	/*
	 * Obviously if the concept is not there create it
	 * 
	 * STEP 1
	 * <Contains xmlns="http://semoss.org/ontologies/Relation/" rdf:about="http://semoss.org/ontologies/Relation/Contains/RottenTomatoes_Critics"/>
	 * 
	 * STEP 2
	 * <rdf:Description rdf:about="http://semoss.org/ontologies/Concept/Title/Title">
	<DatatypeProperty xmlns="http://www.w3.org/2002/07/owl#" rdf:resource="http://semoss.org/ontologies/Relation/Contains/RottenTomatoes_Critics"/>
	*
	*  STEP 3 ?
	*  <rdf:Description rdf:about="http://semoss.org/ontologies/Relation/Contains/">
	<rdfs:subPropertyOf rdf:resource="http://semoss.org/ontologies/Relation/Contains/"/>

	*
	 * 
	 */
	public String addProp(String concept, String property, String type)
	{
		if(!propHash.containsKey(concept + "%" + property)) {
			// same as concepts
			String propSubject = addConcept(concept);
			
			// STEP 1
			String propMaster = SEMOSS_URI + DEFAULT_PROP_CLASS;
			String propObject = propMaster + "/" + property;
			
			engine.addToBaseEngine(propObject, RDF.TYPE.stringValue(), propMaster);
			
			// STEP 2
			engine.addToBaseEngine(propSubject, PROP_DATATYPE_PREDICATE, propObject);
			
			// STEP 3 ?
			// I need some way to also add the types to this
			String typeObject = "TYPE:" + type;
			
			engine.addToBaseEngine(propObject, DATATYPE_PREDICATE, typeObject);
			
			propHash.put(concept + "%" + property, propObject);
		}
		
		return propHash.get(concept + "%" + property);
	}
	
	public String addProp(String concept, String conceptCol, String property, String type)
	{
		if(!propHash.containsKey(concept + "%" + property)) {
			// same as concepts
			String propSubject = addConcept(concept, conceptCol, null);
			
			// STEP 1
			String propMaster = SEMOSS_URI + DEFAULT_PROP_CLASS;
			String propObject = propMaster + "/" + property;
			
			engine.addToBaseEngine(propObject, RDF.TYPE.stringValue(), propMaster);
			
			// STEP 2
			engine.addToBaseEngine(propSubject, PROP_DATATYPE_PREDICATE, propObject);
			
			// STEP 3 ?
			// I need some way to also add the types to this
			String typeObject = "TYPE:" + type;
			
			engine.addToBaseEngine(propObject, DATATYPE_PREDICATE, typeObject);
			
			propHash.put(concept + "%" + property, propObject);
		}
		
		return propHash.get(concept + "%" + property);
	}
	
	public Hashtable<String, String> getConceptHash() {
		return conceptHash;
	}

	public void setConceptHash(Hashtable<String, String> conceptHash) {
		this.conceptHash = conceptHash;
	}

	public Hashtable<String, String> getRelationHash() {
		return relationHash;
	}

	public void setRelationHash(Hashtable<String, String> relationHash) {
		this.relationHash = relationHash;
	}

	public Hashtable<String, String> getPropHash() {
		return propHash;
	}

	public void setPropHash(Hashtable<String, String> propHash) {
		this.propHash = propHash;
	}
	
	public static void main(String [] args)
	{
		OWLER owler = new OWLER("C:\\Users\\pkapaleeswaran\\workspacej3\\owler\\test.owl", IEngine.ENGINE_TYPE.RDBMS);
		owler.addRelation("Studio", "Title", null);
		owler.addProp("Title", "Budget", "Int");
		owler.getOwlAsString();
	}
}
