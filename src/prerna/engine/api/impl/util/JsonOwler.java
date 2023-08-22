package prerna.engine.api.impl.util;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IDatabaseEngine;
import prerna.util.Utility;

public class JsonOwler extends AbstractOwler {

	public static final String JSON_SELECTOR_RELATION_NAME = "JsonSelector";
	public static final String JSON_SELECTOR_RELATION_URI = BASE_RELATION_URI + "/" + JSON_SELECTOR_RELATION_NAME;

	/**
	 * Constructor for the class when we are creating a brand new OWL file
	 * @param fileName				The location of the new OWL file
	 * @param type					The type of the engine the OWL file is being created for
	 */
	public JsonOwler(String owlPath, IDatabaseEngine.DATABASE_TYPE type) {
		super(owlPath, type);
	}
	
	/**
	 * Constructor for the class when we are adding to an existing OWL file
	 * @param existingEngine		The engine we are adding to
	 */
	public JsonOwler(IDatabaseEngine existingEngine) {
		super(existingEngine);
	}
	
	/**
	 * Right now, I really only envision a JSON engine as being a flat table
	 * 
	 * {@link #OWLER.addConcept(String tableName, String conceptual)} really has no meaning
	 * it is just a way so that we have a general table name appear when looking at the structure
	 * 
	 */
	
	/**
	 * Add a concept for the json engine
	 * There should really only be 1 concept and it is a fake node
	 * It is just a reference point for the UI, however, it is not related to any column
	 * @param tableName
	 * @param conceptual
	 * @return
	 */
	public String addConcept(String tableName, String conceptual) {
		if(!conceptHash.containsKey(tableName)) {
			/*
			 * We are doing three things here
			 * 
			 * 1) add the concept as a subclass of http://semoss.org/ontologies/Concept
			 * 2) add the conceptual uri
			 * 3) add the data type of the concept (really don't need this since its a fake concept...)
			 */
			
			// this is #1
			String subject = BASE_NODE_URI + "/" + tableName;
			// add the concept as a subclass of concept
			engine.addToBaseEngine(subject, RDFS.SUBCLASSOF.stringValue(), BASE_NODE_URI);
			
			// this is #2
			// add the conceptual uri
			String conceptualNode = conceptual;
			if(conceptualNode == null) {
				conceptualNode = Utility.cleanVariableString(tableName);
			}
			String conceptualSubject = BASE_NODE_URI + "/" + conceptualNode;
			engine.addToBaseEngine(subject, CONCEPTUAL_RELATION_URI, conceptualSubject);

			// this is #3
			// i think i have to add a data type for proper sync into the local master....
			String typeObject = "TYPE:STRING";
			engine.addToBaseEngine(subject, RDFS.CLASS.stringValue(), typeObject);
			
			// store it in the hash for future use
			conceptHash.put(tableName, subject);
		}
		return conceptHash.get(tableName);
	}
	
	/**
	 * Add a property to the JSON owl including the jsonPath selection portion
	 * @param tableName
	 * @param propertyCol
	 * @param dataType
	 * @param jsonPath
	 * @param conceptual
	 * @return
	 */
	public String addProperty(String tableName, String propertyCol, String dataType, String jsonPath, String conceptual) {
		if(!propHash.containsKey(tableName + "%" + propertyCol)) {
			/*
			 * We are doing a few things here
			 * 
			 * 1) add the property as a property
			 * 2) add the property to the table
			 * 3) add the type of the property
			 * 4) add the JSON path of the property
			 * 5) add the conceptual uri
			 */
			
			// this is #1
			String propertyUri = BASE_PROPERTY_URI + "/" + propertyCol + "/" + tableName;
			engine.addToBaseEngine(propertyUri, RDF.TYPE.stringValue(), BASE_PROPERTY_URI);

			// this is #2
			String tableUri = BASE_NODE_URI + "/" + tableName;
			engine.addToBaseEngine(tableUri, OWL.DatatypeProperty.toString(), propertyUri);

			// this is #3
			String typeObject = "TYPE:" + dataType;
			engine.addToBaseEngine(propertyUri, RDFS.CLASS.stringValue(), typeObject);
			
			// this is #4
			engine.addToBaseEngine(propertyUri, JSON_SELECTOR_RELATION_URI, jsonPath, false);

			// this is #5
			String conceptualProp = conceptual;
			if(conceptualProp == null) {
				conceptualProp = Utility.cleanVariableString(propertyCol);
			}
			// TODO: this is making the assumption about the table conceptual name!!!! need to account for this
			// this is also present in normal OWLER.java
			String conceptualPropertyName = conceptualProp + "/" +  Utility.cleanVariableString(tableName);
			String conceptualProperty = BASE_PROPERTY_URI + "/" + conceptualPropertyName;
			engine.addToBaseEngine(propertyUri, CONCEPTUAL_RELATION_URI, conceptualProperty);
			
			propHash.put(tableName + "%" + propertyCol, propertyUri);
		}
		return propHash.get(tableName + "%" + propertyCol);
	}
	
	public static void main(String[] args) {
		
		
		
	}
}
