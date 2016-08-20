package prerna.util;

import java.io.IOException;
import java.util.Hashtable;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IEngine;
import prerna.poi.main.BaseDatabaseCreator;

public class OWLER {

	// predefined URIs
	public static String BASE_URI = "http://semoss.org/ontologies/";
	public static final String SEMOSS_URI = BASE_URI;
	public static final String DEFAULT_NODE_CLASS = "Concept";
	public static final String DEFAULT_RELATION_CLASS = "Relation";
	public static final String DEFAULT_PROP_CLASS = "Relation/Contains";
	public static final String PHYSICAL_NAME = "http://semoss.org/ontologies/physical/property";
	public static final String PHYSICAL_TABLE = "http://semoss.org/ontologies/physical/table";
	public static final String DEFAULT_PROPERTY_CLASS = "Relation/Contains";
	public static final String CONCEPTUAL_RELATION_NAME = "Conceptual";
	
//	public static final String PROP_DATATYPE_PREDICATE = OWL.DatatypeProperty.toString();
//	public static final String DATATYPE_PREDICATE = RDFS.CLASS.toString();
//	public static final String SUBPROPERTY_URI = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
//	public static final String SUBCLASS_URI = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
//	public static final String CLASS_URI = "http://www.w3.org/2000/01/rdf-schema#Class";
//	public static final String TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
//	public static final String DEFAULT_PROPERTY_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
//	public static final String CLASS = "_CLASS";
//	public static final String CONCEPT_URI = BASE_URI + DEFAULT_NODE_CLASS + "/";
//	public static final String RELATION_URI =  BASE_URI + DEFAULT_PROPERTY_CLASS + "/";
	
	
	// hashtable of concepts
	private Hashtable<String, String> conceptHash = new Hashtable<String, String>();
	// hashtable of relationships
	private Hashtable<String, String> relationHash = new Hashtable<String, String>();
	// hashtable of properties
	private Hashtable<String, String> propHash = new Hashtable<String, String>();
	// need to know the database type due to differences in URIs when the
	// database is RDF vs. RDBMS
	private IEngine.ENGINE_TYPE type = null;
	// the engine here is a wrapper around a RDFFileSesameEngine which helps with adding the URIs into the engine
	private BaseDatabaseCreator engine = null;
	// file name for the location of the OWL file to write to
	private String owlPath = null;
	
	/**
	 * Constructor for the class when we are creating a brand new OWL file
	 * @param fileName				The location of the new OWL file
	 * @param type					The type of the engine the OWL file is being created for
	 */
	public OWLER(String owlPath, IEngine.ENGINE_TYPE type) {
		this.owlPath = owlPath;
		this.type = type;
		
		engine = new BaseDatabaseCreator(owlPath);
		String baseSubject = BASE_URI + DEFAULT_NODE_CLASS ;
		String baseRelation = BASE_URI + DEFAULT_RELATION_CLASS;

		String predicate = RDF.TYPE.stringValue();

		engine.addToBaseEngine(baseSubject, predicate, RDFS.CLASS.stringValue());
		engine.addToBaseEngine(baseRelation, predicate, RDF.PROPERTY.stringValue());
	}
	
	/**
	 * Constructor for the class when we are adding to an existing OWL file
	 * @param existingEngine		The engine we are adding to
	 * @param fileName				The location of the OWL file
	 */
	public OWLER(IEngine existingEngine, String owlPath) {
		this.owlPath = owlPath;
		this.type = existingEngine.getEngineType();
		engine = new BaseDatabaseCreator(existingEngine, owlPath);
	}
	
	/**
	 * Closes the connection to the RDFFileSesameEngine supported by the OWL
	 */
	public void closeOwl() {
		engine.closeBaseEng();
	}
	
	/**
	 * Commits the modifications to the OWL file into the engine
	 */
	public void commit() {
		engine.commit();
	}
	
	/**
	 * Exports the information added into the OWL file located at the owlPath
	 * @throws IOException
	 */
	public void export() throws IOException {
		engine.exportBaseEng(true);
	}

	
	/////////////////// ADDING CONCEPTS INTO THE OWL /////////////////////////////////
	/*
	 * This method is overloaded to make it easier to add concepts into the owl file
	 * The overloading exists since some fields are not required as a result of the difference
	 * between the RDF concept URI (semoss:Concept/Concept_Name) and RDBMS concept URI (semoss:Concept/ColName/TableName)
	 * This is why we keep track of the type of engine when the OWLER is created
	 * 
	 * Lets look at what triples will be added
	 * NOTE: for simplicity, I will use 'semoss:' to denote the prefix 'http://semoss.org/ontologies'
	 * 
	 * For a RDF concept, call it 'C-X', the following triples will be added:
	 * 1) { <semoss:Concept/C-X> <rdfs:subClassOf> <semoss:Concept> }
	 * 2) { <semoss:Concept/C-X> <rdfs:Class> 'TYPE:STRING' } <- note that all RDF concepts are type STRING since they are URIs
	 * 3) { <semoss:Concept/C-X> <semoss:Relation/Conceptual> <semoss:Concept/CX> }
 	 * TODO: no longer doing 4
	 * 4) { <semoss:Concept/CX> <rdfs:subClassOf> <semoss:Concept> }
	 * 
	 * NOTE: IF THE CONCEPT WAS 'CX', #3 would be a self-loop and that's okay
	 * 
	 * For a RDBMS concept, say it is 'C1' for column name and sits on 'T1' table name, the following tiples will be added:
	 * 1) { <semoss:Concept/C1/T1> <rdfs:subClassOf> <semoss:Concept> }
	 * 2) { <semoss:Concept/C1/T1> <rdfs:Class> 'TYPE:DATA_TYPE_HERE' }
	 * 3) { <semoss:Concept/C1/T1> <semoss:Relation/Conceptual> <semoss:Concept/T1> }
 	 * TODO: no longer doing 4
	 * 4) { <semoss:Concept/C1> <rdfs:subClassOf> <semoss:Concept> }
	 */
	
	/**
	 * Adding a concept into the OWL
	 * There are some differences based on how the information is used based on if it is a 
	 * RDF engine or a RDBMS engine
	 * @param tableName				For RDF: This is the name of the concept
	 * 								For RDBMS: This is the name of the table where the concept exists
	 * @param colName				For RDF: This is NOT used
	 * 								For RDBMS: This is the name of the column which contains the concept instances
	 * @param baseURI				This is the base URI for all the meta URIs
	 * 								99.99999% of the time this should be the following string "http://semoss.org/ontologies/"
	 * @param dataType				The dataType for the concept
	 * @return						Returns the physical URI for the node
	 * 								The return is only used in RDF databases where the instance URI must be linked
	 * 								to the meta URI
	 */
	public String addConcept(String tableName, String colName, String baseURI, String dataType) {
		// since RDF uses this multiple times, don't create it each time and just store it in a hash to send back
		if(!conceptHash.containsKey(tableName + colName)) {
			// generate the base node uri and base relationship uri
			// this is going to be using the baseURI passed in and the default node class and the ]
			// default relationship class
			// 99.99999% of the time this will end up being "http://semoss.org/ontologies/Concept" &&
			// "http://semoss.org/ontologies/Relation" respectively
			// not sure when we would every pass in another baseURI....
			String baseNodeURI = baseURI + DEFAULT_NODE_CLASS;
			String baseRelation = baseURI + DEFAULT_RELATION_CLASS;
			
			// here is the logic to create the physical uri for the concept
			// the base URI for the concept will be the baseNodeURI
			String subject = baseNodeURI + "/";
			// we also want to keep track of what the conceptual name is
			// jk, this will always be the table name now
//			String subjectName = null;
			// if it is an RDBMS engine, we need to account when the table name and column name are not the same
			if(type.equals(IEngine.ENGINE_TYPE.RDBMS)) {
				// if the column is null or empty, assume table name and col name are the same
				if(colName == null || colName.isEmpty()) {
					subject += tableName + "/" + tableName;
//					subjectName = tableName;
				} else {
					// they might be the same, might be diff, don't matter
					// create it appropriately
					subject += colName + "/" + tableName;
//					subjectName = colName;
				}
			} else {
				// here, it must be RDF
				// just add the table name since that is supposed to hold the concept name
				subject += tableName;
//				subjectName = tableName;
			}
			
			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above
			
			// 1) adding the physical URI concept as a subClassOf the baseNodeURI
			engine.addToBaseEngine(subject, RDFS.SUBCLASSOF.stringValue(), baseNodeURI);
			// 2) now lets add the dataType of the concept
			String typeObject = "TYPE:" + dataType;
			String dataTypeUri = RDFS.CLASS.toString();
			engine.addToBaseEngine(subject, dataTypeUri, typeObject);
			// 3 & 4) now lets add the physical URI concept to the conceptual concept URI
			// one of the advantages of the conceptual URI is that we can specify a concept in a database even
			// if it contains any of the special characters that are not allowed in PKQL
			// the conceptual name is the clean version of the name ... for now
			String conceptualRelationship = baseRelation + "/" + CONCEPTUAL_RELATION_NAME;
			String conceptualNode = Utility.cleanVariableString(tableName);
			String conceptualSubject = baseNodeURI + "/" + conceptualNode;
			engine.addToBaseEngine(subject, conceptualRelationship, conceptualSubject);
//			engine.addToBaseEngine(conceptualSubject, RDFS.SUBCLASSOF.stringValue(), baseNodeURI);

			// lastly, store it in the hash for future use
			conceptHash.put(tableName + colName, subject);
		}		
		return conceptHash.get(tableName + colName);
	}
	
	/**
	 * Wrapper around the main addConcept method.  Simplifies the call to use the default 
	 * base SEMOSS_URI.  This version takes in the table name and column name, so it is primarily
	 * meant to be used for RDBMS engines
	 * @param tableName				For RDF: This is the name of the concept
	 * 								For RDBMS: This is the name of the table where the concept exists
	 * @param colName				For RDF: This is NOT used
	 * 								For RDBMS: This is the name of the column which contains the concept instances
	 * @param dataType				The dataType for the concept
	 * @return						Returns the physical URI for the node
	 */
	public String addConcept(String tableName, String colName, String dataType) {
		return addConcept(tableName, colName, SEMOSS_URI, dataType);
	}
	
	/**
	 * Wrapper around the main addConcept method.  Simplifies the call as the column name is not required 
	 * and it defaults to use the default base SEMOSS_URI
	 * This version passes in an empty string as the column name.  If the engine is RDBMS, this means the 
	 * column name for the concept is the same as the table name
	 * @param tableName				For RDF: This is the name of the concept
	 * 								For RDBMS: This is the name of the table AND column name (they are the same) for the concept
	 * @param dataType				The dataType for the concept
	 * @return						Returns the physical URI for the node
	 */
	public String addConcept(String tableName, String dataType) {
		return addConcept(tableName, "", SEMOSS_URI, dataType);
	}
	
	/**
	 * Wrapper around the main addConcept method.  Simplifies the call as the column name is not required 
	 * and it defaults to use the default base SEMOSS_URI and the data type as string
	 * This version is really only intended for RDF databases
	 * Since concepts are URIs, the data type assumption of string is valid when an engine is RDF
	 * @param concept				The name of the concept to a
	 * @return						Returns the physical URI for the node
	 */
	public String addConcept(String concept) {
		return addConcept(concept, "", SEMOSS_URI, "STRING");
	}
	
	/////////////////// END ADDING CONCEPTS INTO THE OWL /////////////////////////////////

	
	/////////////////// ADDING RELATIONSHIP INTO THE OWL /////////////////////////////////
	/*
	 * This method is used to make it easier to add relationships into the owl file
	 * 
	 * Lets look at what triples will be added
	 * NOTE: for simplicity, I will use 'semoss:' to denote the prefix 'http://semoss.org/ontologies'
	 * 
	 * When a predicate is defined, lets call it 'ABC' the following triple is added:
	 * 1) { <semoss:Relation/ABC> <rdfs:subPropertyOf> <semoss:Relation> }
	 * but if the predicate is not defined, and the fromConcept is 'X' and toConcept is 'Y', the following
	 * triple is added: 
	 * 1) { <semoss:Relation/X.X.Y.X_FK> <rdfs:subPropertyOf> <semoss:Relation> }
	 * TODO: For RDBMS databases, this is not accurate, we need to overload this method...
	 * 		The relationship above always assumes the position of the FK on the second node
	 * 		and the method assumes the table name and column name are the name and the connecting column 
	 * 		is the first table with "_FK" on it needs to be overloaded to allow for proper concept creation
	 * 
	 * 
	 * Let's just continue with the relationship URI being <semoss:Relation/ABC>
	 * 
	 * For a RDF relationship between two concept, call each concept 'X' and 'Y', the following triples will be added:
	 * 2) { <semoss:Concept/X> <semoss:Relation/ABC> <semoss:Concept/Y> }
	 * For a RDBMS relationship between two concept, call each concept 'X' and 'Y', the following triples will be added:
	 * 2) { <semoss:Concept/X/X> <semoss:Relation/ABC> <semoss:Concept/Y/Y> }
	 * 
	 * NOTE: RDBMS section #2, if the concept is predefined using the addConcept method with a table and column name
	 * 		already defined, then the URIs for the concepts will not necessarily have the table name and column name
	 * 		as being the same
	 */
	
	/**
	 * Adds a relationship between two concepts within the OWL
	 * @param fromTable				For RDF: This is the name of the start concept
	 * 								For RDBMS: This is the name of the table for the start concept
	 * @param fromColumn			For RDF: This is not used
	 * 								For RDBMS: This is the name of the column for the start concept
	 * 								If empty, the logic assumes it is the fromTable
	 * @param toTable				For RDF: This is the name of the end concept
	 * 								For RDBMS: This is the name of the table for the end concept
	 * @param toColumn				For RDF: This is not used
	 * 								For RDBMS: This is the name of the column for the end concept
	 * 								If empty, the logic assumes it is the toTable
	 * @param predicate				The predicate for the relationship.  If this is null, the code
	 * 								will generate a predicate based on the fromConcept and toConcept.
	 * 								The format will be created based on the default loading of a RDBMS
	 * 								predicate where each concept is its own table and the column name 
	 * 								matches the table name.
	 * @return						Returns the physical URI for the relationship
	 */
	public String addRelation(String fromTable, String fromColumn, String toTable, String toColumn, String predicate) {
		// since RDF uses this multiple times, don't create it each time and just store it in a hash to send back
		if(!relationHash.containsKey(fromTable + fromColumn + toTable + toColumn + predicate)) {
			
			// need to make sure both the fromConcept and the toConcept are already defined as concepts
			// TODO: this works for RDBMS even though it only takes in the concept names because we usually perform
			// 		the addConcept call before... this is really just intended to retrieve the concept URIs from the
			// 		conceptHash as they should already there
			String fromConceptURI = addConcept(fromTable, fromColumn, null);
			String toConceptURI = addConcept(toTable, toColumn, null);
			
			// if a predicate is not defined, we create it based on the connection
			// TODO: this makes a lot of assumptions regarding the structure of the database
			// 		the assumption is only accurate for databases that we create within the tool through upload
			if(predicate == null) {
				// assume the foreign key is on the toTable
				// this will be the foreign key column in the toTable, which is the from column name + "_FK"
				// however, if from column is empty, it will be the assumption that from column is the same as from table
				
				// store the foreign key column
				String fk_Col = null;
				// this will be the column name in case it is empty and the same as the table name
				String fromCol = null;
				if(!fromColumn.isEmpty()) {
					fk_Col = fromColumn;
					fromCol = fromColumn;
				} else {
					// if the fromColumn is empty, assume it is the same as the table
					fk_Col = fromTable;
					fromCol = fromTable;
				}
				fk_Col += "_FK";
				
				// generate the predicate string
				predicate = fromTable + "." + fromCol + "." + toTable + "." + fk_Col;
			}
			
			// TODO: this always assumes the baseURI for the relationship is the default semoss uri
			// create the base relationship uri
			String baseRelationURI = SEMOSS_URI + DEFAULT_RELATION_CLASS;
			String predicateSubject = baseRelationURI + "/" + predicate;
			
			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above
			
			// 1) now add the physical relationship URI
			engine.addToBaseEngine(predicateSubject, RDFS.SUBPROPERTYOF.stringValue(), baseRelationURI);
			
			// 2) now add the relationship between the two nodes
			engine.addToBaseEngine(fromConceptURI, predicateSubject, toConceptURI);
			
			// lastly, store it in the hash for future use
			relationHash.put(fromTable + fromColumn + toTable + toColumn + predicate, predicateSubject);
		}
		return relationHash.get(fromTable + fromColumn + toTable + toColumn + predicate);
	}
	
	/**
	 * Adds a relationship between two concepts within the OWL
	 * @param fromConcept			For RDF: This is the name of the start concept
	 * 								For RDBMS: This is the name of the table AND column name (they are the same) for
	 * 								the start concept
	 * @param toConcept				For RDF: This is the name of the end concept
	 * 								For RDBMS: This is the name of the table AND column name (they are the same) for 
	 * 								the end concept
	 * @param predicate				The predicate for the relationship.  If this is null, the code
	 * 								will generate a predicate based on the fromConcept and toConcept.
	 * 								The format will be created based on the default loading of a RDBMS
	 * 								predicate where each concept is its own table and the column name 
	 * 								matches the table name.
	 * @return						Returns the physical URI for the relationship
	 */
	public String addRelation(String fromConcept, String toConcept, String predicate) {
		return addRelation(fromConcept, "", toConcept, "", predicate);
	}
	
	/////////////////// END ADDING RELATIONSHIP INTO THE OWL /////////////////////////////////

	
	/////////////////// ADDING PROPERTIES TO CONCEPTS IN THE OWL /////////////////////////////////
	/*
	 * This method is overloaded to make it easier to add properties for concepts into the owl file
	 * 
	 * Lets look at what triples will be added
	 * NOTE: for simplicity, I will use 'semoss:' to denote the prefix 'http://semoss.org/ontologies'
	 * 
	 * For a RDF property, call it 'P-P' on concept 'X' the following triples will be added:
	 * 1) { <semoss:Relation/Contains/P-P> <rdf:type> <semoss:Relation/Contains> }
	 * 2) { <semoss:Concept/X> <semoss:Relation/Contains> <semoss:Relation/Contains/P-P> }
	 * 3) { <semoss:Relation/Contains/P-P> <rdfs:Class> TYPE:DATA_TYPE_HERE }
	 * 4) { <semoss:Relation/Contains/P-P> <semoss:Relation/Conceptual> <semoss:Relation/Contains/PP> }
	 * TODO: no longer doing 5
	 * 5) { <semoss:Relation/Contains/PP> <owl.DatatypeProperty> <semoss:Relation/Contains> }
	 * 
	 * NOTE: IF THE PROPERTY WAS 'PP', #4 would be a self-loop and that's okay
	 * 
	 * For a RDBMS property, call it 'P' on concept with table name 'X' and column name 'Y' the following triples will be added:
	 * 1) { <semoss:Relation/Contains/P/X> <rdf:type> <semoss:Relation/Contains> }
	 * 2) { <semoss:Concept/Y/X> <semoss:Relation/Contains> <semoss:Relation/Contains/P/X> }
	 * 3) { <semoss:Relation/Contains/P/X> <rdfs:Class> TYPE:DATA_TYPE_HERE }
	 * 4) { <semoss:Relation/Contains/P/X> <semoss:Relation/Conceptual> <semoss:Relation/Contains/P> }
	 * TODO: no longer doing 5
	 * 5) { <semoss:Relation/Contains/P> <owl.DatatypeProperty> <semoss:Relation/Contains> }
	 */
	
	/**
	 * This method will add a property onto a concept in the OWL file
	 * There are some differences based on how the information is used based on if it is a 
	 * RDF engine or a RDBMS engine
	 * @param tableName				For RDF: This is the name of the concept
	 * 								For RDBMS: This is the name of the table where the concept exists
	 * @param colName				For RDF: This is NOT used
	 * 								For RDBMS: This is the name of the column which contains the concept instances
	 * @param propertyCol			This will be the name of the property
	 * @param dataType				The dataType for the property
	 * @return						Returns the physical URI for the node
	 * 								TODO: RDF databases end up constructing the prop URI regardless.. need them to take this return
	 */
	public String addProp(String tableName, String colName, String propertyCol, String dataType)
	{
		// since RDF uses this multiple times, don't create it each time and just store it in a hash to send back
		if(!propHash.containsKey(tableName + "%" + propertyCol)) {
			// TODO: this always assumes the baseURI for the property is the default semoss uri
			String basePropURI = SEMOSS_URI + DEFAULT_PROP_CLASS;
			String baseRelation = SEMOSS_URI + DEFAULT_RELATION_CLASS;

			// add the concept to get the physical URI
			// TODO: this also makes the assumption that the concept is already added since the dataType being passed is null
			//		is inaccurate... this is really just intended to retrieve the concept URIs from the conceptHash
			String conceptURI = addConcept(tableName, colName, null); 
			
			// create the property URI
			String property = basePropURI + "/";
			// if it is an RDBMS engine, we need to account when the table name and column name are not the same
			if(type.equals(IEngine.ENGINE_TYPE.RDBMS)) {
				// need to append the table name onto the URI
				property += propertyCol + "/" + tableName;
			} else {
				// here, it must be RDF
				// just add the property col 
				property += propertyCol;
			}
			
			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above
			
			// 1) adding the property as type of base property URI
			engine.addToBaseEngine(property, RDF.TYPE.stringValue(), basePropURI);
			// 2) adding the property to the concept
			engine.addToBaseEngine(conceptURI, OWL.DatatypeProperty.toString(), property);
			// 3) adding the property data type
			String typeObject = "TYPE:" + dataType;
			String dataTypeUri = RDFS.CLASS.toString();
			engine.addToBaseEngine(property, dataTypeUri, typeObject);
			
			// 4 & 5) now lets add the physical property to the conceptual property
			// one of the advantages of the conceptual URI is that we can specify a property in a database even 
			// if it contains any of the special characters that are not allowed in PKQL
			// the conceptual property name is the clean version of the property name ... for now
			String conceptualRelationship = baseRelation +  "/" + CONCEPTUAL_RELATION_NAME;
			String conceptualPropertyName = Utility.cleanVariableString(propertyCol);
			String conceptualProperty = basePropURI + "/" + conceptualPropertyName;
			engine.addToBaseEngine(property, conceptualRelationship, conceptualProperty);
//			engine.addToBaseEngine(conceptualProperty, RDF.TYPE.stringValue(), basePropURI);
			
			// lastly, store it in the hash for future use
			propHash.put(tableName + "%" + propertyCol, property);
		}
		
		return propHash.get(tableName + "%" + propertyCol);
	}
	
	
	/**
	 * This method will add a property onto a concept in the OWL file
	 * There are some differences based on how the information is used based on if it is a 
	 * RDF engine or a RDBMS engine
	 * @param tableName				For RDF: This is the name of the concept
	 * 								For RDBMS: This is the name of the table where the concept exists. If the concept
	 * 									doesn't exist, it is assumed the column name of the concept is the same as the table name
	 * @param propertyCol			This will be the name of the property
	 * @param dataType				The dataType for the property
	 * @return						Returns the physical URI for the node
	 */
	public String addProp(String tableName, String propertyCol, String dataType) {
		return addProp(tableName, "", propertyCol, dataType);
	}
	
	/////////////////// END ADDING PROPERTIES TO CONCEPTS INTO THE OWL /////////////////////////////////

	
	/////////////////// ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////
	
	/**
	 * Have one class a subclass of another class
	 * This code is really intended for RDF databases... not sure what use it will have
	 * to utilize this within an RDBMS
	 * @param childType					The child concept node
	 * @param parentType				The parent concept node
	 */
	public void addSubclass(String childType, String parentType) {
		String childURI = addConcept(childType);
		String parentURI = addConcept(parentType);
		engine.addToBaseEngine(childURI, RDFS.SUBCLASSOF.stringValue(), parentURI);
	}
	
	/**
	 * Store the custom base URI used to create instance URIs within the OWL
	 * E.g. of usage is current RDF MHS databases, which use "http://health.mil/ontologies" as the custom base URI
	 * @param customBaseURI				The customBaseURI to store
	 */
	public void addCustomBaseURI(String customBaseURI) {
		engine.addToBaseEngine("SEMOSS:ENGINE_METADATA", "CONTAINS:BASE_URI", customBaseURI+"/"+DEFAULT_NODE_CLASS+"/");
	}
	
	/////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////

	
	///////////////////// TESTING /////////////////////
	public static void main(String [] args) {
		OWLER owler = new OWLER("C:\\workspace\\Semoss_Dev\\test.owl", IEngine.ENGINE_TYPE.RDBMS);
//		owler.addConcept("Movie_Table", "Title", "varchar(800)");
//		owler.addProp("Movie_Table", "Title", "Revenue-Domestic", "DOUBLE");
//		owler.addConcept("Genre", "Genre", "varchar(800)");
//		owler.addRelation("Movie_Table", "Title", "Genre", "Genre", "Movie_Table.Title.Genre.Title_FK");

		owler.addConcept("Title", "varchar(800)");
		owler.addProp("Title", "Revenue-Domestic", "DOUBLE");
		owler.addConcept("Genre", "varchar(800)");
		owler.addRelation("Title", "Genre", null);
		
		owler.getOwlAsString();
	}
	
	public String getOwlAsString() {
		// this will both write the owl to a file and print it onto the console
		String owl = null;
		try {
			owl = engine.exportBaseEngAsString(true);
			System.out.println("OWL.. " + owl);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return owl;
	}
	///////////////////// END TESTING /////////////////////

	
	///////////////// GETTERS ///////////////////////
	
	/**
	 * Get the owl file path set in the owler
	 * @return
	 */
	public String getOwlPath() {
		return this.owlPath;
	}
	
	/*
	 * The getters exist for the conceptHash, relationHash, and propHash
	 * These are only used during RDF uploading
	 * RDF requires the meta data information to also be stored in the database
	 * along with the instance data
	 */
	
	public Hashtable<String, String> getConceptHash() {
		return conceptHash;
	}
	public Hashtable<String, String> getRelationHash() {
		return relationHash;
	}
	public Hashtable<String, String> getPropHash() {
		return propHash;
	}
	
	///////////////// END GETTERS ///////////////////////

	///////////////// SETTERS ///////////////////////
	/*
	 * These methods are not actually used.. kinda here just in case we end up needing them
	 */
	
	public void setOwlPath(String owlPath) {
		this.owlPath = owlPath;
	}
	public void setConceptHash(Hashtable<String, String> conceptHash) {
		this.conceptHash = conceptHash;
	}
	public void setRelationHash(Hashtable<String, String> relationHash) {
		this.relationHash = relationHash;
	}
	public void setPropHash(Hashtable<String, String> propHash) {
		this.propHash = propHash;
	}
	///////////////// END SETTERS ///////////////////////
}
