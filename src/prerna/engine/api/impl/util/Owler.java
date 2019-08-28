package prerna.engine.api.impl.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Utility;

public class Owler extends AbstractOwler {

	/**
	 * Constructor for the class when we are creating a brand new OWL file
	 * @param fileName				The location of the new OWL file
	 * @param type					The type of the engine the OWL file is being created for
	 */
	public Owler(String owlPath, IEngine.ENGINE_TYPE type) {
		super(owlPath, type);
	}
	
	/**
	 * Constructor for the class when we are adding to an existing OWL file
	 * @param existingEngine		The engine we are adding to
	 */
	public Owler(IEngine existingEngine) {
		super(existingEngine);
	}
	
	/////////////////// ADDING CONCEPTS INTO THE OWL /////////////////////////////////

	/**
	 * Add a concept to the OWL
	 * If RDF : a concept has a data type (String)
	 * If RDBMS : this will represent a table and not have a datatype
	 * @param tableName
	 * @param dataType
	 * @param conceptual
	 * @return
	 */
	public String addConcept(String tableName, String dataType, String conceptual) {
		// since RDF uses this multiple times, don't create it each time and just store it in a hash to send back
		if(!conceptHash.containsKey(tableName)) {
			// here is the logic to create the physical uri for the concept
			// the base URI for the concept will be the baseNodeURI
			String subject = BASE_NODE_URI + "/" + tableName;
			
			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above

			// 1) adding the physical URI concept as a subClassOf the baseNodeURI
			engine.addToBaseEngine(subject, RDFS.SUBCLASSOF.stringValue(), BASE_NODE_URI);

			// 2) now lets add the dataType of the concept 
			// this will only apply if it is RDF
			if(dataType != null) {
				String typeObject = "TYPE:" + dataType;
				engine.addToBaseEngine(subject, RDFS.CLASS.stringValue(), typeObject);
			}
			if(MetadataUtility.ignoreConceptData(this.type)) {
				// add an ignore data tag so we can easily query
				engine.addToBaseEngine(subject, RDFS.DOMAIN.toString(), "noData", false);
			}

			// 3) now lets add the physical URI to the pixel name URI
			String pixelName = Utility.cleanVariableString(tableName);
			pixelNames.add(pixelName);
			String pixelUri = BASE_NODE_URI + "/" + pixelName;
			engine.addToBaseEngine(subject, PIXEL_RELATION_URI, pixelUri);
			
			// 4) let us add the original table name as the conceptual name
			if(conceptual == null) {
				conceptual = tableName;
			}
			engine.addToBaseEngine(subject, CONCEPTUAL_RELATION_URI, conceptual, false);
			
			
			// store it in the hash for future use
			// NOTE : The hash contains the physical URI
			conceptHash.put(tableName, subject);
		}
		return conceptHash.get(tableName);
	}
	
	public String addConcept(String tableName, String dataType) {
		return addConcept(tableName, dataType, null);
	}
	
	public String addConcept(String concept) {
		return addConcept(concept, "STRING", null);
	}
	
	/////////////////// END ADDING CONCEPTS INTO THE OWL /////////////////////////////////

	
	/////////////////// ADDING RELATIONSHIP INTO THE OWL /////////////////////////////////
	
	/**
	 * Add a relationship between two concepts
	 * In RDBMS : the predicate must be fromTable.fromColumn.toTable.toColumn 
	 * @param fromTable
	 * @param toTable
	 * @param predicate
	 * @return
	 */
	public String addRelation(String fromTable, String toTable, String predicate) {
		// since RDF uses this multiple times, don't create it each time and just store it in a hash to send back
		if(!relationHash.containsKey(fromTable + toTable + predicate)) {
			
			// need to make sure both the fromConcept and the toConcept are already defined as concepts
			// TODO: this works for RDBMS even though it only takes in the concept names because we usually perform
			// 		the addConcept call before... this is really just intended to retrieve the concept URIs from the
			// 		conceptHash as they should already there
			String fromConceptURI = addConcept(fromTable, null, null);
			String toConceptURI = addConcept(toTable, null, null);
			
			// create the base relationship uri
			String baseRelationURI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
			String predicateSubject = baseRelationURI + "/" + predicate;
			
			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above
			
			// 1) now add the physical relationship URI
			engine.addToBaseEngine(predicateSubject, RDFS.SUBPROPERTYOF.stringValue(), baseRelationURI);
			
			// 2) now add the relationship between the two nodes
			engine.addToBaseEngine(fromConceptURI, predicateSubject, toConceptURI);
			
			// lastly, store it in the hash for future use
			relationHash.put(fromTable + toTable +predicate, predicateSubject);
		}
		return relationHash.get(fromTable + toTable + predicate);
	}
	
	/**
	 * Remove an added predicate joining two tables together
	 * @param fromTable
	 * @param toTable
	 * @param predicate
	 */
	public void removeRelation(String fromTable, String toTable, String predicate) {
		String fromConceptURI = addConcept(fromTable, null, null);
		String toConceptURI = addConcept(toTable, null, null);
		
		// create the base relationship uri
		String baseRelationURI = SEMOSS_URI_PREFIX + DEFAULT_RELATION_CLASS;
		String predicateSubject = baseRelationURI + "/" + predicate;
		
		// now lets start to add the triples
		// lets add the triples pertaining to those numbered above
		
		// 1) now add the physical relationship URI
		engine.removeFromBaseEngine(predicateSubject, RDFS.SUBPROPERTYOF.stringValue(), baseRelationURI);
		
		// 2) now add the relationship between the two nodes
		engine.removeFromBaseEngine(fromConceptURI, predicateSubject, toConceptURI);
		
		// lastly, store it in the hash for future use
		relationHash.remove(fromTable + toTable + predicate);
	}
	
	/////////////////// END ADDING RELATIONSHIP INTO THE OWL /////////////////////////////////

	
	/////////////////// ADDING PROPERTIES TO CONCEPTS IN THE OWL /////////////////////////////////

	/**
	 * Add a property to a given concept
	 * @param tableName
	 * @param propertyCol
	 * @param dataType
	 * @param adtlDataType
	 * @param conceptual
	 * @return
	 */
	public String addProp(String tableName, String propertyCol, String dataType, String adtlDataType, String conceptual) {
		if(!propHash.containsKey(tableName + "%" + propertyCol)) {
			String conceptURI = addConcept(tableName, null, null); 
			
			// create the property URI
			String property = null;
			if(type == IEngine.ENGINE_TYPE.SESAME) {
				// THIS IS BECAUSE OF LEGACY QUERIES!!!
				property = BASE_PROPERTY_URI + "/" + propertyCol;
			} else {
				property = BASE_PROPERTY_URI + "/" + propertyCol + "/" + tableName;
			}
			
			// now lets start to add the triples
			// lets add the triples pertaining to those numbered above
			
			// 1) adding the property as type of base property URI
			engine.addToBaseEngine(property, RDF.TYPE.stringValue(), BASE_PROPERTY_URI);
			
			// 2) adding the property to the concept
			engine.addToBaseEngine(conceptURI, OWL.DatatypeProperty.toString(), property);
			
			// 3) adding the property data type
			String typeObject = "TYPE:" + dataType;
			engine.addToBaseEngine(property, RDFS.CLASS.stringValue(), typeObject);
			
			// 4) adding the property additional data type, if available
			if (adtlDataType != null && !adtlDataType.isEmpty()) {
				String adtlTypeObject = "ADTLTYPE:" + adtlDataType.replace("/", "{{REPLACEMENT_TOKEN}}").replace("'", "((SINGLE_QUOTE))").replace(" ", "((SPACE))");
				String adtlTypeUri = SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS + "/AdtlDataType";
				engine.addToBaseEngine(property, adtlTypeUri, adtlTypeObject);
			}
			
			// 5) now lets add the physical URI to the pixel name URI
			String pixelName = Utility.cleanVariableString(propertyCol);
			String pixelFullName = pixelName + "/" +  Utility.cleanVariableString(tableName);
			String pixelUri = BASE_PROPERTY_URI + "/" + pixelFullName;
			engine.addToBaseEngine(property, PIXEL_RELATION_URI, pixelUri);
			
			// 5) let us add the original table name as the conceptual name
			if(conceptual == null) {
				conceptual = propertyCol;
			}
			engine.addToBaseEngine(property, CONCEPTUAL_RELATION_URI, conceptual, false);
			
			// lastly, store it in the hash for future use
			// NOTE : The hash contains the physical URI
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
	 * @param adtlDataType			Additional data type for the property 
	 * @return						Returns the physical URI for the node
	 */
	public String addProp(String tableName, String propertyCol, String dataType, String adtlDataType) {
		return addProp(tableName, propertyCol, dataType, adtlDataType, null);
	}
	
	public String addProp(String tableName, String propertyCol, String dataType) {
		return addProp(tableName, propertyCol, dataType, null, null);
	}
	
	/**
	 * This method will calculate the unique values in each column/property
	 * and add it to the owl file.
	 * 
	 * @param queryEngine
	 */
	public void addUniqueCounts(IEngine queryEngine) {
		String uniqueCountProp = SEMOSS_URI_PREFIX + DEFAULT_PROP_CLASS + "/UNIQUE";

		List<String> pixelConcepts = queryEngine.getPixelConcepts();
		for(String pixelConcept : pixelConcepts) {
			List<String> pSelectors = queryEngine.getPixelSelectors(pixelConcept);
			for(String selectorPixel : pSelectors) {
				SelectQueryStruct qs = new SelectQueryStruct();
				QueryFunctionSelector newSelector = new QueryFunctionSelector();
				newSelector.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
				newSelector.setDistinct(true);
				QueryColumnSelector innerSelector = new QueryColumnSelector(selectorPixel);
				newSelector.addInnerSelector(innerSelector);
				qs.addSelector(newSelector);
				qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
				
				Iterator<IHeadersDataRow> it = WrapperManager.getInstance().getRawWrapper(queryEngine, qs);
				if (!it.hasNext()) {
					continue;
				}
				long uniqueRows = ((Number) it.next().getValues()[0]).longValue();
				String propertyPhysicalUri = queryEngine.getPhysicalUriFromPixelSelector(selectorPixel);
				this.engine.addToBaseEngine(propertyPhysicalUri, uniqueCountProp, uniqueRows, false);
			}
		}
		
		this.engine.commit();
		try {
			this.engine.exportBaseEng(false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/////////////////// END ADDING PROPERTIES TO CONCEPTS INTO THE OWL /////////////////////////////////

	public void addLegacyPrimKey(String tableName, String columnName) {
		String physicalUri = conceptHash.get(tableName);
		if(physicalUri == null) {
			physicalUri = addConcept(tableName, null, null);
		}
		this.engine.addToBaseEngine(physicalUri, AbstractOwler.LEGACY_PRIM_KEY_URI, columnName, false);
	}
	
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
	
	/////////////////// END ADDITIONAL METHODS TO INSERT INTO THE OWL /////////////////////////////////

	///////////////////// TESTING /////////////////////
	public static void main(String [] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		Owler owler = new Owler( "C:\\workspace\\Semoss_Dev\\themes_OWL.OWL", IEngine.ENGINE_TYPE.RDBMS);
		
		owler.addConcept("ADMIN_THEME ", "id", "VARCHAR(255)");
		owler.addProp("ADMIN_THEME ", "id", "theme_name", "VARCHAR(255)", null);
		owler.addProp("ADMIN_THEME ", "id", "theme_map", "CLOB", null);
		owler.addProp("ADMIN_THEME ", "id", "is_active", "BOOLEAN", null);

		// load the owl into a rfse
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
		}
		owler.getOwlAsString();
		rfse.openFile(owler.getOwlPath(), "RDF/XML", SEMOSS_URI_PREFIX);
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
}
