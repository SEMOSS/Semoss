//package prerna.engine.impl;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Vector;
//
//import org.apache.commons.io.FileUtils;
//import org.openrdf.model.vocabulary.OWL;
//
//import com.hp.hpl.jena.vocabulary.RDF;
//import com.hp.hpl.jena.vocabulary.RDFS;
//
//import prerna.engine.api.IEngine.ENGINE_TYPE;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.Constants;
//import prerna.util.OWLER;
//import prerna.util.Utility;
//
//@Deprecated
//public class OwlConceptualNameModernizer {
//
//	private String owlPath;
//	private RDFFileSesameEngine rfse;
//	private ENGINE_TYPE engineType = ENGINE_TYPE.RDBMS;
//
//	public OwlConceptualNameModernizer(Properties prop) {
//		// create the owl engine
//		// since we need to do this on start up
//		// we dont want to load the entire engine
//
//		this.owlPath = SmssUtilities.getOwlFile(prop).getAbsolutePath();
//		
//		// owl is stored as RDF/XML file
//		this.rfse = new RDFFileSesameEngine();
//		this.rfse.openFile(this.owlPath, null, null);
//
//		// also need to get the engine class to determine if RDF or RDBMS
//		String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
//		if(!engineClass.contains("RDBMSNativeEngine")) {
//			// it is either RDBMS or some kind of RDF
//			// the type of RDF actually doesn't matter
//			this.engineType = ENGINE_TYPE.SESAME;
//		}
//	}
//
//	/**
//	 * Will do all the processing required and keep the code encapsulated to 
//	 * minimize touching other files
//	 * @throws IOException 
//	 */
//	public void run() throws IOException {
//
//		// see if we need to modernize
//		if(requireModernization()) {
//			// okay, we got some clean up to do
//
//			// at this point, all the conceptual names are auto generated
//			// so no need to preserve existing conceptual names
//			// ^see requireModernization method which shows the 2 ways the 
//			// owl can be out dated
//
//			// note: the below needs to follow separate processing
//			// if it is a rdf engine vs. rdbms engine
//
//			// we need the list of concepts and data types
//			Map<String, String> conceptUris = new Hashtable<String, String>();
//			// we need the list of properties and the concepts they connect to
//			Map<String, Map<String, String>> conceptProperties = new Hashtable<String, Map<String, String>>();
//			// we need all the relationships
//			List<Object[]> relationships = new Vector<Object[]>();
//			// we need the base_uri
//			String baseUri = null;
//			
//			try {
//				// fill the above data structures
//				
//				// first, fill in all the concepts by themselves
//				String getAllConceptUrisQuery = "SELECT DISTINCT ?concept ?dataType WHERE { "
//						+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//						+ "{?concept <" + RDFS.Class.toString() + "> ?dataType} "
//						+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
//						+ " && ?concept != <" + RDFS.Class + "> "
//						+ " && ?concept != <" + RDFS.Resource + "> "
//						+ ")"
//						+ "}";
//				
//				IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(this.rfse, getAllConceptUrisQuery);
//				while(manager.hasNext()) {
//					IHeadersDataRow stmt = manager.next();
//					Object[] rawValues = stmt.getRawValues();
//					String conceptName = rawValues[0] + "";
//					String dataType = rawValues[1] + "";
//					// clean the portion of text we add for the type
//					if(dataType.contains("TYPE:")) {
//						dataType = dataType.replace("TYPE:", "");
//					}
//					conceptUris.put(conceptName, dataType);
//				}
//				
//				// second, fill in properties for concepts that have them
//				String getAllConceptPropertiesQuery = "SELECT DISTINCT ?concept ?property ?dataType WHERE { "
//						+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//						+ "{?concept  <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//						+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//						+ "{?property <" + RDFS.Class.toString() + "> ?dataType} "
//						+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
//						+ " && ?concept != <" + RDFS.Class + "> "
//						+ " && ?concept != <" + RDFS.Resource + "> "
//						+ ")"
//						+ "}";
//				
//				manager = WrapperManager.getInstance().getRawWrapper(this.rfse, getAllConceptPropertiesQuery);
//				while(manager.hasNext()) {
//					IHeadersDataRow stmt = manager.next();
//					Object[] rawValues = stmt.getRawValues();
//					String conceptUri = rawValues[0] + "";
//					String propertyUri = rawValues[1] + "";
//					String propDataType = rawValues[2] + "";
//					// clean the portion of text we add for the type
//					if(propDataType.contains("TYPE:")) {
//						propDataType = propDataType.replace("TYPE:", "");
//					}
//					
//					Map<String, String> propMap = null;
//					if(conceptProperties.containsKey(conceptUri)) {
//						propMap = conceptProperties.get(conceptUri);
//					} else {
//						propMap = new Hashtable<String, String>();
//						conceptProperties.put(conceptUri, propMap);
//					}
//					
//					// property is unique for a concept
//					// so no need to do additional check on map
//					propMap.put(propertyUri, propDataType);
//				}
//				
//				// lastly, get all the relationships
//				String getAllRelationshipsQuery = "SELECT DISTINCT ?fromConcept ?relationship ?toConcept WHERE { "
//						+ "{?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//						+ "{?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//						+ "{?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
//						+ "{?fromConcept ?relationship ?toConcept} "
//						+ "FILTER("
//						+ "	   ?fromConcept != <http://semoss.org/ontologies/Concept> "
//						+ " && ?fromConcept != <" + RDFS.Class + "> "
//						+ " && ?fromConcept != <" + RDFS.Resource + "> "
//						
//						+ " && ?toConcept != <http://semoss.org/ontologies/Concept> "
//						+ " && ?toConcept != <" + RDFS.Class + "> "
//						+ " && ?toConcept != <" + RDFS.Resource + "> "
//
//						+ " && ?relationship != <http://semoss.org/ontologies/Relation> "
//						+ ")"
//						+ "}";
//				
//				manager = WrapperManager.getInstance().getRawWrapper(this.rfse, getAllRelationshipsQuery);
//				while(manager.hasNext()) {
//					IHeadersDataRow stmt = manager.next();
//					Object[] rawValues = stmt.getRawValues();
//					relationships.add(rawValues);
//				}
//				
//				String getBaseUriQuery = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";
//				manager = WrapperManager.getInstance().getRawWrapper(this.rfse, getBaseUriQuery);
//				if(manager.hasNext()) {
//					IHeadersDataRow stmt = manager.next();
//					Object[] rawValues = stmt.getRawValues();
//					baseUri = rawValues[0] + "";
//					baseUri = baseUri.replace("/Concept/", "");
//				} else {
//					// set it to be the default value
//					baseUri = "http://semoss.org/ontologies";
//				}
//				
//				
//			} finally {
//				// make sure to close the connection!
//				// we need to do this since the way OWLER
//				// is set up, it will create a connection to the engine
//				// and we can't have both 2 connections at the same time
//				this.rfse.close();
//			}
//
//			// now we need to delete the OWL and make a new one
//			// actually, i do not want to mess up all the OWL files and get yelled at
//			// will save it as a new file first
//			File owlFile = new File(this.owlPath);
//			File inCaseIMessUpFile = new File(this.owlPath + "_OLD");
//			try {
//				FileUtils.copyFile(owlFile, inCaseIMessUpFile);
//			} catch (IOException e1) {
//				e1.printStackTrace();
//				throw new IOException("Cannot save old version of OWL. "
//						+ "Too risky to overwrite existing one in case automatic routine has errors!");
//			}
//			
//			// we were able to copy existing one, so now can delete the old file
//			owlFile.delete();
//			PrintWriter writer = null;
//			// input default parameters into the owl
//			try {
//				owlFile.createNewFile();
//				writer = new PrintWriter(this.owlPath);
//				writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
//				writer.println("<rdf:RDF");
//				writer.println("\txmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"");
//				writer.println("\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
//				writer.println("</rdf:RDF>");
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//				throw new IOException("Error creating new base OWL file!");
//			} finally {
//				if(writer != null) {
//					writer.close();
//				}
//			}
//			
//			// lets create an OWLER to help us out
//			OWLER owler = new OWLER(this.owlPath, this.engineType);
//
//			// add the concepts into the owl
//			// need to consider prim key for rdbms
//			if(this.engineType.equals(ENGINE_TYPE.RDBMS)) {
//				for(String conceptUri : conceptUris.keySet()) {
//					String tableName = Utility.getInstanceName(conceptUri);
//					String colName = Utility.getClassName(conceptUri);
//					String dataType = conceptUris.get(conceptUri);
//					
//					// add via owler
//					owler.addConcept(tableName, colName, dataType);
//				}
//			} else {
//				for(String conceptUri : conceptUris.keySet()) {
//					String tableName = Utility.getInstanceName(conceptUri);
//					String dataType = conceptUris.get(conceptUri);
//					
//					// add via owler
//					owler.addConcept(tableName, dataType);
//				}
//			}
//			
//			// add the properties into the owl
//			// again, need to consider the prim key for rdbms
//			if(this.engineType.equals(ENGINE_TYPE.RDBMS)) {
//				for(String conceptUri : conceptProperties.keySet()) {
//					String tableName = Utility.getInstanceName(conceptUri);
//					String colName = Utility.getClassName(conceptUri);
//					Map<String, String> propMap = conceptProperties.get(conceptUri);
//					
//					for(String propertyUri : propMap.keySet()) {
//						String dataType = propMap.get(propertyUri);
//						
//						// in general, the format for this is ../Contains/PropName/TableName
//						// but some owls are so old, they do not have the tableName at the end
//						// so need to add a check for this
//						String propertyName = Utility.getClassName(propertyUri);
//						if(propertyName.equals("Contains")) {
//							propertyName = Utility.getInstanceName(propertyUri);
//						}
//						// add via owler
//						owler.addProp(tableName, colName, propertyName, dataType);
//					}
//				}
//			} else {
//				for(String conceptUri : conceptProperties.keySet()) {
//					String tableName = Utility.getInstanceName(conceptUri);
//					Map<String, String> propMap = conceptProperties.get(conceptUri);
//					
//					for(String propertyUri : propMap.keySet()) {
//						String dataType = propMap.get(propertyUri);
//						String propertyName = Utility.getInstanceName(propertyUri);
//						// add via owler
//						owler.addProp(tableName, propertyName, dataType);
//					}
//				}
//			}
//			
//			// add the relationships into the owl
//			// again, need to consider the prim key for rdbms
//			if(this.engineType.equals(ENGINE_TYPE.RDBMS)) {
//				for(Object[] rel : relationships) {
//					String fromConceptUri = rel[0] + "";
//					String relUri = rel[1] + "";
//					String toConceptUri = rel[2] + "";
//					
//					String fromTableName = Utility.getInstanceName(fromConceptUri);
//					String fromColName = Utility.getClassName(fromConceptUri);
//					
//					String pred = Utility.getInstanceName(relUri);
//					
//					String toTableName = Utility.getInstanceName(toConceptUri);
//					String toColName = Utility.getClassName(toConceptUri);
//					
//					// add via owler
//					owler.addRelation(fromTableName, fromColName, toTableName, toColName, pred);
//				}
//			} else {
//				for(Object[] rel : relationships) {
//					String fromConceptUri = rel[0] + "";
//					String relUri = rel[1] + "";
//					String toConceptUri = rel[2] + "";
//					
//					String fromConcept = Utility.getInstanceName(fromConceptUri);
//					String pred = Utility.getInstanceName(relUri);
//					String toConcept = Utility.getInstanceName(toConceptUri);
//					
//					// add via owler
//					owler.addRelation(fromConcept, toConcept, pred);
//				}
//			}
//			
//			// add the custom base uri
//			owler.addCustomBaseURI(baseUri);
//			
//			// write the owl
//			try {
//				owler.commit();
//				owler.export();
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//			owler.closeOwl();
//			
//			// we done
//		}
//	}
//
//
//
//	/**
//	 * Run queries to see if we need to modernize 
//	 * @return
//	 */
//	private boolean requireModernization() {
//		// first check, see if there are any conceptual names on concepts
//		String conceptualConcepts = "SELECT DISTINCT ?conceptual WHERE { "
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
//				+ "} LIMIT 1";
//
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(this.rfse, conceptualConcepts);
//		if(!wrapper.hasNext()) {
//			// no conceptual names
//			return true;
//		}
//
//		// second check, see if properties are the correct format
//		String propertiesCorrectFormat = "SELECT DISTINCT ?property ?x ?propertyConceptual WHERE { "
//				+ "{?concept <" + RDFS.subClassOf.toString() + "> <http://semoss.org/ontologies/Concept> } "
//				+ "{?property <" + RDF.type.toString() + "> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ "{?concept <" + OWL.DATATYPEPROPERTY.toString() + "> ?property} "
//				+ "{?property ?x ?propertyConceptual} "
//				+ "}";
//
//		wrapper = WrapperManager.getInstance().getRawWrapper(this.rfse, propertiesCorrectFormat);
////		while(wrapper.hasNext()) {
////			System.out.println(Arrays.toString(wrapper.next().getRawValues()));
////		}
//		if(wrapper.hasNext()) {
//			// lets test the values now
//			IHeadersDataRow stmt = wrapper.next();
//			Object[] rawVal = stmt.getRawValues();
//			String propConceptualUri = rawVal[0] + "";
//
//			// based on the new version
//			// the properties do not need to be unique across concepts
//			// so we append the tableName at the end of the uri
//			// so if the className is "contains" we know we need to modernize
//
//			if(Utility.getClassName(propConceptualUri).equals("Contains")) {
//				return true;
//			}
//
//			// it doesn't have contains, i guess we are all up to date
//
//			return false;
//		} else {
//			// what? you have conceptual on concepts but not properties
//			// that is messed up... don't even know how that happens
//			
//			// wait.. above is dumb, db could just have 0 properties
//			// which would obvious make sense why there are no conceptual on properties
//			
//			String anyProperties = "SELECT DISTINCT ?property WHERE { "
//					+ "{?concept <" + RDFS.subClassOf.toString() + "> <http://semoss.org/ontologies/Concept> } "
//					+ "{?property <" + RDF.type.toString() + "> <http://semoss.org/ontologies/Relation/Contains>} "
//					+ "{?concept <" + OWL.DATATYPEPROPERTY.toString() + "> ?property} "
//					+ "}";
//			wrapper = WrapperManager.getInstance().getRawWrapper(this.rfse, anyProperties);
//			if(wrapper.hasNext()) {
//				// okay, we confirmed that this is messed up
//				return true;
//			} else {
//				// cool, no props at all
//				// we be good
//				return false;
//			}
//		}
//	}
//
//
//
//
//}
