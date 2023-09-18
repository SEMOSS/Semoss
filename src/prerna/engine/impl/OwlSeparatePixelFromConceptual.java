//package prerna.engine.impl;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Vector;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.openrdf.model.vocabulary.RDFS;
//
//import com.google.gson.Gson;
//
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.api.impl.util.AbstractOwler;
//import prerna.engine.api.impl.util.Owler;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.nameserver.DeleteFromMasterDB;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.Constants;
//import prerna.util.Utility;
//import prerna.util.gson.GsonUtility;
//
//@Deprecated
//public class OwlSeparatePixelFromConceptual {
//
//	private static final Logger classLogger = LogManager.getLogger(OwlSeparatePixelFromConceptual.class);
//	
//	public static void fixOwl(Properties prop) {
//		String uniqueAppName = SmssUtilities.getUniqueName(prop);
//		File owlFile = SmssUtilities.getOwlFile(prop);
//		if(owlFile != null && owlFile.exists()) {
//			// owl is stored as RDF/XML file
//			RDFFileSesameEngine rfse = new RDFFileSesameEngine();
//			
//			rfse.openFile(owlFile.getAbsolutePath(), null, null);
//	
//			boolean requiresFix = requiresFix(rfse);
//			if(requiresFix) {
//				String eClass = prop.get(Constants.ENGINE_TYPE).toString().toLowerCase();
//				boolean rdbms = eClass.contains("rdbms") || eClass.contains("impala");
//				try {
//					FileUtils.copyFile(owlFile, new File(owlFile.getAbsolutePath() + "_SaveCopy"));
//				} catch (IOException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//				System.out.println("REQUIRE FIX FOR " + uniqueAppName);
//				System.out.println("REQUIRE FIX FOR " + uniqueAppName);
//				System.out.println("REQUIRE FIX FOR " + uniqueAppName);
//				System.out.println("REQUIRE FIX FOR " + uniqueAppName);
//				System.out.println("REQUIRE FIX FOR " + uniqueAppName);
//				System.out.println("REQUIRE FIX FOR " + uniqueAppName);
//				try {
//					performFix(rfse, owlFile, rdbms);
//					// drop from local master
//					if(!uniqueAppName.equals(Constants.LOCAL_MASTER_DB) &&
//						!uniqueAppName.equals(Constants.SECURITY_DB)) {
//						DeleteFromMasterDB deleter = new DeleteFromMasterDB();
//						deleter.deleteEngineRDBMS(prop.getProperty(Constants.ENGINE));
//					}
//				} catch (Exception e) {
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName);
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName );
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName );
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName );
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName );
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName );
//					System.out.println("ERROR occurred TRYING TO FIX " + uniqueAppName );
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//	}
//	
//	private static void performFix(RDFFileSesameEngine rfse, File owlFile, boolean rdbms) throws Exception {
//		Gson gson = GsonUtility.getDefaultGson(true);
//		
//		// i need to grab all the concepts - done
//		// i need to grab all the conceptual names - done
//		// i need to grab all the properties - done
//		// i need to grab all the property conceptual names - done
//		// i need to grab all the relationships - done
//		// i need the data types - done
//		// the BASE URI
//		// then i will delete everything from the rfse
//		// then i will insert new triples via owler
//		
//		String baseUri = getBaseUri(rfse);
//		
//		Map<String, String> physicalConceptsAndConceptuals = getConceptPhsysicalToConceptual(rfse);
//		System.out.println("Concept physical to conceptual");
//		System.out.println(gson.toJson(physicalConceptsAndConceptuals));
//		
//		Map<String, List<String>> physicalConceptToProperties = getConceptToPropertyPhsysicals(rfse);
//		System.out.println("Concept physical to properties list");
//		System.out.println(gson.toJson(physicalConceptToProperties));
//
//		Map<String, Map<String, String>> physicalConceptToPropertyAndConceptuals = getPhysicalPropertiesAndConceptual(rfse);
//		System.out.println("Concept physical to properties physical to conceptual");
//		System.out.println(gson.toJson(physicalConceptToPropertyAndConceptuals));
//		
//		List<String[]> relationships = getRelationships(rfse);
//		System.out.println("Relationships");
//		System.out.println(gson.toJson(relationships));
//		
//		Map<String, String> phsysicalConceptDataType = getPhysicalConceptDataType(rfse);
//		System.out.println("Concept data types");
//		System.out.println(gson.toJson(phsysicalConceptDataType));
//		
//		System.out.println("Property data types");
//		Map<String, Map<String, String>> physicalConceptToPropertyAndDataTypes = getPhysicalConceptToPropertyAndDataTypes(rfse);
//		System.out.println(gson.toJson(physicalConceptToPropertyAndDataTypes));
//
//		deleteAllTriples(rfse);
//		
//		IDatabaseEngine.DATABASE_TYPE dbType = rdbms ? IDatabaseEngine.DATABASE_TYPE.RDBMS : IDatabaseEngine.DATABASE_TYPE.SESAME;
//		Owler owler = new Owler(owlFile.getAbsolutePath(), dbType);
//		
//		// add all the concepts and properties
//		for(String conceptPhysicalURI : physicalConceptsAndConceptuals.keySet()) {
//			// add the concept/table name
//			String tableName = Utility.getInstanceName(conceptPhysicalURI);
//			if(rdbms) {
//				owler.addConcept(tableName);
//
//				// i need to set the legacy prim key for the correct SQL creation
//				String legacyPrimKey = Utility.getClassName(conceptPhysicalURI);
//				owler.addLegacyPrimKey(tableName, legacyPrimKey);
//
//				// i need to add the hidden column as a prop
//				String type = phsysicalConceptDataType.get(conceptPhysicalURI);
//				// i will not use the conceptual name in this case
//				owler.addProp(tableName, legacyPrimKey, type);
//			} else {
//				String conceptualUri = physicalConceptsAndConceptuals.get(conceptPhysicalURI);
//				String type = phsysicalConceptDataType.get(conceptPhysicalURI);
//				owler.addConcept(tableName, type, Utility.getInstanceName(conceptualUri));
//			}
//			
//			// now address the properties for this concept - if there are properties
//			List<String> propertiesURI = physicalConceptToProperties.get(conceptPhysicalURI);
//			if(propertiesURI != null && !propertiesURI.isEmpty()) {
//				Map<String, String> propertyURIDataTypeMap = physicalConceptToPropertyAndDataTypes.get(conceptPhysicalURI);
//				Map<String, String> propertyURIConceptualMap = physicalConceptToPropertyAndConceptuals.get(conceptPhysicalURI);
//				for(String propertyURI : propertiesURI) {
//					String type = propertyURIDataTypeMap.get(propertyURI);
//					String conceptualUri = propertyURIConceptualMap.get(propertyURI);
//					if(rdbms) {
//						owler.addProp(tableName, Utility.getClassName(propertyURI), type, null, Utility.getClassName(conceptualUri));
//					} else {
//						owler.addProp(tableName, Utility.getInstanceName(propertyURI), type, null, Utility.getClassName(conceptualUri));
//					}
//				}
//			}
//		}
//		
//		// add all the relationships
//		for(String[] rel : relationships) {
//			// since RDF and RDBMS both keep the table name at the end
//			// we just need to loop through and add
//			String fromTable = Utility.getInstanceName(rel[0]);
//			String toTable = Utility.getInstanceName(rel[2]);
//			String predicate = Utility.getInstanceName(rel[1]);
//			owler.addRelation(fromTable, toTable, predicate);
//		}
//		
//		if(baseUri != null) {
//			String baseUriInput = baseUri.replace("/"+AbstractOwler.DEFAULT_NODE_CLASS+"/", "");
//			owler.addCustomBaseURI(baseUriInput);
//		}
//		
//		// save!
//		owler.commit();
//		try {
//			owler.export();
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	//////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////////////////////////////////////////
//
//	private static String getBaseUri(RDFFileSesameEngine rfse) throws Exception {
//		String query = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";
//		IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		if(wrap.hasNext()) {
//			IHeadersDataRow data = wrap.next();
//			return data.getRawValues()[0] + "";
//		}
//		return null;
//	}
//
//	/**
//	 * For each concept, get its URI and its conceptual URI
//	 * @param rfse
//	 * @return
//	 * @throws Exception 
//	 */
//	private static Map<String, String> getConceptPhsysicalToConceptual(RDFFileSesameEngine rfse) throws Exception {
//		Map<String, String> retMap = new HashMap<String, String>();
//		String query = "select ?concept ?conceptual where {"
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?concept <" + AbstractOwler.CONCEPTUAL_RELATION_URI + "> ?conceptual} "
//				+ "}";
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		while(wrapper.hasNext()) {
//			Object[] row = wrapper.next().getRawValues();
//			retMap.put(row[0].toString(), row[1].toString());
//		}
//		
//		return retMap;
//	}
//	
//	/**
//	 * For each concept, gets its list of properties
//	 * @param rfse
//	 * @return
//	 * @throws Exception 
//	 */
//	private static Map<String, List<String>> getConceptToPropertyPhsysicals(RDFFileSesameEngine rfse) throws Exception {
//		Map<String, List<String>> retMap = new HashMap<String, List<String>>();
//		String query = "select ?concept ?property where {"
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//				+ "}";
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		while(wrapper.hasNext()) {
//			Object[] row = wrapper.next().getRawValues();
//			String cUri = row[0].toString();
//			
//			List<String> propUriList = null;
//			if(retMap.containsKey(cUri)) {
//				propUriList = retMap.get(cUri);
//			} else {
//				propUriList = new Vector<String>();
//				retMap.put(cUri, propUriList);
//			}
//			propUriList.add(row[1].toString());
//		}
//		
//		return retMap;
//	}
//	
//	/**
//	 * For each concept, get its URI and its conceptual URI
//	 * @param rfse
//	 * @return
//	 * @throws Exception 
//	 */
//	private static Map<String, Map<String, String>> getPhysicalPropertiesAndConceptual(RDFFileSesameEngine rfse) throws Exception {
//		Map<String, Map<String, String>> retMap = new HashMap<String, Map<String, String>>();
//		String query = "select ?concept ?property ?conceptual where {"
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//				+ "{?property <" + AbstractOwler.CONCEPTUAL_RELATION_URI + "> ?conceptual} "
//				+ "}";
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		while(wrapper.hasNext()) {
//			Object[] row = wrapper.next().getRawValues();
//			String cUri = row[0].toString();
//
//			Map<String, String> propMap = null;
//			if(retMap.containsKey(cUri)) {
//				propMap = retMap.get(cUri);
//			} else {
//				propMap = new HashMap<String, String>();
//				retMap.put(cUri, propMap);
//			}
//			
//			propMap.put(row[1].toString(), row[2].toString());
//		}
//		
//		return retMap;
//	}
//	
//	/**
//	 * Get the relationships
//	 * @param rfse
//	 * @return
//	 * @throws Exception 
//	 */
//	private static List<String[]> getRelationships(RDFFileSesameEngine rfse) throws Exception {
//		List<String[]> rels = new Vector<String[]>();
//		String query = "select ?concept1 ?rel ?concept2 where {"
//				+ "{?concept1 <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?concept2 <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
//				+ "{?concept1 ?rel ?concept2}"
//				+ "}";
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		while(wrapper.hasNext()) {
//			Object[] row = wrapper.next().getRawValues();
//			if(row[1].toString().equals("http://semoss.org/ontologies/Relation")) {
//				continue;
//			}
//			rels.add(new String[] {row[0].toString(), row[1].toString(), row[2].toString()});
//		}
//		
//		return rels;
//	}
//	
//	private static Map<String, String> getPhysicalConceptDataType(RDFFileSesameEngine rfse) throws Exception {
//		Map<String, String> retMap = new HashMap<String, String>();
//		String query = "select ?concept ?type where {"
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?concept <" + RDFS.CLASS.stringValue() + "> ?type} "
//				+ "}";
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		while(wrapper.hasNext()) {
//			IHeadersDataRow data = wrapper.next();
//			Object[] raw = data.getRawValues();
//			Object[] cleanRow = data.getValues();
//
//			String uri = raw[0].toString();
//			String type = cleanRow[1].toString();
//			if(type.contains("TYPE:")) {
//				type = type.replace("TYPE:", "");
//			}
//			
//			retMap.put(uri, type);
//		}
//		
//		return retMap;
//	}
//	
//	private static Map<String, Map<String, String>> getPhysicalConceptToPropertyAndDataTypes(RDFFileSesameEngine rfse) throws Exception {
//		Map<String, Map<String, String>> retMap = new HashMap<String, Map<String, String>>();
//		String query = "select ?concept ?property ?type where {"
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//				+ "{?property <" + RDFS.CLASS.stringValue() + "> ?type} "
//				+ "}";
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//		while(wrapper.hasNext()) {
//			IHeadersDataRow data = wrapper.next();
//			Object[] raw = data.getRawValues();
//			Object[] cleanRow = data.getValues();
//
//			String cUri = raw[0].toString();
//			String pUri = raw[1].toString();
//			String type = cleanRow[2].toString();
//			if(type.contains("TYPE:")) {
//				type = type.replace("TYPE:", "");
//			}
//			
//			Map<String, String> propMap = null;
//			if(retMap.containsKey(cUri)) {
//				propMap = retMap.get(cUri);
//			} else {
//				propMap = new HashMap<String, String>();
//				retMap.put(cUri, propMap);
//			}
//			
//			propMap.put(pUri, type);
//		}
//		
//		return retMap;
//	}
//	
//	private static void deleteAllTriples(RDFFileSesameEngine rfse) {
//		String query = "delete {?s ?p ?o} where {?s ?p ?o}";
//		rfse.removeData(query);
//	}
//	
//	//////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////////////////////////////////////////
//
//	/**
//	 * Determine if we need to fix this OWL
//	 * @param rfse
//	 * @return
//	 * @throws Exception 
//	 */
//	private static boolean requiresFix(RDFFileSesameEngine rfse) {
//		String query = "select ?s ?p ?o where {"
//				+ "bind(<" + AbstractOwler.PIXEL_RELATION_URI + "> as ?p)"
//				+ "{?s ?p ?o}"
//				+ "}";
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//			return !wrapper.hasNext();
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(wrapper != null) {
//				try {
//					wrapper.close();
//				} catch (IOException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		
//		return true;
//	}
//	
//	/*
//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//		String smss = "C:\\workspace\\Semoss_Dev\\db\\MovieDates__2da0688f-fc35-4427-aba5-7bd7b7ac9472.smss";
//		Properties prop = new Properties();
//		FileInputStream fileIn = new FileInputStream(smss);
//		prop.load(fileIn);
//		try {
//			fixOwl(prop);
//		} finally {
//			fileIn.close();
//		}
//		
//		smss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data__133db94b-4371-4763-bff9-edf7e5ed021b.smss";
//		prop = new Properties();
//		fileIn = new FileInputStream(smss);
//		prop.load(fileIn);
//		try {
//			fixOwl(prop);
//		} finally {
//			fileIn.close();
//		}
//		
//	}
//	*/
//	
//}
