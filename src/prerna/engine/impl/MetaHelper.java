//package prerna.engine.impl;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.StringWriter;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeMap;
//import java.util.TreeSet;
//import java.util.Vector;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.openrdf.model.vocabulary.RDFS;
//import org.openrdf.repository.RepositoryException;
//import org.openrdf.rio.RDFHandlerException;
//import org.openrdf.rio.rdfxml.RDFXMLWriter;
//
//import com.hp.hpl.jena.vocabulary.OWL;
//
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.IExplorable;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.api.impl.util.AbstractOwler;
//import prerna.engine.api.impl.util.Owler;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.nameserver.utility.MetamodelVertex;
//import prerna.query.interpreters.IQueryInterpreter;
//import prerna.query.querystruct.AbstractQueryStruct;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.Constants;
//import prerna.util.Utility;
//
//public class MetaHelper implements IExplorable {
//
//	private static final Logger classLogger = LogManager.getLogger(MetaHelper.class);
//
//	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
//	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
//	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";
//	
//	public RDFFileSesameEngine baseDataEngine = null;
//	IDatabaseEngine.DATABASE_TYPE dbType = IDatabaseEngine.DATABASE_TYPE.RDBMS;
//	String engineName = null;
//	
//	private static final String FROM_SPARQL = "SELECT DISTINCT ?entity WHERE { "
//			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//			+ "{?x ?rel  ?y} "
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
//			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
//			+ "}";
//
//	private static final String TO_SPARQL = "SELECT DISTINCT ?entity WHERE { "
//			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//			+ "{?x ?rel ?y} "
//			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
//			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
//			+ "}";
//	
//
//	public MetaHelper(RDFFileSesameEngine baseDataEngine, IDatabaseEngine.DATABASE_TYPE dbType, String engineName)
//	{
//		this.baseDataEngine = baseDataEngine;
//		if(dbType !=  null)
//			this.dbType = dbType;
//		if(engineName != null)
//			this.engineName = engineName;
//		else
//			this.engineName = "Unassigned";
//	}
//
//	@Override
//	public Vector<String> getFromNeighbors(String physicalNodeType, int neighborHood) {
//		// this is where this node is the from node
//		//String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI + Utility.getInstanceName(nodeType), false);
//		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
//		+ "{?node ?rel ?start}}";
//		return Utility.getVectorOfReturn(query, baseDataEngine, true);
//	}
//
//	public Vector<String[]> getFromNeighborsWithRelation(String physicalNodeType, int neighborHood) {
//		// this is where this node is the from node
//		//String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI + Utility.getInstanceName(nodeType), false);
//		String query = "SELECT DISTINCT ?node ?rel WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
//		+ "{?node ?rel ?start}}";
//		return Utility.getVectorArrayOfReturn(query, baseDataEngine, true);
//	}
//
//	// gets the to nodes
//	public Vector<String> getToNeighbors(String physicalNodeType, int neighborHood) {
//		// this is where this node is the to node
//		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
//		+ "{?start ?rel ?node}}";
//		return Utility.getVectorOfReturn(query, baseDataEngine, true);
//	}
//
//	// gets the from and to nodes
//	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
//		Vector<String> from = getFromNeighbors(nodeType, 0);
//		Vector<String> to = getToNeighbors(nodeType, 0);
//		from.addAll(to);
//		return from;
//	}
//
//	@Override
//	public String getOWLDefinition() {
//		StringWriter output = new StringWriter();
//		try {
//			baseDataEngine.getRc().export(new RDFXMLWriter(output));
//		} catch (RepositoryException re) {
//			classLogger.error(Constants.STACKTRACE, re);
//		} catch (RDFHandlerException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		return output.toString();
//	}
//	
//	@Override
//	public String getOwlFilePath() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void setOwlFilePath(String owlFilePath) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void commitOWL() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void addProperty(String key, String value) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public String getProperty(String key) {
//		return null;
//	}
//
//	@Override
//	public Vector<String> executeInsightQuery(String sparqlQuery,
//			boolean isDbQuery) {
//		return null;
//	}
//
//	@Override
//	public String getNodeBaseUri() {
//		String baseUri = null;
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, GET_BASE_URI_FROM_OWL);
//			if(wrapper.hasNext()) {
//				IHeadersDataRow data = wrapper.next();
//				baseUri = data.getRawValues()[0] + "";
//			}
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
//		if(baseUri == null){
//			baseUri = Constants.CONCEPT_URI;
//		}
//		return baseUri;
//	}
//
//	public Vector<String> getConcepts() {
//		String query = "SELECT ?concept WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ " Filter(?concept != <http://semoss.org/ontologies/Concept>) }";
//		return Utility.getVectorOfReturn(query, baseDataEngine, true);
//	}
//	
//	/**
//	 * Runs a select query on the base data engine of this engine
//	 */
//	public Object execOntoSelectQuery(String query) {
//		classLogger.debug("Running select query on base data engine of " + this.engineName);
//		classLogger.debug("Query is " + query);
//		return this.baseDataEngine.execQuery(query);
//	}
//
//	/**
//	 * Runs insert query on base data engine of this engine
//	 */
//	public void ontoInsertData(String query) {
//		classLogger.debug("Running insert query on base data engine of " + this.engineName);
//		classLogger.debug("Query is " + query);
//		baseDataEngine.insertData(query);
//	}
//
//	/**
//	 * This method runs an update query on the base data engine which contains all owl and metamodel information
//	 */
//	public void ontoRemoveData(String query) {
//		classLogger.debug("Running update query on base data engine of " + this.engineName);
//		classLogger.debug("Query is " + query);
//		baseDataEngine.removeData(query);
//	}
//
//	@Override
//	public String getDataTypes(String uri) {
//		String query = "SELECT DISTINCT ?TYPE WHERE { {<" + uri + "> <" + RDFS.CLASS.toString() + "> ?TYPE} }";
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			if(wrapper.hasNext()) {
//				return wrapper.next().getValues()[0].toString();
//			}
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
//		return null;
//	}
//
//	@Override
//	public Map<String, String> getDataTypes(String... uris) {
//		StringBuilder bindBuilder = new StringBuilder();
//		for(String uri : uris) {
//			bindBuilder.append("(<").append(uri).append(">)");	
//		}
//		String query = "SELECT DISTINCT ?NODE ?TYPE WHERE { {?NODE <" + RDFS.CLASS.toString() + "> ?TYPE} } ";
//		String bindings = bindBuilder.toString();
//		if(!bindings.isEmpty()) {
//			query += "BINDINGS ?NODE {" + bindings + "}";
//		}
//		// results to be stored
//		Map<String, String> retMap = new Hashtable<>();
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			while(wrapper.hasNext()) {
//				Object[] row = wrapper.next().getValues();
//				String node = row[0].toString();
//				String type = row[1].toString();
//				retMap.put(node, type);
//			}
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
//		return retMap;
//	}
//	
//	@Override
//	public String getAdtlDataTypes(String uri){
//		String query = "SELECT DISTINCT ?ADTLTYPE WHERE { {<" + uri + "> <" + Owler.ADDITIONAL_DATATYPE_RELATION_URI + "> ?ADTLTYPE} }";
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			if(wrapper.hasNext()) {
//				IHeadersDataRow row = wrapper.next();
//				String adtlType = row.getValues()[0].toString().replace("ADTLTYPE:", "");
//				return adtlType;
//			}
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
//		return null;
//	}
//	
//	@Override
//	public Map<String, String> getAdtlDataTypes(String... uris){
//		StringBuilder bindBuilder = new StringBuilder();
//		for(String uri : uris) {
//			bindBuilder.append("(<").append(uri).append(">)");	
//		}
//		String query = "SELECT DISTINCT ?NODE ?ADTLTYPE WHERE { {?NODE <" + Owler.ADDITIONAL_DATATYPE_RELATION_URI + "> ?ADTLTYPE} } ";
//		String bindings = bindBuilder.toString();
//		if(!bindings.isEmpty()) {
//			query += "BINDINGS ?NODE {" + bindings + "}";
//		}
//		// results to be stored
//		Map<String, String> retMap = new Hashtable<>();
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			while(wrapper.hasNext()) {
//				Object[] row = wrapper.next().getValues();
//				String node = row[0].toString();
//				String type = row[1].toString();
//				if (type != null && !type.equals("")) {
//					type = type.replace("ADTLTYPE:", "");
//					retMap.put(node, type);
//				}
//				retMap.put(node, type);
//			}
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
//		return retMap;
//	}
//
//	@Override
//	public Map<String, Object[]> getMetamodel() {
//		// create this from the query struct
//		Map<String, MetamodelVertex> tableToVert = new TreeMap<>();
//
//		String getSelectorsInformation = "SELECT DISTINCT ?concept ?property WHERE { "
//				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ "OPTIONAL {"
//				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + CONTAINS_BASE_URI + "> } "
//				+ "{?concept <" + OWL.DatatypeProperty.toString() + "> ?property } "
//				+ "}" // END OPTIONAL
//				+ "}"; // END WHERE
//
//		// execute the query and loop through and add the nodes and props
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getSelectorsInformation);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow hrow = wrapper.next();
//				Object[] raw = hrow.getRawValues();
//				if(raw[0].toString().equals("http://semoss.org/ontologies/Concept")) {
//					continue;
//				}
//
//				String concept = Utility.getInstanceName(raw[0].toString());
//				Object property = raw[1];
//
//				if(!tableToVert.containsKey(concept)) {
//					MetamodelVertex vert = new MetamodelVertex(concept);
//					tableToVert.put(concept, vert);
//				}
//
//				if(property != null && !property.toString().isEmpty()) {
//					tableToVert.get(concept).addProperty(Utility.getClassName(property.toString()));
//				}
//			}
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
//		List<Map<String, String>> relationships = new Vector<>();
//
//		// query to get all the relationships 
//		String getRelationshipsInformation = "SELECT DISTINCT ?fromConceptualConcept ?rel ?toConceptualConcept WHERE { "
//				+ "{?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
//				+ "{?rel <" + RDFS.SUBPROPERTYOF.toString() + "> <http://semoss.org/ontologies/Relation>} "
//				+ "{?fromConcept ?rel ?toConcept} "
//				+ "{?fromConcept <http://semoss.org/ontologies/Relation/Conceptual> ?fromConceptualConcept }"
//				+ "{?toConcept <http://semoss.org/ontologies/Relation/Conceptual> ?toConceptualConcept }"
//				+ "}"; // END WHERE
//
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getRelationshipsInformation);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow hrow = wrapper.next();
//				Object[] row = hrow.getValues();
//
//				if(hrow.getRawValues()[1].toString().equals("http://semoss.org/ontologies/Relation")) {
//					continue;
//				}
//
//				String fromConcept = row[0].toString();
//				String rel = row[1].toString();
//				String toConcept = row[2].toString();
//
//				Map<String, String> edgeMap = new TreeMap<>();
//				edgeMap.put("source", fromConcept);
//				edgeMap.put("target", toConcept + "");
//				edgeMap.put("rel", rel);
//				relationships.add(edgeMap);
//			}
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
//		Map<String, Object[]> retObj = new Hashtable<>();
//		retObj.put("nodes", tableToVert.values().toArray());
//		retObj.put("edges", relationships.toArray());
//		return retObj;
//	}
//
//	@Override
//	public File getOwlPositionFile() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public IQueryInterpreter getQueryInterpreter() {
//		return null;
//	}
//
//	@Override
//	public boolean isBasic() {
//		return true;
//	}
//
//	@Override
//	public void setBasic(boolean isBasic) {
//		// this is always basic - used only for OWL
//	}
//
//	@Override
//	public RDFFileSesameEngine getBaseDataEngine() {
//		return this.baseDataEngine;
//	}
//
//	@Override
//	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
//		this.baseDataEngine = baseDataEngine;
//	}
//	
//	
//	////////////////////////////////////////////////////////////////////////////////////////////
//	
//	/*
//	 * NEW PIXEL TO REPLACE CONCEPTUAL NAMES
//	 */
//	
//	@Override
//	public List<String> getPixelConcepts() {
//		String query = "SELECT ?pixelName WHERE {"
//				+ " {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ " {?concept <http://semoss.org/ontologies/Relation/Pixel> ?pixelName }"
//				+ " }";
//		return Utility.getVectorOfReturn(query, baseDataEngine, false);
//	}
//	
//	@Override
//	public List<String> getPixelSelectors(String conceptPixelName) {
//		// first grab the concept if it has data
//		String query = "SELECT DISTINCT ?pixelName WHERE { "
//				+ " BIND(<http://semoss.org/ontologies/Concept/" + conceptPixelName + "> as ?concept) "
//				+ " {?concept <http://semoss.org/ontologies/Relation/Pixel> ?pixelName }"
//				+ " FILTER NOT EXISTS {?concept <http://www.w3.org/2000/01/rdf-schema#domain> \"noData\" }"
//				+ " }";
//		// then grab the properties of the concept which always have data
//		Vector<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, false);
//		query = "SELECT DISTINCT ?pixelName WHERE { "
//				+ " BIND(<http://semoss.org/ontologies/Concept/" + conceptPixelName + "> as ?concept) "
//				+ " {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//				+ " {?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ " {?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//				+ " {?property <http://semoss.org/ontologies/Relation/Pixel> ?pixelName}"
//				+ " }";
//		Vector<String> pArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
//		for(String p : pArr) {
//			retArr.add(conceptPixelName + "__" + Utility.getClassName(p));
//		}
//		
//		return retArr;
//	}
//	
//	@Override
//	public List<String> getPropertyPixelSelectors(String conceptPixelName) {
//		// then grab the properties of the concept which always have data
//		String query = "SELECT DISTINCT ?pixelName WHERE { "
//				+ " BIND(<http://semoss.org/ontologies/Concept/" + conceptPixelName + "> as ?concept) "
//				+ " {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
//				+ " {?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ " {?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//				+ " {?property <http://semoss.org/ontologies/Relation/Pixel> ?pixelName}"
//				+ " }";
//		List<String> retArr = new Vector<>();
//		Vector<String> pArr = Utility.getVectorOfReturn(query, baseDataEngine, false);
//		for(String p : pArr) {
//			retArr.add(conceptPixelName + "__" + p);
//		}
//		
//		return retArr;
//	}
//	
//	@Override
//	public List<String> getPhysicalConcepts() {
//		String query = "SELECT ?concept WHERE {"
//						+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//						+ "Filter(?concept != <http://semoss.org/ontologies/Concept>)"
//						+ "}";
//		return Utility.getVectorOfReturn(query, baseDataEngine, true);
//	}
//	
//	@Override
//	public List<String[]> getPhysicalRelationships() {
//		String query = "SELECT DISTINCT ?start ?end ?rel WHERE { "
//				+ "{?start <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ "{?end <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
//				+ "{?start ?rel ?end}"
//				+ "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)"
//				+ "Filter(?rel != <http://semoss.org/ontologies/Relation>)"
//				+ "}";
//		return Utility.getVectorArrayOfReturn(query, baseDataEngine, true);
//	}
//	
//	@Override
//	public List<String> getPropertyUris4PhysicalUri(String physicalUri) {
//		String query = "SELECT DISTINCT ?property WHERE { "
//					+ "BIND(<" + physicalUri + "> AS ?concept) "
//					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
////					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//					+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
//					+ "}";
//		return Utility.getVectorOfReturn(query, baseDataEngine, true);
//	}
//	
//	@Override
//	public String getPhysicalUriFromPixelSelector(String pixelSelector) {
//		String semossConceptName = pixelSelector;
//		String semossPropertyName = null;
//		if(semossConceptName.contains("__")) {
//			String[] split = pixelSelector.split("__");
//			semossConceptName = split[0];
//			semossPropertyName = split[1];
//			
//			// accounting if we are using the prim key placeholder
//			if(semossPropertyName.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
//				semossPropertyName = null;
//			}
//		}
//		
//		String query = null;
//		if(semossPropertyName == null) {
//			// this is just a concept
//			query = "SELECT DISTINCT ?concept WHERE { "
//					+ " BIND(<http://semoss.org/ontologies/Concept/" + semossConceptName + "> as ?pixelName) "
//					+ " {?concept <http://semoss.org/ontologies/Relation/Pixel> ?pixelName } "
//					+ " }";
//		} else {
//			// this is a property
//			query = "SELECT DISTINCT ?property WHERE { "
//					+ " BIND(<http://semoss.org/ontologies/Relation/Contains/" + semossPropertyName + "/" + semossConceptName + "> as ?pixelName) "
//					+ " {?property <http://semoss.org/ontologies/Relation/Pixel> ?pixelName } "
//					+ " }";
//		}
//		
//		List<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
//		if(!retArr.isEmpty()) {
//			return retArr.get(0);
//		}
//		return null;
//	}
//
//	@Override
//	@Deprecated
//	/**
//	 * We cannot use this cause of the fact that we have not updated the OWL triples
//	 * for a RDF engine for the properties to contain the Concept in the URL (which would make it unique)
//	 * Example: Right now we have http://semoss.org/ontologies/Relation/Contains/Description as a 
//	 * property which could point to multiple concepts
//	 */
//	public String getPixelUriFromPhysicalUri(String physicalUri) {
//		String query = "SELECT DISTINCT ?pixel WHERE { "
//					+ " BIND(<" + physicalUri + "> as ?physicalUri) "
//					+ " {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
//					+ " }";
//		List<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
//		if(!retArr.isEmpty()) {
//			if(retArr.size() > 1) {
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + physicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + physicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + physicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + physicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + physicalUri);
//			}
//			return retArr.get(0);
//		}
//		return null;
//	}
//	
//	@Override
//	public String getConceptPixelUriFromPhysicalUri(String conceptPhysicalUri) {
//		String query = "SELECT DISTINCT ?pixel WHERE { "
//				+ " BIND(<" + conceptPhysicalUri + "> as ?physicalUri) "
//				+ " {?physicalUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ " {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
//				+ " }";
//		List<String> retArr = Utility.getVectorOfReturn(query, baseDataEngine, true);
//		if(!retArr.isEmpty()) {
//			if(retArr.size() > 1) {
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + conceptPhysicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + conceptPhysicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + conceptPhysicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + conceptPhysicalUri);
//				classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + conceptPhysicalUri);
//			}
//			return retArr.get(0);
//		}
//		return null;
//	}
//
//	@Override
//	/**
//	 * This is so annoying... no simple work around for the issue with {@link #getPixelUriFromPhysicalUri(String)} 
//	 */
//	public String getPropertyPixelUriFromPhysicalUri(String conceptPhysicalUri, String propertyPhysicalUri) {
//		String query = "SELECT DISTINCT ?pixel ?parentPixel WHERE { "
//				+ " BIND(<" + propertyPhysicalUri + "> as ?propertyPhysicalUri) "
//				+ " BIND(<" + conceptPhysicalUri + "> as ?conceptPhysicalUri) "
//				+ " {?conceptPhysicalUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//				+ " {?conceptPhysicalUri <http://semoss.org/ontologies/Relation/Pixel> ?parentPixel } "
//				+ " {?propertyPhysicalUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//				+ "	{?conceptPhysicalUri <http://www.w3.org/2002/07/owl#DatatypeProperty> ?propertyPhysicalUri} "
//				+ " {?propertyPhysicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
//				+ " }";
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			String conceptName = null;
//			if(wrapper.hasNext()) {
//				Object[] raw = wrapper.next().getRawValues();
//				String propPixel = raw[0].toString();
//				String parentPixel = raw[1].toString();
//				conceptName = Utility.getInstanceName(parentPixel);
//				if(Utility.getInstanceName(propPixel).equals(conceptName)) {
//					return propPixel;
//				}
//			}
//			classLogger.debug("UGH... WHY ARE YOU NOT UNIQUE AS PHYSICAL!!! " + conceptPhysicalUri + " ::: " + propertyPhysicalUri);
//			while(wrapper.hasNext()) {
//				Object[] raw = wrapper.next().getRawValues();
//				String propPixel = raw[0].toString();
//				if(Utility.getInstanceName(propPixel).equals(conceptName)) {
//					return propPixel;
//				}
//			}
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
//		return null;
//	}
//	
//	@Override
//	public String getPixelSelectorFromPhysicalUri(String physicalUri) {
//		String query = "SELECT DISTINCT ?pixel ?type WHERE { "
//					+ " {"
//						+ " BIND(\"concept\" as ?type) "
//						+ " BIND(<" + physicalUri + "> as ?physicalUri) "
//						+ " {?physicalUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
//						+ " {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
//					+ " }"
//					+ " UNION "
//					+ "	{"
//						+ " BIND(\"property\" as ?type) "
//						+ " BIND(<" + physicalUri + "> as ?physicalUri) "
//						+ " {?physicalUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }"
//						+ " {?physicalUri <http://semoss.org/ontologies/Relation/Pixel> ?pixel } "
//					+ "	}"
//					+ " }";
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			while(wrapper.hasNext()) {
//				Object[] raw = wrapper.next().getRawValues();
//				if(raw[1].toString().contains("concept")) {
//					return Utility.getInstanceName(raw[0].toString());
//				} else {
//					String parent = Utility.getInstanceName(raw[0].toString());
//					String child = Utility.getClassName(raw[0].toString());
//					return parent + "__" + child;
//				}
//			}
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
//		return null;
//	}
//	
//	@Override
//	public String getConceptualName(String physicalUri) {
//		String query = "SELECT DISTINCT ?conceptual WHERE { "
//				+ "BIND(<" + physicalUri + "> AS ?uri) "
//				+ "{?uri <" + AbstractOwler.CONCEPTUAL_RELATION_URI + "> ?conceptual } "
//				+ "}";
//
//		String conceptualName = null;
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			if(wrapper.hasNext()) {
//				conceptualName = wrapper.next().getValues()[0].toString();
//			}
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
//		return conceptualName;
//	}
//
//	@Override
//	public Set<String> getLogicalNames(String physicalUri) {
//		String query = "SELECT DISTINCT ?logical WHERE { "
//				+ "BIND(<" + physicalUri + "> AS ?uri) "
//				+ "{?uri <" + OWL.sameAs.toString() + "> ?logical } "
//				+ "}";
//
//		Set<String> logicals = new TreeSet<>();
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			while(wrapper.hasNext()) {
//				logicals.add(wrapper.next().getValues()[0].toString());
//			}
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
//		return logicals;
//	}
//	
//	@Override
//	public String getDescription(String physicalUri) {
//		String query = "SELECT DISTINCT ?description WHERE { "
//				+ "BIND(<" + physicalUri + "> AS ?uri) "
//				+ "{?uri <" + RDFS.COMMENT.toString() + "> ?description } "
//				+ "}";
//
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			if(wrapper.hasNext()) {
//				return wrapper.next().getValues()[0].toString();
//			}
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
//		return null;
//	}
//	
//	@Override
//	@Deprecated
//	public String getLegacyPrimKey4Table(String physicalUri) {
//		String query = "SELECT DISTINCT ?value WHERE { "
//				+ "BIND(<" + physicalUri + "> AS ?uri) "
//				+ "{?uri <" + AbstractOwler.LEGACY_PRIM_KEY_URI + "> ?value } "
//				+ "}";
//
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
//			if(wrapper.hasNext()) {
//				return wrapper.next().getValues()[0].toString();
//			}
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
//		return null;
//	}
//
//}
