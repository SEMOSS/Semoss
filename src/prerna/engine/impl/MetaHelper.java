package prerna.engine.impl;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IEngine;
import prerna.engine.api.IExplorable;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public class MetaHelper implements IExplorable {

	public RDFFileSesameEngine baseDataEngine = null;
	Logger logger = Logger.getLogger(getClass());
	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private transient Map<String, String> tableUriCache = new HashMap<String, String>();
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	IEngine.ENGINE_TYPE engineType = IEngine.ENGINE_TYPE.RDBMS;
	String engineName = null;
	private static final String FROM_SPARQL = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel  ?y} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
			+ "}";

	private static final String TO_SPARQL = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel ?y} "
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
			+ "}";
	
	public RDBMSNativeEngine insightRDBMS = null;

	public MetaHelper(RDFFileSesameEngine baseDataEngine, IEngine.ENGINE_TYPE engineType, String engineName)
	{
		this.baseDataEngine = baseDataEngine;
		if(engineType !=  null)
			this.engineType = engineType;
		if(engineName != null)
			this.engineName = engineName;
		else
			this.engineName = "Unassigned";
	}

	public MetaHelper(RDFFileSesameEngine baseDataEngine, IEngine.ENGINE_TYPE engineType, String engineName, RDBMSNativeEngine insightRDBMS)
	{
		this.baseDataEngine = baseDataEngine;
		if(engineType !=  null)
			this.engineType = engineType;
		if(engineName != null)
			this.engineName = engineName;
		else
			this.engineName = "Unassigned";
		if(insightRDBMS != null)
			this.insightRDBMS = insightRDBMS;
	}

	@Override
	public Vector<String> getPerspectives() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getInsights(String perspective) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getInsights() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<Insight> getInsight(String... id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getFromNeighbors(String physicalNodeType, int neighborHood) {
		// this is where this node is the from node
		//String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI + Utility.getInstanceName(nodeType), false);
		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
		+ "{?node ?rel ?start}}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	public Vector<String[]> getFromNeighborsWithRelation(String physicalNodeType, int neighborHood) {
		// this is where this node is the from node
		//String physicalNodeType = getTransformedNodeName(Constants.DISPLAY_URI + Utility.getInstanceName(nodeType), false);
		String query = "SELECT DISTINCT ?node ?rel WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
		+ "{?node ?rel ?start}}";
		return Utility.getVectorArrayOfReturn(query, baseDataEngine, true);
	}

	// gets the to nodes
	public Vector<String> getToNeighbors(String physicalNodeType, int neighborHood) {
		// this is where this node is the to node
		String query = "SELECT DISTINCT ?node WHERE { BIND(<" + physicalNodeType + "> AS ?start) {?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
		+ "{?start ?rel ?node}}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	// gets the from and to nodes
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		Vector<String> from = getFromNeighbors(nodeType, 0);
		Vector<String> to = getToNeighbors(nodeType, 0);
		from.addAll(to);
		return from;
	}

	@Override
	public void setOWL(String owl) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getOWLDefinition() {
		// TODO Auto-generated method stub
		StringWriter output = new StringWriter();
		try {
			baseDataEngine.getRc().export(new RDFXMLWriter(output));
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output.toString();
	}

	@Override
	public void commitOWL() {
		// TODO Auto-generated method stub

	}

	@Override
	public void addProperty(String key, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getProperty(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IEngine getInsightDatabase() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInsightDefinition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> executeInsightQuery(String sparqlQuery,
			boolean isDbQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNodeBaseUri() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public Vector<String> getConcepts() {
		String query = "SELECT ?concept WHERE {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}
	
	/**
	 * Goes to the owl using a regex sparql query to get the physical uri
	 * @param physicalName e.g. Studio
	 * @return e.g. http://semoss.org/ontologies/Concept/Studio
	 */
	public String getConceptUri4PhysicalName(String physicalName){
		if(tableUriCache.containsKey(physicalName)){
			return tableUriCache.get(physicalName);
		}
		Vector<String> cons = this.getConcepts();
		for(String checkUri : cons){
			if(Utility.getInstanceName(checkUri).equals(physicalName)){
				tableUriCache.put(physicalName, checkUri);
				return checkUri;
			}
		}

		return "unable to get table uri for " + physicalName;
	}

	/**
	 * Runs a select query on the base data engine of this engine
	 */
	public Object execOntoSelectQuery(String query) {
		logger.debug("Running select query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		return this.baseDataEngine.execQuery(query);
	}

	/**
	 * Runs insert query on base data engine of this engine
	 */
	public void ontoInsertData(String query) {
		logger.debug("Running insert query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		baseDataEngine.insertData(query);
	}

	/**
	 * This method runs an update query on the base data engine which contains all owl and metamodel information
	 */
	public void ontoRemoveData(String query) {
		logger.debug("Running update query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		baseDataEngine.removeData(query);
	}

	@Override
	public Vector<String> getConcepts(boolean conceptualNames) {
		String query = "";
		if(!conceptualNames) {
			query = "SELECT ?concept WHERE {"
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
					+ "Filter(?concept != <http://semoss.org/ontologies/Concept>)"
					+ "}";
		} else {
			query = "SELECT DISTINCT (COALESCE(?conceptual, ?concept) AS ?retConcept) WHERE { "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
						+ "OPTIONAL {"
							+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
						+ "}" // end optional for conceptual names if present
					+ "}"; // end where
		}
		
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}
	
	public Vector<String[]> getRelationships(boolean conceptualNames) {
		String query = "";
		if(!conceptualNames) {
			query = "SELECT DISTINCT ?start ?end ?rel WHERE { "
				+ "{?start <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?end <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
				+ "{?start ?rel ?end}"
				+ "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)"
				+ "Filter(?rel != <http://semoss.org/ontologies/Relation>)"
				+ "}";
		} else {
			query = "SELECT DISTINCT ?startC ?endC ?rel WHERE { "
					+ "{?start <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}"
					+ "{?start <http://semoss.org/ontologies/Relation/Conceptual> ?startC}"
					+ "{?end <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}"
					+ "{?end <http://semoss.org/ontologies/Relation/Conceptual> ?endC}"
					+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
					+ "{?start ?rel ?end}"
					+ "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)"
					+ "Filter(?rel != <http://semoss.org/ontologies/Relation>)"
					+ "}";
		}
		
		return Utility.getVectorArrayOfReturn(query, baseDataEngine, true);
	}

	/**
	 * Returns the list of key URIs for the concept
	 * 
	 * @param concept The physical concept URI
	 * @param conceptualNames Whether to return conceptual names
	 * @return List of key URIs
	 */
	public List<String> getKeys4Concept(String concept, Boolean conceptualNames) {
		// get the physical URI for the concept
		String conceptPhysical = getConceptPhysicalUriFromConceptualUri(concept);
		
		String query = null;
		if (conceptualNames) {
			query = "SELECT DISTINCT ?keyConceptual WHERE { "
					+ "BIND(<" + conceptPhysical + "> AS ?concept) "
					+ "BIND(<" + Constants.META_KEY + "> AS ?keyPredicate) "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
					+ "{?concept ?keyPredicate ?key} "
					+ "{?key <http://semoss.org/ontologies/Relation/Conceptual> ?keyConceptual} "
					+ "}";
		} else {
			query = "SELECT DISTINCT ?key WHERE { "
					+ "BIND(<" + conceptPhysical + "> AS ?concept) "
					+ "BIND(<" + Constants.META_KEY + "> AS ?keyPredicate) "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
					+ "{?concept ?keyPredicate ?key} "
					+ "}";
		}
		System.out.println("QUERY ::: " + query);
		return Utility.getVectorOfReturn(query, baseDataEngine, true);	
	}
	
	@Override
	public List<String> getProperties4Concept(String conceptPhysicalUri, Boolean conceptualNames) {
		String query = null;
		// instead of getting the URIs in a set format and having to do a conversion afterwards
		// just have the query get the values directly
		if(conceptualNames) {
			query = "SELECT DISTINCT ?propertyConceptual WHERE { "
					+ "BIND(<" + conceptPhysicalUri + "> AS ?concept) "
					+ "BIND(<http://www.w3.org/2002/07/owl#DatatypeProperty> AS ?propPredicate) "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
					+ "{?concept ?propPredicate ?property} "
//					+ "{?propertyConceptual <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
					+ "{?property <http://semoss.org/ontologies/Relation/Conceptual> ?propertyConceptual} "
					+ "}";
		} else {
			query = "SELECT DISTINCT ?property WHERE { "
					+ "BIND(<" + conceptPhysicalUri + "> AS ?concept) "
					+ "BIND(<http://www.w3.org/2002/07/owl#DatatypeProperty> AS ?propPredicate) "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
					+ "{?concept ?propPredicate ?property} "
					+ "}";
		}
		
		System.out.println("QUERY ::: " + query);
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	@Override
	public String getConceptPhysicalUriFromConceptualUri(String conceptualURI) {
		// the URI is not valid if it doesn't start with http://
		// if it is not valid, assume only the last part of the URI was passed and create the URI
		// the URI is not valid if it doesn't start with http://
		// if it is not valid, assume only the last part of the URI was passed and create the URI
		if(!conceptualURI.startsWith("http://")) {
			conceptualURI = "http://semoss.org/ontologies/Concept/" + conceptualURI;
		}
		
		// this query needs to take into account if the conceptual URI is a concept or a property
		String query = "SELECT DISTINCT ?uri WHERE { "
				+ "BIND(<" + conceptualURI + "> AS ?conceptual) "
				+ "{?uri <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
				+ "{?uri <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual } "
				+ "}";

		Vector<String> queryReturn = Utility.getVectorOfReturn(query, baseDataEngine, true);
		// if it is empty, either the URI is bad or it is already the physical URI
		if(queryReturn.isEmpty()) {
			return null;
		}
		// there should only be one return in the vector since conceptual URIs are a one-to-one match with the physical URIs
		return queryReturn.get(0);
	}
	
	@Override
	public String getPropertyPhysicalUriFromConceptualUri(String conceptualURI, String parentConceptualUri) {
		if(!conceptualURI.startsWith("http://")) {
			conceptualURI = "http://semoss.org/ontologies/Relation/Contains/" + conceptualURI;
			if(parentConceptualUri.startsWith("http://")) {
				conceptualURI += "/" + Utility.getInstanceName(parentConceptualUri);
			} else {
				conceptualURI += "/" + parentConceptualUri;
			}
		}
		
		// this query needs to take into account if the conceptual URI is a concept or a property
		String query = "SELECT DISTINCT ?uri WHERE { "
				+ "BIND(<" + conceptualURI + "> AS ?conceptual) "
				+ "{?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> } "
				+ "{?uri <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual } "
				+ "}";

		Vector<String> queryReturn = Utility.getVectorOfReturn(query, baseDataEngine, true);
		// if it is empty, either the URI is bad or it is already the physical URI
		if(queryReturn.isEmpty()) {
			return null;
		}
		// there should only be one return in the vector since conceptual URIs are a one-to-one match with the physical URIs
		return queryReturn.get(0);
	}

	@Override
	public String getDataTypes(String uri) {
		String query = "SELECT DISTINCT ?TYPE WHERE { {<" + uri + "> <" + RDFS.CLASS.toString() + "> ?TYPE} }";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
		String type = null;
		while(wrapper.hasNext()) {
			type = wrapper.next().getValues()[0].toString();
		}
		
		return type;
	}

	@Override
	public Map<String, String> getDataTypes(String... uris) {
		StringBuilder bindBuilder = new StringBuilder();
		for(String uri : uris) {
			bindBuilder.append("(<").append(uri).append(">)");	
		}
		String query = "SELECT DISTINCT ?NODE ?TYPE WHERE { {?NODE <" + RDFS.CLASS.toString() + "> ?TYPE} } ";
		String bindings = bindBuilder.toString();
		if(!bindings.isEmpty()) {
			query += "BINDINGS ?NODE {" + bindings + "}";
		}
		Map<String, String> retMap = new Hashtable<String, String>();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			String node = row[0].toString();
			String type = row[1].toString();
			retMap.put(node, type);
		}
		
		return retMap;
	}
	
	@Override
	public String getAdtlDataTypes(String uri){
		String cleanUri = uri;
		String query = "SELECT DISTINCT ?ADTLTYPE WHERE { {<" + cleanUri + "> <http://semoss.org/ontologies/Relation/Contains/AdtlDataType> ?ADTLTYPE} }";
			
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
		String adtlType = null;
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			adtlType = row.getValues()[0].toString().replace("ADTLTYPE:", "").replace("((REPLACEMENT_TOKEN))", "/").replace("((SINGLE_QUOTE))", "''").replace("((SPACE))", " ");
		}
		
		return adtlType;
	}
	
	@Override
	public Map<String, String> getAdtlDataTypes(String... uris){
		Map<String, String> retMap = new Hashtable<String, String>();
		String bindings = "";
		for(String uri : uris) {
			String cleanUri = uri;
			bindings += "(<" + cleanUri + ">)";	
		}
		String query = null;
		if(!bindings.isEmpty()) {
			query = "SELECT DISTINCT ?NODE ?ADTLTYPE WHERE { {?NODE <http://semoss.org/ontologies/Relation/Contains/AdtlDataType> ?ADTLTYPE} } BINDINGS ?NODE {" + bindings + "}";
			
		} else {
			// if no bindings, return everything
			query = "SELECT DISTINCT ?NODE ?ADTLTYPE WHERE { {?NODE <http://semoss.org/ontologies/Relation/Contains/AdtlDataType> ?ADTLTYPE} }";
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			String node = row.getRawValues()[0].toString();
			String type = row.getValues()[1].toString();
			if (type != null && type != "") {
				String conceptName = node.substring(node.lastIndexOf("/") + 1);
				type = type.replace("ADTLTYPE:", "").replace("((REPLACEMENT_TOKEN))", "/").replace("((SINGLE_QUOTE))", "'").replace("((SPACE))", " ");
				retMap.put(conceptName + "__" + Utility.getClassName(node), type);
			}
		}
		
		return retMap;
	}

	@Override
	public String getParentOfProperty(String property) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SelectQueryStruct getDatabaseQueryStruct() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object[]> getMetamodel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConceptualUriFromPhysicalUri(String physicalURI) {

		String query = "SELECT DISTINCT ?conceptual WHERE { "
				+ "BIND(<" + physicalURI + "> AS ?uri) "
				+ "{?uri <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual } "
				+ "}"; // end where

		Vector<String> queryReturn = Utility.getVectorOfReturn(query, baseDataEngine, true);
		// there should only be one return in the vector since conceptual URIs are a one-to-one match with the physical URIs
		if(queryReturn.isEmpty()) {
			return physicalURI;
		}
		return queryReturn.get(0);
	}

	@Override
	public String getOWL() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void setInsightDatabase(IEngine insightDatabase) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isBasic() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setBasic(boolean isBasic) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RDFFileSesameEngine getBaseDataEngine() {
		return this.baseDataEngine;
	}

	@Override
	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
		this.baseDataEngine = baseDataEngine;
	}

	@Override
	public Set<String> getLogicalNames(String physicalURI) {
		String query = "SELECT DISTINCT ?logical WHERE { "
				+ "BIND(<" + physicalURI + "> AS ?uri) "
				+ "{?uri <" + OWL.sameAs.toString() + "> ?logical } "
				+ "}";

		Set<String> logicals = new TreeSet<String>();
		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
		while(manager.hasNext()) {
			logicals.add(manager.next().getValues()[0].toString());
		}
		return logicals;
	}
	
	@Override
	public String getDescription(String physicalURI) {
		String query = "SELECT DISTINCT ?description WHERE { "
				+ "BIND(<" + physicalURI + "> AS ?uri) "
				+ "{?uri <" + RDFS.COMMENT.toString() + "> ?description } "
				+ "}";

		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(baseDataEngine, query);
		while(manager.hasNext()) {
			return manager.next().getValues()[0].toString();
		}
		return null;
	}
}
