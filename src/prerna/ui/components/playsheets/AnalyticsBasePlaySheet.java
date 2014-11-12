package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.cluster.LocalOutlierFactorAlgorithm;
import prerna.algorithm.impl.CentralityCalculator;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class AnalyticsBasePlaySheet extends BrowserPlaySheet {
	
	private IEngine engine;
	
	public AnalyticsBasePlaySheet(IEngine engine) {
		this.engine = engine;
	}
	
	public AnalyticsBasePlaySheet() {
		
	}
	
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public void runAnalytics() {
		generateScatter(engine);
		getQuestionsWithoutParams(engine);
		getMostInfluentialInstancesForAllTypes(engine);
	}

	public Hashtable<String, Object> generateScatter(IEngine engine) {
		final String getConceptListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} }";
		final String getConceptsAndInstanceCountsQuery = "SELECT DISTINCT ?entity (COUNT(DISTINCT ?instance) AS ?count) WHERE { {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?entity} } GROUP BY ?entity";
		final String getConceptsAndPropCountsQuery = "SELECT DISTINCT ?nodeType (COUNT(DISTINCT ?entity) AS ?entityCount) WHERE { {?nodeType <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?nodeType} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop } } GROUP BY ?nodeType";
		final String getConceptEdgesCountQuery = "SELECT DISTINCT ?entity (SUM(?count) AS ?totalCount) WHERE { { SELECT DISTINCT ?entity (COUNT(?outRel) AS ?count) WHERE { FILTER(?outRel != <http://semoss.org/ontologies/Relation>) {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?node2 ?outRel ?entity} } GROUP BY ?entity } UNION { SELECT DISTINCT ?entity (COUNT(?inRel) AS ?count) WHERE { FILTER(?inRel != <http://semoss.org/ontologies/Relation>) {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?entity ?inRel ?node1} } GROUP BY ?entity } } GROUP BY ?entity";
		final String getConceptInsightCountQuery = "SELECT DISTINCT ?entity (COUNT(DISTINCT ?insight) AS ?count) WHERE { BIND(<@ENGINE_NAME@> AS ?engine) {?insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?engine ?engineInsight ?insight} {?insight <INSIGHT:PARAM> ?param} {?param <PARAM:TYPE> ?entity} } GROUP BY ?entity @ENTITY_BINDINGS@";
		final String eccentricityQuery = "SELECT DISTINCT ?s ?p ?o WHERE { ?s ?p ?o} LIMIT 1";
		
		Vector<String> conceptList = engine.getEntityOfType(getConceptListQuery);
		Hashtable<String, Hashtable<String, Object>> allData = constructDataHash(conceptList);

		allData = addToAllData(engine, getConceptsAndInstanceCountsQuery, "x", allData);
		allData = addToAllData(engine, getConceptsAndPropCountsQuery, "z", allData);
		
		RDFFileSesameEngine baseDataEngine = ((AbstractEngine)engine).getBaseDataEngine();
		allData = addToAllData(baseDataEngine, getConceptEdgesCountQuery, "y", allData);
		
		GraphPlaySheet graphPS = CentralityCalculator.createMetamodel(baseDataEngine.getRC(), eccentricityQuery);
		Hashtable<String, SEMOSSVertex> vertStore  = graphPS.getGraphData().getVertStore();
		Hashtable<SEMOSSVertex, Double> unDirEccentricity = CentralityCalculator.calculateEccentricity(vertStore, false);
		allData = addToAllData(unDirEccentricity, "time", allData);

		String engineName = engine.getEngineName();
		String specificInsightQuery = getConceptInsightCountQuery.replace("@ENGINE_NAME@", "http://semoss.org/ontologies/Concept/Engine/".concat(engineName));
		String bindings = "BINDINGS ?entity { ";
		for(String concept : conceptList) {
			if(!concept.equals("http://semoss.org/ontologies/Concept"))
				bindings = bindings.concat("(<").concat(concept).concat(">)");
		}
		bindings = bindings.concat(" }");
		specificInsightQuery = specificInsightQuery.replace("@ENTITY_BINDINGS@", bindings);
		RDFFileSesameEngine insightEngine = ((AbstractEngine)engine).getInsightBaseXML();
		allData = addToAllData(insightEngine, specificInsightQuery, "heat", allData);

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", allData.values());
		allHash.put("title", "Exploring Data Types in ".concat(engineName));
		allHash.put("xAxisTitle", "Number of Instances");
		allHash.put("yAxisTitle", "Number of Edges");
		allHash.put("zAxisTitle", "Number of Properties");
		allHash.put("heatTitle", "Number of Insights");
		
		return allHash;
	}
	
	private Hashtable<String, Hashtable<String, Object>> constructDataHash(Vector<String> conceptList) {
		Hashtable<String, Hashtable<String, Object>> allData = new Hashtable<String, Hashtable<String, Object>>();
		int length = conceptList.size();
		int i = 0;
		for(;i < length; i++) {
			String key = conceptList.get(i);
			if(!key.equals("http://semoss.org/ontologies/Concept")) {
				Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
				elementHash.put("series", "Concepts");
				elementHash.put("label", conceptList.get(i));
				allData.put(conceptList.get(i), elementHash);
			}
		}
		
		return allData;
	}
	
	private Hashtable<String, Hashtable<String, Object>> addToAllData(IEngine engine, String query, String key, Hashtable<String, Hashtable<String, Object>> allData) {
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String concept = sjss.getRawVar(param1).toString();
			if(!concept.equals("http://semoss.org/ontologies/Concept")) {
				Object val = sjss.getVar(param2);
				
				Hashtable<String, Object> elementData = allData.get(concept);
				elementData.put(key, val);
			}
		}
		
		return allData;
	}
	
	private Hashtable<String, Hashtable<String, Object>> addToAllData(Hashtable<SEMOSSVertex, Double> unDirEccentricity, String key, Hashtable<String, Hashtable<String, Object>> allData) {
		for(SEMOSSVertex vert : unDirEccentricity.keySet()) {
			String concept = vert.getURI().replaceAll(" ", "");
			if(!concept.equals("http://semoss.org/ontologies/Concept")) {
				Object val = unDirEccentricity.get(vert);
				
				Hashtable<String, Object> elementData = allData.get(concept);
				elementData.put(key, val);
			}
		}

		return allData;
	}
	
	public List<Hashtable<String, String>> getQuestionsWithoutParams(IEngine engine) {
		final String getInsightsWithoutParamsQuery = "SELECT DISTINCT ?questionDescription WHERE { BIND(<@ENGINE_NAME@> AS ?engine) {?insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?engine ?engineInsight ?insight} {?insight <http://semoss.org/ontologies/Relation/Contains/Label> ?questionDescription} MINUS{ {?insight <INSIGHT:PARAM> ?param} {?param <PARAM:TYPE> ?entity} } }";
		
		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();
		
		RDFFileSesameEngine insightEngine = ((AbstractEngine)engine).getInsightBaseXML();
		String query = getInsightsWithoutParamsQuery.replace("@ENGINE_NAME@", "http://semoss.org/ontologies/Concept/Engine/".concat(engine.getEngineName()));
				
		SesameJenaSelectWrapper sjsw = Utility.processQuery(insightEngine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Hashtable<String, String> questionHash = new Hashtable<String, String>();
			questionHash.put("Questions", sjss.getVar(param1).toString());
			retList.add(questionHash);
		}
		
		return retList;
	}
	
	public List<Hashtable<String, String>> getQuestionsForParam(IEngine engine, String typeURI) {
		final String getInsightsWithParamsQuery = "SELECT DISTINCT ?questionDescription WHERE { BIND(<@ENTITY_TYPE@> AS ?entity) BIND(<@ENGINE_NAME@> AS ?engine) {?insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?engine ?engineInsight ?insight} {?insight <http://semoss.org/ontologies/Relation/Contains/Label> ?questionDescription} {?insight <INSIGHT:PARAM> ?param} {?param <PARAM:TYPE> ?entity} }";
		
		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();
		
		RDFFileSesameEngine insightEngine = ((AbstractEngine)engine).getInsightBaseXML();
		String query = getInsightsWithParamsQuery.replace("@ENGINE_NAME@", "http://semoss.org/ontologies/Concept/Engine/".concat(engine.getEngineName()));		
		query = query.replace("@ENTITY_TYPE@", typeURI);
				
		SesameJenaSelectWrapper sjsw = Utility.processQuery(insightEngine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Hashtable<String, String> questionHash = new Hashtable<String, String>();
			questionHash.put("Questions", sjss.getVar(param1).toString());
			retList.add(questionHash);
		}
		
		return retList;
	}
	
	public List<Hashtable<String, String>> getMostInfluentialInstancesForAllTypes(IEngine engine) {
		final String getMostConncectedInstancesQuery = "SELECT DISTINCT ?entity ?instance (COUNT(?inRel) + COUNT(?outRel) AS ?edgeCount) WHERE { { FILTER (STR(?entity)!='http://semoss.org/ontologies/Concept') {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?entity} {?instance <http://www.w3.org/2000/01/rdf-schema#label> ?instanceLabel2} {?node2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?relLabel2} {?node2 ?inRel ?instance} } UNION { FILTER (STR(?entity)!='http://semoss.org/ontologies/Concept') {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?entity} {?instance <http://www.w3.org/2000/01/rdf-schema#label> ?instanceLabel1} {?node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?relLabel1} {?instance ?outRel ?node1} } } GROUP BY ?entity ?instance ORDER BY DESC(?edgeCount)";
		return mostConnectedInstancesProcessing(engine, getMostConncectedInstancesQuery);
	}
	
	public List<Hashtable<String, String>> getMostInfluentialInstancesForSpecificTypes(IEngine engine, String typeURI) {
		final String getMostConnectedInstancesWithType = "SELECT DISTINCT ?nodeType ?entity (COUNT(?inRel) + COUNT(?outRel) AS ?edgeCount) WHERE { BIND(<@NODE_URI@> AS ?nodeType) { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?nodeType} {?node2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?label2} {?node2 ?inRel ?entity} } UNION { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?nodeType} {?node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?label1} {?entity ?outRel ?node1} } } GROUP BY ?entity ?type ORDER BY DESC(?edgeCount)";
		String query = getMostConnectedInstancesWithType.replaceAll("@NODE_URI@", typeURI);
		return mostConnectedInstancesProcessing(engine, query);
	}

	private List<Hashtable<String, String>> mostConnectedInstancesProcessing(IEngine engine, String query) {
		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		String param3 = names[2];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Hashtable<String, String> instancesHash = new Hashtable<String, String>();
			instancesHash.put("Node_Type", sjss.getRawVar(param1).toString());
			instancesHash.put("Instance", sjss.getRawVar(param2).toString());
			instancesHash.put("Num_of_Edges", sjss.getVar(param3).toString());
			retList.add(instancesHash);
		}
		
		return retList;
	}
	
	public List<Hashtable<String, Object>> getLargestOutliers(IEngine engine, String typeURI) {
		final String baseQuery = "SELECT DISTINCT ?@TYPE@ @PROPERTIES@ WHERE { {?@TYPE@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@TYPE_URI@>} @PROP_TRIPLES@ }";
		final String propListQuery = "SELECT DISTINCT ?prop WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@TYPE_URI@>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?entity ?prop ?val} }";
		
		String type = Utility.getInstanceName(typeURI);
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, propListQuery.replace("@TYPE_URI@", typeURI));
		String[] names = sjsw.getVariables();
		String retVar = names[0];
		String propVars = "";
		String propTriples = "";
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String varName = sjss.getVar(retVar).toString().replaceAll("-", "_");
			String varURI = sjss.getRawVar(retVar).toString();
			
			propVars = propVars.concat("?").concat(varName).concat(" ");
			propTriples = propTriples.concat("OPTIONAL{?").concat(type).concat(" <").concat(varURI).concat("> ").concat("?").concat(varName).concat("} ");
		}
		
		String query = baseQuery.replaceAll("@TYPE@", type).replace("@TYPE_URI@", typeURI).replace("@PROPERTIES@", propVars).replace("@PROP_TRIPLES@", propTriples);
		
		ArrayList<Object[]> results = new ArrayList<Object[]>();
		sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		int length = names.length;
		while(sjsw.hasNext()) {
			Object[] row = new Object[length];
			SesameJenaSelectStatement sjss = sjsw.next();
			int i = 0;
			for(i = 0; i < length; i++) {
				row[i] = sjss.getVar(names[i]);
			}
			results.add(row);
		}
		
		LocalOutlierFactorAlgorithm alg = new LocalOutlierFactorAlgorithm(results, names);
		alg.setK(25);
		alg.execute();
		
		results = alg.getMasterTable();
		double[] lof = alg.getLOF();
		double[] lop = alg.getLOP();
		
		int i = 0;
		int j = 0;
		length = lop.length;
		int numResults = 10;
		Integer[] maxIndicies = new Integer[numResults];
		for(; i < length; i++) {
			for(; j < numResults; j++) {
				// for the first 10 entries
				if(maxIndicies[j] == null) {
					maxIndicies[j] = i;
					break;
				}
				else if(lof[maxIndicies[j]] < lof[i]) {
					int k = numResults - 1;
					// insert index in correct spot
					for(; k < j; k--) {
						maxIndicies[k] = maxIndicies[k - 1];
					}
					maxIndicies[j] = i;
					break;
				}
			}
		}
		
		List<Hashtable<String, Object>> retList = new ArrayList<Hashtable<String, Object>>();

		i = 0;
		for(; i < numResults; i++) {
			int index = maxIndicies[i];
			Hashtable<String, Object> instancesHash = new Hashtable<String, Object>();
			instancesHash.put("Instance", results.get(index)[0]);
			instancesHash.put("Outlier_Prob", lop[index]);
			retList.add(instancesHash);
		}
		
		return retList;
	}
	
	public Hashtable<String, List<Hashtable<String, Object>>> getConnectionMap(IEngine engine, String instanceURI) {
		final String baseOutRelQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT ?outRel) AS ?count) WHERE { BIND(<@INSTANCE_URI@> AS ?instance) {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?type <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?instance ?outRel ?node} } GROUP BY ?instance";
		final String baseInRelQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT ?inRel) AS ?count) WHERE { BIND(<@INSTANCE_URI@> AS ?instance) {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?type <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?node ?inRel ?instance} } GROUP BY ?type";
		
		String outRelQuery = baseOutRelQuery.replace("@INSTANCE_URI@", instanceURI);
		String inRelQuery = baseInRelQuery.replace("@INSTANCE_URI@", instanceURI);
		
		Hashtable<String, List<Hashtable<String, Object>>> retHash = new Hashtable<String, List<Hashtable<String, Object>>>();
		retHash.put("outBound", new ArrayList<Hashtable<String, Object>>());
		retHash.put("inBound", new ArrayList<Hashtable<String, Object>>());

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, outRelQuery);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String type = sjss.getVar(param1).toString();
			if(!type.equals("Concept")){
				Object value = sjss.getVar(param2);
				Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
				innerHash.put(type, value);
				retHash.get("outBound").add(innerHash);
			}
		}

		sjsw = Utility.processQuery(engine, inRelQuery);
		names = sjsw.getVariables();
		param1 = names[0];
		param2 = names[1];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String type = sjss.getVar(param1).toString();
			if(!type.equals("Concept")){
				Object value = sjss.getVar(param2);
				Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
				innerHash.put(type, value);
				retHash.get("inBound").add(innerHash);
			}
		}
		
		return retHash;
	}
	
	
	public List<Hashtable<String, String>> getPropertiesForInstance(IEngine engine, String instanceURI) {
		final String getPropertiesForInstance = "SELECT DISTINCT ?entity ?prop WHERE { BIND(<@INSTANCE_URI@> AS ?source) {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop } } ORDER BY ?entity";

		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();

		String query = getPropertiesForInstance.replace("@INSTANCE_URI@", instanceURI);
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Hashtable<String, String> propHash = new Hashtable<String, String>();
			propHash.put("Property", sjss.getVar(param1).toString());
			propHash.put("Value", sjss.getVar(param2).toString());
			retList.add(propHash);
		}
		
		return retList;
	}
	
	
}
