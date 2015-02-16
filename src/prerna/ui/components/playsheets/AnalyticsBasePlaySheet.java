/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.playsheets;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.cluster.LocalOutlierFactorAlgorithm;
import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.impl.SubclassingMapGenerator;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class AnalyticsBasePlaySheet extends BrowserPlaySheet {
	
	private IEngine engine;
	private static final Logger LOGGER = LogManager.getLogger(AnalyticsBasePlaySheet.class.getName());

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
		final String getConceptInsightCountQuery = "SELECT DISTINCT ?entity (COUNT(DISTINCT ?insight) AS ?count) WHERE { BIND(<@ENGINE_NAME@> AS ?engine) {?insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?engine ?engineInsight ?insight} {?insight <INSIGHT:PARAM> ?param} {?param <PARAM:TYPE> ?entity} } GROUP BY ?entity @ENTITY_BINDINGS@";
		final String eccentricityQuery = "SELECT DISTINCT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";

		LOGGER.info("Getting the list of concepts...");
		Vector<String> conceptList = engine.getEntityOfType(getConceptListQuery);
		Hashtable<String, Hashtable<String, Object>> allData = constructDataHash(conceptList);

		LOGGER.info("Getting the number of instances for each concept...");
		allData = addToAllData(engine, getConceptsAndInstanceCountsQuery, "z", allData);
		
		LOGGER.info("Getting the number of properties for each concept...");
		allData = addToAllData(engine, getConceptsAndPropCountsQuery, "w", allData);
		
		RDFFileSesameEngine baseDataEngine = ((AbstractEngine)engine).getBaseDataEngine();
		LOGGER.info("Creating subclass map...");
		SubclassingMapGenerator subclassGen = new SubclassingMapGenerator();
		subclassGen.processSubclassing(baseDataEngine);
		HashMap<String, Integer> edgeCountsHash = subclassGen.calculateEdgeCounts(baseDataEngine);
		LOGGER.info("Adding number of edges for each concept...");
		allData = addToAllData(edgeCountsHash, "y", allData);

		LOGGER.info("Generating metamodel graph for centrality measures...");
		GraphPlaySheet graphPS = CentralityCalculator.createMetamodel(baseDataEngine.getRC(), eccentricityQuery);
		LOGGER.info("Extending metamodel graph for subclassed concepts...");
		Hashtable<String, SEMOSSVertex> vertStore  = graphPS.getGraphData().getVertStore();
		subclassGen.updateVertAndEdgeStoreForSubclassing(vertStore, graphPS.getGraphData().getEdgeStore());
		vertStore = subclassGen.getVertStore();
		
		LOGGER.info("Calculating centrality values for each concepts...");
		Hashtable<SEMOSSVertex, Double> centralityHash = new Hashtable<SEMOSSVertex, Double>();
		centralityHash = addToCentralityHash(CentralityCalculator.calculateEccentricity(vertStore, false), centralityHash);
		centralityHash = addToCentralityHash(CentralityCalculator.calculateBetweenness(vertStore, false), centralityHash);
		centralityHash = addToCentralityHash(CentralityCalculator.calculateCloseness(vertStore, false), centralityHash);
		centralityHash = averageCentralityValues(centralityHash, 3);
		LOGGER.info("Adding average centrality value for each concepts...");
		allData = addToAllData(centralityHash, "x", allData);

		LOGGER.info("Constructing query to calculate the number of insights for each query...");
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
		LOGGER.info("Adding number of insights for each concept...");
		allData = addToAllData(insightEngine, specificInsightQuery, "heat", allData);

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", allData.values());
		allHash.put("title", "Exploring_Data_Types_in_".concat(engineName));
		allHash.put("zAxisTitle", "Number_of_Instances");
		allHash.put("yAxisTitle", "Number_of_Edges");
		allHash.put("xAxisTitle", "Centrality_Value");
		allHash.put("heatTitle", "Number_of_Insights");
		allHash.put("wAxisTitle", "Number_of_Properties");
		
		LOGGER.info("Passing to monolith instance...");
		return allHash;
	}	
	
	private Hashtable<SEMOSSVertex, Double> averageCentralityValues(Hashtable<SEMOSSVertex, Double> appendingHash, double n) {
		for(SEMOSSVertex key : appendingHash.keySet()) {
			Double val = appendingHash.get(key);
			val /= n;
			appendingHash.put(key, val);
		}
		return appendingHash;
	}
	
	private Hashtable<SEMOSSVertex, Double> addToCentralityHash(Hashtable<SEMOSSVertex, Double> dataToAdd, Hashtable<SEMOSSVertex, Double> appendingHash) {
		for(SEMOSSVertex key : dataToAdd.keySet()) {
			if(appendingHash.containsKey(key)) {
				Double val = appendingHash.get(key);
				val += dataToAdd.get(key);
				appendingHash.put(key, val);
			} else {
				Double val = dataToAdd.get(key);
				appendingHash.put(key, val);
			}
		}
		
		return appendingHash;
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
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String concept = sjss.getRawVar(param1).toString();
			if(!concept.equals("http://semoss.org/ontologies/Concept")) {
				if(allData.containsKey(concept)) {
					Hashtable<String, Object> elementData = allData.get(concept);
					elementData.put(key, sjss.getVar(param2));
				} else {
					//TODO: add to error message
				}
			}
		}
		
		return allData;
	}
	
	private Hashtable<String, Hashtable<String, Object>> addToAllData(Hashtable<SEMOSSVertex, Double> unDirEccentricity, String key, Hashtable<String, Hashtable<String, Object>> allData) {
		for(SEMOSSVertex vert : unDirEccentricity.keySet()) {
			String concept = vert.getURI().replaceAll(" ", "");
			if(!concept.equals("http://semoss.org/ontologies/Concept")) {
				if(allData.containsKey(concept)) {
					Hashtable<String, Object> elementData = allData.get(concept);
					elementData.put(key, unDirEccentricity.get(vert));
				} else {
					//TODO: add to error message
				}
			}
		}

		return allData;
	}
	
	private Hashtable<String, Hashtable<String, Object>> addToAllData(HashMap<String, Integer> edgeCounts, String key, Hashtable<String, Hashtable<String, Object>> allData) {
		for(String concept : edgeCounts.keySet()) {
			if(allData.containsKey(concept)) {
				Hashtable<String, Object> elementData = allData.get(concept);
				elementData.put(key, edgeCounts.get(concept));
			} else {
				//TODO: add to error message
			}
		}

		return allData;
	}
	
	public List<Hashtable<String, String>> getQuestionsWithoutParams(IEngine engine) {
		final String getInsightsWithoutParamsQuery = "SELECT DISTINCT ?perspective ?question ?viz WHERE { BIND(<@ENGINE_NAME@> AS ?engine) {?perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?engine <http://semoss.org/ontologies/Relation/Engine:Perspective> ?perspective} {?insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?perspective <http://semoss.org/ontologies/Relation/Perspective:Insight> ?insight} {?insight <http://semoss.org/ontologies/Relation/Contains/Label> ?question} {?insight <http://semoss.org/ontologies/Relation/Contains/Layout> ?viz} MINUS{ {?insight <INSIGHT:PARAM> ?param} {?param <PARAM:TYPE> ?entity} } }";
		
		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();
		
		String engineName = engine.getEngineName();
		RDFFileSesameEngine insightEngine = ((AbstractEngine)engine).getInsightBaseXML();
		String query = getInsightsWithoutParamsQuery.replace("@ENGINE_NAME@", "http://semoss.org/ontologies/Concept/Engine/".concat(engineName));
				
		ISelectWrapper sjsw = Utility.processQuery(insightEngine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		String param3 = names[2];
		
		String removeToGetPerspective = engineName.concat(":");
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Hashtable<String, String> questionHash = new Hashtable<String, String>();
			questionHash.put("database", engineName);
			questionHash.put("keyword", ""); // no keyword since this is returning questions without parameters
			questionHash.put("perspective", sjss.getVar(param1).toString().replace(removeToGetPerspective, ""));
			questionHash.put("question", sjss.getVar(param2).toString());
			questionHash.put("viz", sjss.getVar(param3).toString());
			retList.add(questionHash);
		}
		
		return retList;
	}
	
	public List<Hashtable<String, String>> getQuestionsForParam(IEngine engine, String typeURI) {
		final String getInsightsWithParamsQuery = "SELECT DISTINCT ?perspective ?question ?viz WHERE { BIND(<@ENTITY_TYPE@> AS ?entity) BIND(<@ENGINE_NAME@> AS ?engine) {?perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?engine <http://semoss.org/ontologies/Relation/Engine:Perspective> ?perspective} {?insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?perspective <http://semoss.org/ontologies/Relation/Perspective:Insight> ?insight} {?insight <http://semoss.org/ontologies/Relation/Contains/Label> ?question} {?insight <http://semoss.org/ontologies/Relation/Contains/Layout> ?viz} {?insight <INSIGHT:PARAM> ?param} {?param <PARAM:TYPE> ?entity} }";
		
		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();
		
		String engineName = engine.getEngineName();
		RDFFileSesameEngine insightEngine = ((AbstractEngine)engine).getInsightBaseXML();
		String query = getInsightsWithParamsQuery.replace("@ENGINE_NAME@", "http://semoss.org/ontologies/Concept/Engine/".concat(engineName));
		query = query.replace("@ENTITY_TYPE@", typeURI);
				
		ISelectWrapper sjsw = Utility.processQuery(insightEngine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		String param3 = names[2];

		String removeToGetPerspective = engineName.concat(":");
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Hashtable<String, String> questionHash = new Hashtable<String, String>();
			questionHash.put("database", engineName);
			questionHash.put("keyword", typeURI);
			questionHash.put("perspective", sjss.getVar(param1).toString().replace(removeToGetPerspective, ""));
			questionHash.put("question", sjss.getVar(param2).toString());
			questionHash.put("viz", sjss.getVar(param3).toString());
			retList.add(questionHash);
		}
		
		return retList;
	}
	
	public List<Hashtable<String, String>> getMostInfluentialInstancesForAllTypes(IEngine engine) {
		final String getMostConncectedInstancesQuery = "SELECT DISTINCT ?entity ?instance (COUNT(?inRel) + COUNT(?outRel) AS ?edgeCount) WHERE { { FILTER (STR(?entity)!='http://semoss.org/ontologies/Concept') {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?entity} {?instance <http://www.w3.org/2000/01/rdf-schema#label> ?instanceLabel2} {?node2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?relLabel2} {?node2 ?inRel ?instance} } UNION { FILTER (STR(?entity)!='http://semoss.org/ontologies/Concept') {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?entity} {?instance <http://www.w3.org/2000/01/rdf-schema#label> ?instanceLabel1} {?node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?relLabel1} {?instance ?outRel ?node1} } } GROUP BY ?entity ?instance ORDER BY DESC(?edgeCount) LIMIT 10";
		return mostConnectedInstancesProcessing(engine, getMostConncectedInstancesQuery);
	}
	
	public List<Hashtable<String, String>> getMostInfluentialInstancesForSpecificTypes(IEngine engine, String typeURI) {
		final String getMostConnectedInstancesWithType = "SELECT DISTINCT ?type ?entity (COUNT(?inRel) + COUNT(?outRel) AS ?edgeCount) WHERE { BIND(<@NODE_URI@> AS ?type) { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?node2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?label2} {?node2 ?inRel ?entity} } UNION { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?label1} {?entity ?outRel ?node1} } } GROUP BY ?entity ?type ORDER BY DESC(?edgeCount) LIMIT 10";
		
		String query = getMostConnectedInstancesWithType.replaceAll("@NODE_URI@", typeURI);
		return mostConnectedInstancesProcessing(engine, query);
	}

	private List<Hashtable<String, String>> mostConnectedInstancesProcessing(IEngine engine, String query) {
		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();
		
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		String param3 = names[2];
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Hashtable<String, String> instancesHash = new Hashtable<String, String>();
			instancesHash.put("NodeTypeURI", sjss.getRawVar(param1).toString());
			instancesHash.put("NodeTypeName", sjss.getVar(param1).toString());
			instancesHash.put("InstanceURI", sjss.getRawVar(param2).toString());
			instancesHash.put("InstanceName", sjss.getVar(param2).toString());
			instancesHash.put("Num_of_Edges", sjss.getVar(param3).toString());
			retList.add(instancesHash);
		}
		
		return retList;
	}
	
	public List<Hashtable<String, Object>> getLargestOutliers(IEngine engine, String typeURI) {
		LOGGER.info("Constructing query to look get all properties for instances of type " + typeURI + "...");
		final String basePropQuery = "SELECT DISTINCT ?@TYPE@ @PROPERTIES@ WHERE { {?@TYPE@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@TYPE_URI@>} @PROP_TRIPLES@ }";
		final String propListQuery = "SELECT DISTINCT ?prop WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@TYPE_URI@>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?entity ?prop ?val} }";
		final String baseRelQuery = "SELECT DISTINCT ?type (COUNT(?inRel) AS ?inRelCount) (COUNT(?outRel) AS ?outRelCount) WHERE { BIND(<@TYPE_URI@> AS ?entity) { {?type <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?entity} {?node2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?label2} {?node2 ?inRel ?type} } UNION { {?type <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?label1} {?type ?outRel ?node1} } } GROUP BY ?type";
		String type = Utility.getInstanceName(typeURI);
		
		boolean hasProperties = false;
		ISelectWrapper sjsw = Utility.processQuery(engine, propListQuery.replace("@TYPE_URI@", typeURI));
		String[] names = sjsw.getVariables();
		String retVar = names[0];
		String propVars = "";
		String propTriples = "";
		while(sjsw.hasNext()) {
			hasProperties = true;
			ISelectStatement sjss = sjsw.next();
			String varName = sjss.getVar(retVar).toString().replaceAll("-", "_");
			String varURI = sjss.getRawVar(retVar).toString();
			
			propVars = propVars.concat("?").concat(varName).concat(" ");
			propTriples = propTriples.concat("OPTIONAL{?").concat(type).concat(" <").concat(varURI).concat("> ").concat("?").concat(varName).concat("} ");
		}
		
		//if no properties found -> use number of edges
		String query = null;
		if(!hasProperties) {
			LOGGER.info("...No properties found. Using counts for in/out relationships to determine outliers...");
			query = baseRelQuery.replace("@TYPE_URI@", typeURI);
		} else {
			query = basePropQuery.replaceAll("@TYPE@", type).replace("@TYPE_URI@", typeURI).replace("@PROPERTIES@", propVars).replace("@PROP_TRIPLES@", propTriples);
		}
		
		LOGGER.info("Constructing dataset based on query...");
		ArrayList<Object[]> results = new ArrayList<Object[]>();
		sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		int length = names.length;
		while(sjsw.hasNext()) {
			Object[] row = new Object[length];
			ISelectStatement sjss = sjsw.next();
			int i = 0;
			for(i = 0; i < length; i++) {
				if(i == 0) {
				// want to pass URI's for basepage
					row[0] = sjss.getRawVar(names[0]);
				} else {
					row[i] = sjss.getVar(names[i]);
				}
			}
			results.add(row);
		}
		
		// when no results, return null
		if(results.isEmpty()) {
			LOGGER.info("...No data found...");
			return null;
		}
		// when not enough instances to determine outliers, return empty
		if(results.size() < 20) {
			LOGGER.info("...Insufficient data size to run algorithm....");
			return new ArrayList<Hashtable<String, Object>>();
		}
		
		LocalOutlierFactorAlgorithm alg = new LocalOutlierFactorAlgorithm(results, names);
		alg.setK(25);
		alg.execute();
		
		results = alg.getMasterTable();
		double[] lop = alg.getLOP();
		
		LOGGER.info("Ordering the probabilities to get the top results...");
		int i;
		length = lop.length;
		// loop through and order all values and indices
		int[] indexOrder = new int[length];
		for(i = 0; i < length; i++) {
			indexOrder[i] = i;
		}	
		double tempProb;
		int tempIndex;
		boolean flag = true;
		while(flag) {
			flag = false;
			for(i = 0; i < length - 1; i++) {
				if(lop[i] < lop[i+1]){
					tempProb = lop[i];
					lop[i] = lop[i+1];
					lop[i+1] = tempProb;
					// change order of index value
					tempIndex = indexOrder[i];
					indexOrder[i] = indexOrder[i+1];
					indexOrder[i+1] = tempIndex;
					flag = true;
				}
			}
		}
		
				
		int numResults = 10;
		List<Hashtable<String, Object>> retList = new ArrayList<Hashtable<String, Object>>();
		DecimalFormat df = new DecimalFormat("#%");
		i = 0;
		for(; i < numResults; i++) {
			int index = indexOrder[i];
			Hashtable<String, Object> instancesHash = new Hashtable<String, Object>();
			String instanceURI = results.get(index)[0].toString();
			instancesHash.put("NodeTypeURI", typeURI);
			instancesHash.put("NodeTypeName", type);
			instancesHash.put("InstanceURI", instanceURI);
			instancesHash.put("InstanceName", Utility.getInstanceName(instanceURI));
			instancesHash.put("Outlier_Prob", df.format(lop[i]));
			retList.add(instancesHash);
		}
		
		LOGGER.info("Passing to monolith instance...");
		return retList;
	}
	
	public List<Hashtable<String, Object>> getConnectionMap(IEngine engine, String instanceURI) {
		final String baseQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT ?rel) AS ?count) ?direction ?description WHERE { { SELECT DISTINCT ?type ?rel ?direction ?description WHERE { FILTER(?type != <http://semoss.org/ontologies/Concept>) BIND(<@INSTANCE_URI@> AS ?instance) {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?rel <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?type <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?node ?rel ?instance} BIND('in' AS ?direction) BIND(CONCAT( REPLACE(STR(?type), '.*/', ''), ' To ', REPLACE(STR(?instance), '.*/', '')) AS ?description) } } UNION { SELECT DISTINCT ?type ?rel ?direction ?description WHERE { FILTER(?type != <http://semoss.org/ontologies/Concept>) BIND(<@INSTANCE_URI@> AS ?instance) {?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?rel <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?type <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type} {?instance ?rel ?node} BIND('out' AS ?direction) BIND(CONCAT( REPLACE(STR(?instance), '.*/', ''), ' To ', REPLACE(STR(?type), '.*/', '')) AS ?description)} } } GROUP BY ?type ?direction ?description ORDER BY DESC(?count)";
		
		String query = baseQuery.replaceAll("@INSTANCE_URI@", instanceURI);
		
		List<Hashtable<String, Object>> retList = new ArrayList<Hashtable<String, Object>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		String param3 = names[2];
		String param4 = names[3];
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("typeURI", sjss.getRawVar(param1));
			innerHash.put("typeName", sjss.getVar(param1));
			innerHash.put("value", sjss.getVar(param2));
			innerHash.put("direction", sjss.getVar(param3));
			innerHash.put("description", sjss.getVar(param4));
			retList.add(innerHash);
		}

		return retList;
	}
	
	
	public List<Hashtable<String, String>> getPropertiesForInstance(IEngine engine, String instanceURI) {
		final String getPropertiesForInstance = "SELECT DISTINCT ?entity ?prop WHERE { BIND(<@INSTANCE_URI@> AS ?source) {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop } } ORDER BY ?entity";

		List<Hashtable<String, String>> retList = new ArrayList<Hashtable<String, String>>();

		String query = getPropertiesForInstance.replace("@INSTANCE_URI@", instanceURI);
		
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Hashtable<String, String> propHash = new Hashtable<String, String>();
			propHash.put("Property", sjss.getVar(param1).toString());
			propHash.put("Value", sjss.getVar(param2).toString());
			retList.add(propHash);
		}
		
		return retList;
	}
	
	
}
