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
package prerna.nameserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.nlp.TextHelper;
import prerna.auth.User;
import prerna.auth.UserPermissionsMasterDB;
import prerna.auth.User.LOGIN_TYPES;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AddToMasterDB extends ModifyMasterDB {

	private String wordnetPath;
	private HypernymListGenerator hypernymGenerator;
	
	public AddToMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}
	public AddToMasterDB() {
		super();
	}

	public boolean registerEngineLocal(String engineName) {
		boolean success = false;
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}
		
		hypernymGenerator = new HypernymListGenerator(wordnetPath);
		hypernymGenerator.addMappings(parentChildMapping);

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName + "");
		String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		GraphPlaySheet gps = CentralityCalculator.createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRc(), sparql);
		Hashtable<String, SEMOSSVertex> vertStore  = gps.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = gps.getGraphData().getEdgeStore();

		addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
		RepositoryConnection rc = engine.getInsightDB();
		addInsights(rc);

		logger.info("Finished adding new engine " + engineName);
		success = true;

		masterEngine.commit();
		masterEngine.infer();

		return success;
	}
	
	public boolean registerEngineLocal(IEngine engine) {
		boolean success = false;
		String engineName = engine.getEngineName();
		
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}
		
		hypernymGenerator = new HypernymListGenerator(wordnetPath);
		hypernymGenerator.addMappings(parentChildMapping);

		String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		GraphPlaySheet gps = CentralityCalculator.createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRc(), sparql);
		Hashtable<String, SEMOSSVertex> vertStore  = gps.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = gps.getGraphData().getEdgeStore();

		addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
		RepositoryConnection rc = engine.getInsightDB();
		addInsights(rc);

		logger.info("Finished adding new engine " + engineName);
		success = true;

		masterEngine.commit();
		masterEngine.infer();

		return success;
	}
	
	public Hashtable<String, Boolean> registerEngineLocal(ArrayList<String> dbArray) {
		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}
		
		hypernymGenerator = new HypernymListGenerator(wordnetPath);
		hypernymGenerator.addMappings(parentChildMapping);

		for(String engineName : dbArray) {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName + "");
			String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
			GraphPlaySheet gps = CentralityCalculator.createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRc(), sparql);
			Hashtable<String, SEMOSSVertex> vertStore  = gps.getGraphData().getVertStore();
			Hashtable<String, SEMOSSEdge> edgeStore = gps.getGraphData().getEdgeStore();

			addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
			RepositoryConnection rc = engine.getInsightDB();
			addInsights(rc);

			logger.info("Finished adding new engine " + engineName);
			successHash.put(engineName, true);
		}

//		for(String parent : parentChildMapping.keySet()) {
//			logger.info("Parent: " + parent + ". Child: " + parentChildMapping.get(parent));
//		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}

	public Hashtable<String, Boolean> registerEngineAPI(String baseURL, ArrayList<String> dbArray) throws RDFParseException, RepositoryException, IOException {

		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();

//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}

		hypernymGenerator = new HypernymListGenerator(wordnetPath);
		hypernymGenerator.addMappings(parentChildMapping);
		
		for(String engineName : dbArray) {
			String engineAPI = baseURL + "/s-"+engineName;
			String owl = Utility.retrieveResult(engineAPI + "/getOWLDefinition", null);
			RepositoryConnection owlRC = getNewRepository();
			owlRC.add(new ByteArrayInputStream(owl.getBytes("UTF-8")), "http://semoss.org", RDFFormat.RDFXML);

			String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
			GraphPlaySheet gps = CentralityCalculator.createMetamodel(owlRC, sparql);
			Hashtable<String, SEMOSSVertex> vertStore  = gps.getGraphData().getVertStore();
			Hashtable<String, SEMOSSEdge> edgeStore = gps.getGraphData().getEdgeStore();

			addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.PROP_URI + "/" + "API",baseURL,true);

			String insights = Utility.retrieveResult(engineAPI + "/getInsightDefinition", null);
			RepositoryConnection insightsRC = getNewRepository();
			insightsRC.add(new ByteArrayInputStream(insights.getBytes("UTF-8")), "http://semoss.org", RDFFormat.RDFXML);

			addInsights(insightsRC);

			logger.info("Finished adding new engine " + engineName);
			successHash.put(engineName, true);
		}

//		for(String parent : parentChildMapping.keySet()) {
//			logger.info("Parent: " + parent + ". Child: " + parentChildMapping.get(parent));
//		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}	

	private void addNewDBConcepts(String engineName, Hashtable<String, SEMOSSVertex> vertStore, Hashtable<String, SEMOSSEdge> edgeStore, Map<String, String> parentChildMapping) {
		MasterDatabaseForest<String> forest = new MasterDatabaseForest<String>();
		MasterDatabaseBipartiteGraph<String> keywordConceptBipartiteGraph = new MasterDatabaseBipartiteGraph<String>();

		Iterator<SEMOSSVertex> vertItr = vertStore.values().iterator();
		while(vertItr.hasNext()) {
			SEMOSSVertex vert = vertItr.next();
			String vertName = vert.getProperty(Constants.VERTEX_NAME).toString();
			if(!vertName.equals("Concept")) {
				String[] nouns = TextHelper.breakCompoundText(vertName);
				int numNouns = nouns.length;

				// update the concept-concept tree and the keyword-concept graph
				String cleanVertName = Utility.cleanString(vertName, false);
				String typeURI =vert.getURI();//full URI of this keyword
				MasterDBHelper.addNode(masterEngine, typeURI);
				MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + cleanVertName, typeURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + cleanVertName + ":" + cleanVertName);

				BipartiteNode<String> biNode = new BipartiteNode<String>(cleanVertName);
				int i = 0;
				for(; i < numNouns; i++) {
					String noun = nouns[i].toLowerCase();
					biNode.addChild(noun);
					List<String> hypernymList = hypernymGenerator.getHypernymList(noun);
					TreeNode<String> node = hypernymGenerator.getHypernymTree(hypernymList);
					forest.addNodes(node);
					String topHypernym = Utility.cleanString(hypernymList.get(hypernymList.size()-1), false);
					String cleanNoun = Utility.cleanString(noun, false);
					MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanNoun, MasterDatabaseURIs.MC_BASE_URI + "/" + topHypernym, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/HasTopHypernym/" + cleanNoun + ":" + topHypernym);
				}
				keywordConceptBipartiteGraph.addToKeywordSet(biNode);
			}
		}

		// add mc to mc information to db
		Map<String, Set<String>> mcValueMapping = forest.getValueMapping();
		for(String parentMC : mcValueMapping.keySet()) {
			Set<String> childrenMC = mcValueMapping.get(parentMC);
			String cleanParentMC = Utility.cleanString(parentMC, false);
			// null when node doesn't have a parent
			if(childrenMC != null && !childrenMC.isEmpty()) {
				for(String childMC : childrenMC) {
					String cleanChildMC = Utility.cleanString(childMC, false);
					MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanParentMC);
					MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanChildMC);
					MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanParentMC, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanChildMC, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/ParentOf/" + cleanParentMC + ":" + cleanChildMC);
				}
			}
		}

		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);
		// add keyword to mc information to db
		Map<String, Set<String>> keywordMapping = keywordConceptBipartiteGraph.getKeywordMapping();
		for(String keyword : keywordMapping.keySet()) {
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + engineName + ":" +keyword);
			Set<String> mcList = keywordMapping.get(keyword);
			for(String mc : mcList) {
				String cleanMC = Utility.cleanString(mc, false);
				MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword);
				MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanMC);
				MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanMC, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/ComposedOf/" + keyword + ":" + cleanMC);
			}
		}
		
		Iterator<SEMOSSEdge> edgeItr = edgeStore.values().iterator();
		while(edgeItr.hasNext()) {
			SEMOSSEdge edge = edgeItr.next();
			String edgeName = edge.getProperty(Constants.EDGE_TYPE).toString();
			String firstVertURI = edge.outVertex.getURI();
			String firstVertName = edge.outVertex.getProperty(Constants.VERTEX_NAME).toString();
			String secondVertURI = edge.inVertex.getURI();
			String secondVertName = edge.inVertex.getProperty(Constants.VERTEX_NAME).toString();
			
			String edgeConcat = engineName + ":" + firstVertName + ":" + edgeName + ":" + secondVertName;

			//add a node representing the engine-relation
			//add property on the engine relation node with the name
			//add a relation connecting the engine to the engine-relation node
			//add a relation connecting the in vert to the engine-relation node
			//add a relation connecting the engine-relation node to the out vert

			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, MasterDatabaseURIs.PROP_URI + "/Name", edgeName, false);
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + engineName + ":" + edgeConcat);
			MasterDBHelper.addRelationship(masterEngine, firstVertURI, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Provides/" + firstVertName + ":" + edgeConcat);
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, secondVertURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Consumes/" + edgeConcat + ":" + secondVertName);

		}

		
	}

	public RepositoryConnection getNewRepository() {
		try {
			RepositoryConnection rc = null;
			Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();
			return rc;
		} catch (RepositoryException e) {
			logger.error("Could not get a new repository");
		}
		return null;
	}

	private void addInsights(RepositoryConnection rc) {
		try {
			RepositoryResult<Statement> results = rc.getStatements(null, null, null, true);
			while(results.hasNext()) {
				Statement s = results.next();
				boolean concept = true;
				Object obj = s.getObject();
				if(s.getObject() instanceof Literal) {
					concept = false;
					obj = ( (Value)obj).stringValue();
				}
				this.masterEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{s.getSubject(), s.getPredicate(), obj, concept});
			}
		} catch (RepositoryException e) {
			logger.info("Repository Error adding insights");
		}
	}
	
	public boolean processInsightExecutionForUser(String userId, Insight insight) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		final String userInsight = userId + "-" + insight.getId();
		
		UserPermissionsMasterDB up = new UserPermissionsMasterDB(Constants.LOCAL_MASTER_DB_NAME);
		up.addUser(new User(Constants.ANONYMOUS_USER_ID, "Anonymous", LOGIN_TYPES.anonymous, "Anonymous"));
		
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_USER_INSIGHT.replace("@USERINSIGHT@", userInsight));
		if(!sjsw.hasNext()) {
			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_INSIGHT_LABEL, insight.getLabel(), false);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_EXECUTION_COUNT_PROP_URI, new Double(1), false);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_LAST_EXECUTED_DATE_PROP_URI, new Date(), false);
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + userId, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USER_USERINSIGHT_REL_URI);
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.INSIGHT_BASE_URI + "/" + insight.getId(), MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.INSIGHT_USERINSIGHT_REL_URI);
		} else {
			Double count = 1.0;
			sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_USER_INSIGHT_EXECUTED_COUNT.replace("@USERINSIGHT@", userInsight));
			if(sjsw.hasNext()) {
				String[] names = sjsw.getVariables();
				count = Double.parseDouble(sjsw.next().getVar(names[0]).toString());
			}
			MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userId + "-" + insight.getId(), MasterDatabaseURIs.USERINSIGHT_EXECUTION_COUNT_PROP_URI, count, false);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userId + "-" + insight.getId(), MasterDatabaseURIs.USERINSIGHT_EXECUTION_COUNT_PROP_URI, ++count, false);
		}
		
		masterEngine.commit();
		masterEngine.infer();
		
		return true;
	}

	public String getWordnetPath() {
		return wordnetPath;
	}

	public void setWordnetPath(String wordnetPath) {
		this.wordnetPath = wordnetPath;
	}
}
