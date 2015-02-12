/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.nameserver;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.sail.memory.model.MemURI;

import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.nlp.TextHelper;
import prerna.error.EngineException;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CreateMasterDB extends ModifyMasterDB {

	private String wordnetPath;
	private HypernymListGenerator hypernymGenerator;
	
	public CreateMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}
	public CreateMasterDB() {
		super();
	}

	public Hashtable<String, Boolean> registerEngineLocal(ArrayList<String> dbArray) throws EngineException {
		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MC_PARENT_CHILD_QUERY);
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
			GraphPlaySheet gps = CentralityCalculator.createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRC(), sparql);
			Hashtable<String, SEMOSSVertex> vertStore  = gps.getGraphData().getVertStore();

			addNewDBConcepts(engineName, vertStore, parentChildMapping);
			RepositoryConnection rc = engine.getInsightDB();
			addInsights(rc);

			logger.info("Finished adding new engine " + engineName);
			successHash.put(engineName, true);
		}

		for(String parent : parentChildMapping.keySet()) {
			logger.info("Parent: " + parent + ". Child: " + parentChildMapping.get(parent));
		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}

	public Hashtable<String, Boolean> registerEngineAPI(String baseURL, ArrayList<String> dbArray) throws EngineException, RDFParseException, RepositoryException, IOException {

		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();

		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MC_PARENT_CHILD_QUERY);
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
			owlRC.add(new StringBufferInputStream(owl), "http://semoss.org", RDFFormat.RDFXML);

			String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
			GraphPlaySheet gps = CentralityCalculator.createMetamodel(owlRC, sparql);
			Hashtable<String, SEMOSSVertex> vertStore  = gps.getGraphData().getVertStore();

			addNewDBConcepts(engineName, vertStore, parentChildMapping);
			addProperty(ENGINE_BASE_URI + "/" + engineName, PROP_URI + "/" + "API",baseURL,true);

			String insights = Utility.retrieveResult(engineAPI + "/getInsightDefinition", null);
			RepositoryConnection insightsRC = getNewRepository();
			insightsRC.add(new StringBufferInputStream(insights), "http://semoss.org", RDFFormat.RDFXML);

			addInsights(insightsRC);

			logger.info("Finished adding new engine " + engineName);
			successHash.put(engineName, true);
		}

		for(String parent : parentChildMapping.keySet()) {
			logger.info("Parent: " + parent + ". Child: " + parentChildMapping.get(parent));
		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}	

	private void addNewDBConcepts(String engineName, Hashtable<String, SEMOSSVertex> vertStore, Map<String, String> parentChildMapping) throws EngineException {
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
					addRelationship(MC_BASE_URI + "/" + cleanNoun, MC_BASE_URI + "/" + topHypernym, SEMOSS_RELATION_URI + "/HasTopHypernym/" + cleanNoun + ":" + topHypernym);
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
					addNode(MC_BASE_URI + "/" + cleanParentMC);
					addNode(MC_BASE_URI + "/" + cleanChildMC);
					addRelationship(MC_BASE_URI + "/" + cleanParentMC, MC_BASE_URI + "/" + cleanChildMC, SEMOSS_RELATION_URI + "/ParentOf/" + cleanParentMC + ":" + cleanChildMC);
				}
			}
		}

		addNode(ENGINE_BASE_URI + "/" + engineName);
		// add keyword to mc information to db
		Map<String, Set<String>> keywordMapping = keywordConceptBipartiteGraph.getKeywordMapping();
		for(String keyword : keywordMapping.keySet()) {
			addRelationship(ENGINE_BASE_URI + "/" + engineName, KEYWORD_BASE_URI + "/" + keyword, SEMOSS_RELATION_URI + "/Has/" + engineName + ":" +keyword);
			Set<String> mcList = keywordMapping.get(keyword);
			for(String mc : mcList) {
				String cleanMC = Utility.cleanString(mc, false);
				addNode(KEYWORD_BASE_URI + "/" + keyword);
				addNode(MC_BASE_URI + "/" + cleanMC);
				addRelationship(KEYWORD_BASE_URI + "/" + keyword, MC_BASE_URI + "/" + cleanMC, SEMOSS_RELATION_URI + "/ComposedOf/" + keyword + ":" + cleanMC);
			}
		}
	}

	/**
	 * Adds a node given a baseURI
	 * @param nodeURI	String representing the URI for the node type. e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @throws EngineException
	 */
	private void addNode(String nodeURI) throws EngineException{

		int index = nodeURI.lastIndexOf("/");
		String baseURI = nodeURI.substring(0,index);
		String instance = nodeURI.substring(index+1);
		masterEngine.addStatement(nodeURI, RDF.TYPE.stringValue(), baseURI, true);
		masterEngine.addStatement(baseURI, RDFS.SUBCLASSOF.stringValue(), SEMOSS_CONCEPT_URI, true);
		masterEngine.addStatement(nodeURI, RDFS.LABEL.stringValue(), instance, false);
	}

	/**
	 * Adds just the relationship given the URIs for the two nodes and the URI of the relation
	 * @param node1URI	String representing the full URI of node 1 URI e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param node2URI	String representing the full URI of node 2 URI e.g. http://semoss.org/ontologies/Concept/Keyword/Dog
	 * @param relationURI	String representing the full URI of the relationship http://semoss.org/ontologies/Relation/Has/Dog:Dog
	 * @throws EngineException
	 */
	private void addRelationship(String node1URI, String node2URI, String relationURI) throws EngineException{
		int relIndex = relationURI.lastIndexOf("/");
		String relBaseURI = relationURI.substring(0,relIndex);
		String relInst = relationURI.substring(relIndex+1);

		masterEngine.addStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), relBaseURI,true);
		masterEngine.addStatement(relBaseURI, RDFS.SUBPROPERTYOF.stringValue(), SEMOSS_RELATION_URI,true);
		masterEngine.addStatement(relationURI, RDFS.LABEL.stringValue(), relInst,false);
		masterEngine.addStatement(node1URI, relationURI, node2URI,true);
	}


	/**
	 * Method to add property on an instance.
	 * @param nodeURI	String containing the node or relationship URI to add the property to e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param propURI	String representing the URI of the property relation e.g. http://semoss.org/ontologies/Relation/Contains/Weight
	 * @param value	Value to add as the property e.g. 1.0
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	public void addProperty(String nodeURI, String propURI, Object value,Boolean isConcept) throws EngineException {
		masterEngine.addStatement(nodeURI, propURI, value, isConcept);
	}

	public RepositoryConnection getNewRepository() {
		try {
			RepositoryConnection rc = null;
			Repository myRepository = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();
			return rc;
		} catch (RepositoryException e) {
			logger.error("Could not get a new repository");
		}
		return null;
	}


	private void addInsights(RepositoryConnection rc) throws EngineException {
		try {
			RepositoryResult<Statement> results = rc.getStatements(null, null, null, true);
			while(results.hasNext()) {
				Statement s = results.next();
				this.masterEngine.sc.addStatement(s.getSubject(),s.getPredicate(),s.getObject(),s.getContext());
				if(s.getPredicate().toString().equals("PARAM:TYPE")) {
					//TODO discuss the relationships
					if(s.getObject() instanceof MemURI) {
						String typeURI = ((MemURI) s.getObject()).stringValue();
						String keyword = typeURI.substring(typeURI.lastIndexOf("/")+1);
						addRelationship(KEYWORD_BASE_URI + "/" + keyword, typeURI, SEMOSS_RELATION_URI + "/Has/" + keyword + ":" + keyword);
						//					this.masterEngine.sc.addStatement(keyword, RDF.TYPE, masterEngine.vf.createURI(keywordBaseURI));
					}
					else {
						logger.info("error adding param to keyword relationship for "+s.getSubject().stringValue()+">>>"+s.getPredicate().stringValue()+">>>"+s.getObject().stringValue());
					}
				}
			}
		} catch (RepositoryException e) {
			logger.info("Repository Error adding insights");
		} catch (SailException e) {
			e.printStackTrace();
			logger.info("Sail Error adding insights");
		}
	}

	public String getWordnetPath() {
		return wordnetPath;
	}

	public void setWordnetPath(String wordnetPath) {
		this.wordnetPath = wordnetPath;
	}
}
