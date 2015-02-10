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

import java.util.ArrayList;
import java.util.Hashtable;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;

import prerna.error.EngineException;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteMasterDB extends ModifyMasterDB {

	//queries for relations to delete
	private static final String KEYWORDS_QUERY = "SELECT DISTINCT ?Keyword WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine ?p ?Keyword}}";
	private static final String API_QUERY = "SELECT DISTINCT ?API WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Contains/API> ?API}}";

	private static final String PERSPECTIVES_QUERY = "SELECT DISTINCT ?Perspective WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>}{?Engine ?p ?Perspective}}";
	private static final String INSIGHTS_QUERY = "SELECT DISTINCT ?Insight WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>}{?Engine ?p ?Insight}}";

	public DeleteMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}

	public DeleteMasterDB() {
		super();
	}

	/**
	 * Deletes an engine from the master database.
	 * Uses QuestionAdministrator to remove all perspectives, insights, and params associated with the engine from Master DB.
	 * Removes any keywords, master concept connections, and master concepts that no other engines use, otherwise leaves them alone.
	 * @param engineName	String instance name of the engine to be deleted
	 */
	public Hashtable<String, Boolean> deleteEngine(ArrayList<String> dbArray) {
		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();

		//instantiate the master database based on default, or what name is given for it
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		for(String engineName : dbArray) {
			try {
				//delete all engine - keyword relationships
				deleteEngineKeywords(engineName);

				//delete the insights
				deleteEngineInsights(engineName);

				//delete the engine
				removeNode(engineBaseURI + "/" + engineName);

				logger.info("Finished deleting engine " + engineName);
				successHash.put(engineName, true);
			} catch (EngineException e) {
				successHash.put(engineName, false);
			}
		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}

	/**
	 * Deletes an engine from the master database.
	 * Uses QuestionAdministrator to remove all perspectives, insights, and params associated with the engine from Master DB.
	 * Removes any keywords, master concept connections, and master concepts that no other engines use, otherwise leaves them alone.
	 * @param engineName	String instance name of the engine to be deleted
	 */
	public Hashtable<String, Boolean> deleteEngineWeb(ArrayList<String> dbArray) {
		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();

		//instantiate the master database based on default, or what name is given for it
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		for(String engineName : dbArray) {
			//delete all engine - keyword relationships
			try {
				deleteEngineKeywords(engineName);

				//delete engine URL
				deleteEngineAPI(engineName);

				//delete insights
				deleteEngineInsights(engineName);

				//delete the engine
				removeNode(engineBaseURI + "/" + engineName);

				logger.info("Finished deleting engine " + engineName);
				successHash.put(engineName, true);
			} catch (EngineException e) {
				successHash.put(engineName, false);
			}
		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}

	public void deleteEngineKeywords(String engineName) throws EngineException{
		String filledKeywordsQuery = KEYWORDS_QUERY.replaceAll("@ENGINE@", engineName);
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,filledKeywordsQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			SesameJenaSelectStatement sjss = wrapper.next();
			String keyword = (String)sjss.getVar(names[0]);
			removeRelationship(engineBaseURI + "/" + engineName, keywordBaseURI + "/" + keyword, semossRelationURI + "/Has/" + engineName + ":" +keyword);
			//TODO: keyword is not deleted from the database, just the relationship between keyword and engine is deleted.
		}
	}

	//TODO
	private void deleteEngineInsights(String engineName) throws EngineException{

		String filledInsightsQuery = INSIGHTS_QUERY.replaceAll("@ENGINE@", engineName);
		SesameJenaSelectWrapper wrapper1 = Utility.processQuery(masterEngine,filledInsightsQuery);
		String[] names1 = wrapper1.getVariables();
		while(wrapper1.hasNext())
		{
			//grab query results
			SesameJenaSelectStatement sjss = wrapper1.next();
			String insight = (String)sjss.getVar(names1[0]);
			removeRelationship(engineBaseURI + "/" + engineName, insightBaseURI + "/" + insight, semossRelationURI + "/Engine:Insight/" + engineName + ":" +insight);
		}

		ArrayList<String> perspectiveList = new ArrayList<String>();
		String filledPerspectivesQuery = PERSPECTIVES_QUERY.replaceAll("@ENGINE@", engineName);
		SesameJenaSelectWrapper wrapper2 = Utility.processQuery(masterEngine,filledPerspectivesQuery);
		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext())
		{
			//grab query results
			SesameJenaSelectStatement sjss = wrapper2.next();
			String perspective = (String)sjss.getVar(names2[0]);
			removeRelationship(engineBaseURI + "/" + engineName, PERSPECTIVE_BASE_URI + "/" + perspective, semossRelationURI + "/Engine:Perspective/" + engineName + ":" +perspective);
			perspectiveList.add(perspective);
		}

		//Use question administrator to remove all the perspectives, insights, and params for this database
		//set the engine, delete all from each perspective and then store the rc and sc.
		RDFFileSesameEngine eng = new RDFFileSesameEngine();
		eng.setEngineName(engineName);
		eng.setEngineURI2Name(engineBaseURI+"/"+engineName);
		eng.createInsightBase();
		RDFFileSesameEngine insightBaseXML = eng.getInsightBaseXML();
		insightBaseXML.setRC(masterEngine.rc);
		insightBaseXML.setSC(masterEngine.sc);
		insightBaseXML.setVF(masterEngine.vf);

		QuestionAdministrator qa = new QuestionAdministrator(eng);
		qa.selectedEngine = engineName;
		qa.setEngineURI2(engineBaseURI + "/" + engineName);
		for(String perspective : perspectiveList) {
			qa.deleteAllFromPerspective(PERSPECTIVE_BASE_URI + "/" +perspective);
		}

		masterEngine.rc = (SailRepositoryConnection)(qa.getInsightBaseXML().getRC());
		masterEngine.sc = (SailConnection)(qa.getInsightBaseXML().getSC());

	}

	private void deleteEngineAPI(String engineName) throws EngineException{
		String filledURLQuery = API_QUERY.replaceAll("@ENGINE@", engineName);
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,filledURLQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			SesameJenaSelectStatement sjss = wrapper.next();
			String url = (String)sjss.getVar(names[0]);
			removeProperty(engineBaseURI + "/" + engineName, propURI + "/" + "API",url,true);
		}
	}

	/**
	 * Removes a node given a baseURI
	 * @param nodeURI	String representing the URI for the node type. e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @throws EngineException	Thrown if statement cannot be removed to the engine
	 */
	private void removeNode(String nodeURI) throws EngineException{

		int index = nodeURI.lastIndexOf("/");
		String baseURI = nodeURI.substring(0,index);
		String instance = nodeURI.substring(index+1);

		masterEngine.removeStatement(nodeURI, RDFS.LABEL.stringValue(), instance, false);
		masterEngine.removeStatement(nodeURI, RDF.TYPE.stringValue(), semossConceptURI, true);
		masterEngine.removeStatement(nodeURI, RDF.TYPE.stringValue(), baseURI, true);
		masterEngine.removeStatement(nodeURI, RDF.TYPE.stringValue(), resourceURI, true);
		masterEngine.removeStatement(nodeURI, RDF.TYPE.stringValue(), resourceURI, false);
	}

	/**
	 * Removes just the relationship given the URIs for the two nodes and the URI of the relation
	 * @param node1URI	String representing the full URI of node 1 URI e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param node2URI	String representing the full URI of node 2 URI e.g. http://semoss.org/ontologies/Concept/Keyword/Dog
	 * @param relationURI	String representing the full URI of the relationship http://semoss.org/ontologies/Relation/Has/Dog:Dog
	 * @throws EngineException	Thrown if statement cannot be removed to the engine
	 */
	private void removeRelationship(String node1URI, String node2URI, String relationURI) throws EngineException{
		int relIndex = relationURI.lastIndexOf("/");
		String relBaseURI = relationURI.substring(0,relIndex);
		String relInst = relationURI.substring(relIndex+1);

		masterEngine.removeStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), semossRelationURI, true);
		masterEngine.removeStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), relBaseURI, true);
		masterEngine.removeStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), relationURI, true);
		masterEngine.removeStatement(relationURI, RDFS.LABEL.stringValue(), relInst, false);
		masterEngine.removeStatement(relationURI, RDF.TYPE.stringValue(), Constants.DEFAULT_PROPERTY_URI, true);
		masterEngine.removeStatement(relationURI, RDF.TYPE.stringValue(), resourceURI, true);
		masterEngine.removeStatement(node1URI, semossRelationURI, node2URI, true);
		masterEngine.removeStatement(node1URI, relBaseURI, node2URI, true);
		masterEngine.removeStatement(node1URI, relationURI, node2URI, true);
	}


	/**
	 * Method to remove property on an instance.
	 * @param nodeURI	String containing the node or relationship URI to remove the property from e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param propURI	String representing the URI of the property relation e.g. http://semoss.org/ontologies/Relation/Contains/Weight
	 * @param value	Value to remove as the property e.g. 1.0
	 * @throws EngineException	Thrown if statement cannot be removed to the engine
	 */
	private void removeProperty(String nodeURI, String propURI, Object value,Boolean isConcept) throws EngineException {
		masterEngine.removeStatement(nodeURI, propURI, value, isConcept);
	}

}
