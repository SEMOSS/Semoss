/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.hp.hpl.jena.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteFromMasterDB extends ModifyMasterDB {

	public DeleteFromMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}

	public DeleteFromMasterDB() {
		super();
	}
	
	public boolean deleteEngine2(String engineName)
	{
		boolean success = false;
		try
		{
			
			IEngine localMaster = (IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
			deleteEngineConceptsWithProperties(engineName, localMaster);
			deleteEngineConceptsWithoutProperties(engineName, localMaster);
			deleteRelations(engineName, localMaster);
			deleteCoreConcepts(engineName, localMaster);
			deleteEngineMetadata(engineName, localMaster);
			success = true;
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		return success;
	}

	/**
	 * Deletes an engine from the master database.
	 * Uses QuestionAdministrator to remove all perspectives, insights, and params associated with the engine from Master DB.
	 * Removes any keywords, master concept connections, and master concepts that no other engines use, otherwise leaves them alone.
	 * @param engineName	String instance name of the engine to be deleted
	 */
	public boolean deleteEngine(String engineName) {
		boolean success = false;

		//instantiate the master database based on default, or what name is given for it
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		try {
			//delete all engine - keyword relationships
			deleteEngineKeywords(engineName);

			//delete all engine - relationship relationships
			deleteEngineRelationships(engineName);
			
			//delete the insights
//			deleteEngineInsights(engineName);

			//delete the engine
			MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);

			logger.info("Finished deleting engine " + engineName);
			success = true;
		} catch (Exception e) {
			success = false;
		}
		
		try {
			deleteKeywordsWithoutEngines();
			success = true;
		} catch (Exception e) {
			e.printStackTrace();
			success = false;
		}

		masterEngine.commit();
		masterEngine.infer();

		return success;
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
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		for(String engineName : dbArray) {
			try {
				//delete all engine - keyword relationships
				deleteEngineKeywords(engineName);

				//delete all engine - relationship relationships
				deleteEngineRelationships(engineName);
				
				//delete the insights
//				deleteEngineInsights(engineName);

				//delete the engine
				MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);

				logger.info("Finished deleting engine " + engineName);
				successHash.put(engineName, true);
			} catch (Exception e) {
				successHash.put(engineName, false);
			}
		}
		
		try {
			deleteKeywordsWithoutEngines();
			successHash.put("keywordDeletion", true);
		} catch (Exception e) {
			successHash.put("keywordDeletion", false);
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
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		for(String engineName : dbArray) {
			//delete all engine - keyword relationships
			try {
				deleteEngineKeywords(engineName);

				//delete engine URL
				deleteEngineAPI(engineName);

				//delete all engine - relationship relationships
				deleteEngineRelationships(engineName);
				
				//delete insights
//				deleteEngineInsights(engineName);

				//delete the engine
				MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);

				logger.info("Finished deleting engine " + engineName);
				successHash.put(engineName, true);
			} catch (Exception e) {
				successHash.put(engineName, false);
			}
		}
		
		try {
			deleteKeywordsWithoutEngines();
			successHash.put("keywordDeletion", true);
		} catch(Exception e) {
			successHash.put("keywordDeletion", false);
		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}

	public void deleteEngineKeywords(String engineName) {
		String filledKeywordsQuery = MasterDatabaseQueries.KEYWORDS_QUERY.replaceAll("@ENGINE@", engineName);
		ISelectWrapper wrapper = Utility.processQuery(masterEngine,filledKeywordsQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			ISelectStatement sjss = wrapper.next();
			String keywordURI = sjss.getRawVar(names[0]) + "";
			String keyword = sjss.getVar(names[0]) + "";
			MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, keywordURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + engineName + ":" +keyword);
		}
	}

	//TODO refactor question administrator so we dont have to create a new engine
//	private void deleteEngineInsights(String engineName) {
//
//		String filledInsightsQuery = MasterDatabaseQueries.INSIGHTS_QUERY.replaceAll("@ENGINE@", engineName);
//		ISelectWrapper wrapper1 = Utility.processQuery(masterEngine,filledInsightsQuery);
//		String[] names1 = wrapper1.getVariables();
//		while(wrapper1.hasNext())
//		{
//			//grab query results
//			ISelectStatement sjss = wrapper1.next();
//			String insight = (String)sjss.getVar(names1[0]);
//			MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.INSIGHT_BASE_URI + "/" + insight, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Engine:Insight/" + engineName + ":" +insight);
//		}
//
//		ArrayList<String> perspectiveList = new ArrayList<String>();
//		String filledPerspectivesQuery = MasterDatabaseQueries.PERSPECTIVES_QUERY.replaceAll("@ENGINE@", engineName);
//		ISelectWrapper wrapper2 = Utility.processQuery(masterEngine,filledPerspectivesQuery);
//		String[] names2 = wrapper2.getVariables();
//		while(wrapper2.hasNext())
//		{
//			//grab query results
//			ISelectStatement sjss = wrapper2.next();
//			String perspective = (String)sjss.getVar(names2[0]);
//			MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.PERSPECTIVE_BASE_URI + "/" + perspective, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Engine:Perspective/" + engineName + ":" +perspective);
//			perspectiveList.add(perspective);
//		}
//
//		//Use question administrator to remove all the perspectives, insights, and params for this database
//		//set the engine, delete all from each perspective and then store the rc and sc.
//		///////////////////////////////
//		//TODO: FIX LOGIC UNDERNEATH FOR DELETING PERSPECTIVES IN MASTER_ENGINE
////		RDFFileSesameEngine eng = new RDFFileSesameEngine();
////		eng.setEngineName(engineName);
////		eng.setEngineURI2Name(MasterDatabaseURIs.ENGINE_BASE_URI+"/"+engineName);
////		eng.createInsightBase();
////
////		QuestionAdministrator qa = new QuestionAdministrator(eng);
////		qa.setEngineURI2(MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);
////		for(String perspective : perspectiveList) {
////			qa.deleteAllFromPerspective(MasterDatabaseURIs.PERSPECTIVE_BASE_URI + "/" +perspective);
////		}
//
//	}

	private void deleteEngineAPI(String engineName) {
		String filledURLQuery = MasterDatabaseQueries.API_QUERY.replaceAll("@ENGINE@", engineName);
		ISelectWrapper wrapper = Utility.processQuery(masterEngine,filledURLQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			ISelectStatement sjss = wrapper.next();
			String url = (String)sjss.getVar(names[0]);
			MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.PROP_URI + "/" + "API",url,true);
		}
	}
	
	/**
	 * Removes all of the keywords that are no longer associated with engines.
	 * Deletes the relationships from keyword to type, keyword to MC and the keyword itself
	 * Run whenever a user deletes an engine.
	 */
	private void deleteKeywordsWithoutEngines() {
		//create a list of keywords that do not have associated engines
		Set<String> keywordsWithoutEnginesList = new HashSet<String>();
		//create a binding string for queries
		String bindingsStr = "";
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.KEYWORDS_WITHOUT_ENGINES_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			ISelectStatement sjss = wrapper.next();
			String keywordURI = sjss.getRawVar(names[0]) + "";
			keywordsWithoutEnginesList.add(keywordURI);
			bindingsStr = bindingsStr.concat("(<").concat(keywordURI).concat(">)");
		}
		
		if(!bindingsStr.isEmpty()) {
			//delete the keyword type relationships
			String boundKeywordTypeQuery = MasterDatabaseQueries.KEYWORDS_TYPE_QUERY.replaceAll("@BINDINGS@", bindingsStr);
			ISelectWrapper wrapper2 = Utility.processQuery(masterEngine,boundKeywordTypeQuery);
			String[] names2 = wrapper2.getVariables();
			while(wrapper2.hasNext())
			{
				ISelectStatement sjss = wrapper2.next();
				String keywordURI = sjss.getRawVar(names2[0]) + "";
				String keyword = sjss.getVar(names2[0]) + "";
				String typeURI = sjss.getRawVar(names2[1]).toString();
				MasterDBHelper.removeRelationship(masterEngine, keywordURI, typeURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + keyword + ":" + keyword);
				masterEngine.removeStatement(new Object[]{typeURI, RDF.TYPE.stringValue(), MasterDatabaseURIs.RESOURCE_URI, true});
			}
			
			//delete the mc keyword relationships
			String boundMCKeywordsQuery = MasterDatabaseQueries.MC_KEYWORDS_QUERY.replaceAll("@BINDINGS@", bindingsStr);
			ISelectWrapper wrapper3 = Utility.processQuery(masterEngine,boundMCKeywordsQuery);
			String[] names3 = wrapper3.getVariables();
			while(wrapper3.hasNext())
			{
				//grab query results
				ISelectStatement sjss = wrapper3.next();
				String mcURI = sjss.getRawVar(names3[0]) + "";
				String mc = sjss.getVar(names3[0]) + "";
				String keywordURI = sjss.getRawVar(names2[0]) + "";
				String keyword = sjss.getVar(names3[1]) + "";
				MasterDBHelper.removeRelationship(masterEngine, keywordURI, mcURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/ComposedOf/" + keyword + ":" +mc);
			}
			
			//delete the keywords
			Iterator<String> keywordIt = keywordsWithoutEnginesList.iterator();
			while(keywordIt.hasNext()) {
				String keyword = keywordIt.next();
				MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword);
			}
		}
	}

	/**
	 * Removes all of the master concepts that are no longer associated with keywords.
	 * For any master concept that does not have a keyword
	 * AND does not have a child master concept that has a keyword,
	 * deletes the relationships from the master concept to its parent master concept and the mc itself.
	 * This is a deep clean run. NOT run every time an engine is deleted
	 */
	public void deleteMCsWithoutKeywords() {
		//create a list of mcs that do not have keywords associated
		Set<String> mcsWithoutKeywords = new HashSet<String>();
		//create a binding string for queries
		String bindingsStr = "";
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MCS_WITHOUT_KEYWORDS_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			ISelectStatement sjss = wrapper.next();
			String mc = (String)sjss.getVar(names[0]);
			mcsWithoutKeywords.add(mc);
			bindingsStr = bindingsStr.concat("(<").concat(MasterDatabaseURIs.MC_BASE_URI).concat("/").concat(mc).concat(">)");

		}
		
		//delete the parent mcs to any mc that no longer has any keywords bounded
		String boundParentChildMCQuery = MasterDatabaseQueries.PARENT_CHILD_MC_QUERY.replaceAll("@BINDINGS@", bindingsStr);
		ISelectWrapper wrapper2 = Utility.processQuery(masterEngine,boundParentChildMCQuery);
		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext())
		{
			ISelectStatement sjss = wrapper2.next();
			String parentMC = (String)sjss.getVar(names2[0]);
			String childMC = (String)sjss.getVar(names2[1]);
			MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + parentMC, MasterDatabaseURIs.MC_BASE_URI + "/" + childMC, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/ParentOf/" + parentMC + ":" + childMC);
		}
		
		//delete the top hypernyms associated with the mcs that no longer have keywords bounded
		String boundTopHypernymQuery = MasterDatabaseQueries.MC_TOP_HYPERNYM_MC_QUERY.replaceAll("@BINDINGS@", bindingsStr);
		ISelectWrapper wrapper3 = Utility.processQuery(masterEngine,boundTopHypernymQuery);
		String[] names3 = wrapper3.getVariables();
		while(wrapper3.hasNext())
		{
			ISelectStatement sjss = wrapper3.next();
			String mc = (String)sjss.getVar(names3[0]);
			String topHypernymMC = (String)sjss.getVar(names3[1]);
			MasterDBHelper.removeRelationship(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + mc, MasterDatabaseURIs.MC_BASE_URI + "/" + topHypernymMC, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/HasTopHypernym/" + mc + ":" + topHypernymMC);
		}		
		
		//delete the master concepts
		Iterator<String> mcItr = mcsWithoutKeywords.iterator();
		while(mcItr.hasNext()) {
			String mc = mcItr.next();
			MasterDBHelper.removeNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + mc);
		}
	}
	
	private void deleteEngineRelationships(String engineName) {
		Set<String> engineRels = new HashSet<String>();
		//create a binding string for queries
		String bindingsStr = "";
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.ENGINE_RELATIONS.replace("@ENGINE@", engineName));
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			//grab query results
			ISelectStatement sjss = wrapper.next();
			String engineURI = sjss.getRawVar(names[0]) + "";
			String hasURI = sjss.getRawVar(names[1]) + "";
			String engineRelURI = sjss.getRawVar(names[2]) + "";
			engineRels.add(engineRelURI);
			
			MasterDBHelper.removeRelationship(masterEngine, engineURI, engineRelURI, hasURI);
			bindingsStr = bindingsStr.concat("(<").concat(engineRelURI).concat(">)");
		}
		
		if(!bindingsStr.isEmpty()) {
			String q1 = "SELECT DISTINCT ?s ?p ?o WHERE { {?s ?p ?o} {?s a <http://semoss.org/ontologies/Concept/EngineRelation>} {?p <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consumes> } } BINDINGS ?s {@BINDINGS@}";
			String q2 = "SELECT DISTINCT ?s ?p ?o WHERE { {?s ?p ?o} {?o a <http://semoss.org/ontologies/Concept/EngineRelation>} {?p <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provides> } } BINDINGS ?o {@BINDINGS@}";
			q1 = q1.replace("@BINDINGS@", bindingsStr);
			q2 = q2.replace("@BINDINGS@", bindingsStr);
			
			wrapper = Utility.processQuery(masterEngine, q1);
			removeTriples(wrapper);
			wrapper = Utility.processQuery(masterEngine, q2);
			removeTriples(wrapper);
			
			for(String engineRelURI : engineRels) {
				MasterDBHelper.removeNode(masterEngine, engineRelURI);
			}
		}
	}
	
	private void removeTriples(ISelectWrapper wrapper) {
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			//grab query results
			ISelectStatement sjss = wrapper.next();
			String s = sjss.getRawVar(names[0]) + "";
			String p = sjss.getRawVar(names[1]) + "";
			Object o = sjss.getRawVar(names[2]);
			
			boolean isConcept = true;
			if(o instanceof Literal) {
				isConcept = false;
			}
			
			if (o instanceof BigdataLiteralImpl && ((BigdataLiteralImpl) o).getDatatype() != null) {
				try {
					o = ((BigdataLiteralImpl)o).doubleValue();
				} catch(NumberFormatException ex) {
					o = ((BigdataLiteralImpl) o).getLabel();
				}
			} else if(o instanceof Literal){
				o = ((Literal)o).getLabel();
			}
			masterEngine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{s, p, o, isConcept});
		}
	}

	/**
	 * Deletes all triples in the master database.
	 * @throws SailException
	 */
	public void deleteAll() throws SailException{
		masterEngine.removeData("DELETE {?x ?y ?z}");
//		masterEngine.sc.clear();
	}
	
	// Delete the properties and concepts
	private void deleteEngineConceptsWithProperties(String engineName, IEngine fromEngine)
	{
		// first get rid of concept with properties
		String deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?prop ?conceptProp."
				+ "?conceptProp ?type ?semossProp. "
				+ "?concept ?type ?semossConcept."
				+ "?concept ?subclass ?rdfConcept."
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?concept ?in ?engine}"
				+ "{?concept ?prop ?conceptProp}"
				+ "{?concept ?type ?semossConcept}"
				+ "{?concept ?subclass ?rdfConcept}"
				+ "{?conceptProp ?type ?semossProp}"
				+ "FILTER(?in = <http://semoss.org/ontologies/Relation/presentin> &&"
				+ "		  ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> &&"
				+ "		  ?type = <" + RDF.TYPE + "> &&"
				+ "		  ?subclass = <" + RDFS.subClassOf + ">"
				+ ")"
				+"}";

		fromEngine.insertData(deleteQuery);	
	}
	
	// delete concepts without properties
	private void deleteEngineConceptsWithoutProperties(String engineName, IEngine fromEngine)
	{
		// first get rid of concept with properties
		String deleteQuery =  "DELETE "
				+ "{"
				+ "?concept ?type ?semossConcept."
				+ "?concept ?subclass ?rdfConcept."
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?concept ?in ?engine}"
				+ "{?concept ?type ?semossConcept}"
				+ "{?concept ?subclass ?rdfConcept}"
				+ "FILTER(?in = <http://semoss.org/ontologies/Relation/presentin> &&"
				+ "		  ?type = <" + RDF.TYPE + "> &&"
				+ "		  ?subclass = <" + RDFS.subClassOf + ">"
				+ ")"
				+"}";
		fromEngine.insertData(deleteQuery);	
	}
	
	// delete concept relationships
	private void deleteRelations(String engineName, IEngine fromEngine)
	{
		String deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?rel ?anotherConcept."
				+ "?rel ?subprop ?semRel."
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?anotherConcept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?rel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?rel ?subprop ?semRel}"
				+ "FILTER(?subprop = <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> &&"
				+ "		  ?semRel = <http://semoss.org/ontologies/Relation>"
				+ ")"
				+"}";
		
		fromEngine.insertData(deleteQuery);	
	}
	
	// cleanup everything else
	private void deleteCoreConcepts(String engineName, IEngine fromEngine)
	{
		String deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?in ?engine;"
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept ?in  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?concept ?in ?engine}"
				+ "FILTER(?in = <http://semoss.org/ontologies/Relation/presentin>"
				+ ")"
				+"}";

		fromEngine.insertData(deleteQuery);	
	}

	// finally wipe out the engine
	private void deleteEngineMetadata(String engineName, IEngine fromEngine)
	{
		String deleteQuery = "DELETE {?engine ?anyPred ?anyObj} WHERE {"
				+ "{<http://semoss.org/ontologies/meta/engine/" + engineName + "> ?anyPred ?anyObj}"
				+ "{?engine ?anyPred ?anyObj}"
				+ "}";

		fromEngine.insertData(deleteQuery);	
	}
	
}
