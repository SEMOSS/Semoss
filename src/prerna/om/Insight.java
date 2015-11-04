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
package prerna.om;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.google.gson.Gson;

import edu.stanford.nlp.util.logging.PrettyLoggable;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.AbstractPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.Utility;

public class Insight {

	private static final Logger LOGGER = LogManager.getLogger(Insight.class.getName());
	public static final transient String COMP = "Component";
	public static final transient String POST_TRANS = "PostTrans";
	public static final transient String PRE_TRANS = "PreTrans";
	public static final transient String ACTION = "Action";

	// type of database where it is
	public enum DB_TYPE {MEMORY, FILE, REST};
	
	private String insightID;													// id of the question
	private boolean multiInsightQuery;											// boolean if the query is a multi-insight query
	
	private Map<String, Object> propHash = new Hashtable<String, Object>();
	private static final String INSIGHT_NAME = "questionName";					// label of the question
	private static final String OUTPUT_KEY = "output";							// output type of the question
	private static final String INSIGHT_MAKEUP = "insightMakeup";				// defines how the data is created -- engines, queries, joins
	private static final String DESCRIPTION_KEY = "description";				// description of question
	private static final String ORDER_KEY = "order";							// order of the question
	private static final String PERSPECTIVE_KEY = "perspective"; 				// the perspective for the insight
	private static final String RDBMS_ID = "rdbmsId"; 							// the original idea of insight in the rdbms engine
	private static final String IS_DB_QUERY = "isDbQuery";						// is the query should be run on the owl or database

	private transient IEngine mainEngine;
	private transient IEngine makeupEngine;
	private transient IPlaySheet playSheet;
	private Map<String, String> dataTableAlign;
	private Gson gson = new Gson();
	
	private Boolean append = false;
	
	// Defines how to make the data for the insight
	private transient IDataMaker dataMaker;
	private String dataMakerName;
//	private transient DataMakerComponent[] dmComponents;
	private transient List<DataMakerComponent> dmComponents;
	private transient Map<String, List<Object>> paramHash;
	private transient Vector<SEMOSSParam> insightParameters;
	
	// database id where this insight is
	// this may be a URL
	// in memory
	// or a file
	String databaseIDkey = "databaseID";
	
	public Insight(IEngine mainEngine, String dataMakerName, String layout){
		this.mainEngine = mainEngine;
		this.dataMakerName = dataMakerName;
		this.propHash.put(OUTPUT_KEY, layout);
		this.propHash.put(IS_DB_QUERY, true);
//		this.dataMaker = this.getDataMaker();
	}
	
	public Insight(IPlaySheet playsheet){
		this.playSheet = playsheet;
	}
	
	public void setAppend(Boolean append){
		this.append = append;
	}
	
	public Boolean getAppend(){
		return this.append;
	}
	
	public String getInsightID() {
		return this.insightID;
	}
	
	public void setInsightID(String insightID) {
		this.insightID = insightID;
		// update the playsheet id if it is not null
		if(this.playSheet != null) {
			this.playSheet.setQuestionID(insightID);
		}
	}
	
	public void setMultiInsightQuery(boolean multiInsightQuery) {
		this.multiInsightQuery = multiInsightQuery;
	}
	
	public boolean isMultiInsightQuery() {
		return this.multiInsightQuery;
	}
	
	public String getInsightName() {
		return (String) this.propHash.get(INSIGHT_NAME);
	}
	
	public void setInsightName(String insightName) {
		this.propHash.put(INSIGHT_NAME, insightName);
	}
	
	public String getPerspective() {
		return (String) this.propHash.get(PERSPECTIVE_KEY);
	}
	
	public void setPerspective(String perspective) {
		this.propHash.put(PERSPECTIVE_KEY, perspective);
	}
	
	public void setRdbmsId(String origId){
		this.propHash.put(RDBMS_ID, origId);
	}
	
	public String getRdbmsId(){
		return (String) propHash.get(RDBMS_ID);
	}
	
	public String getOutput() {
		return (String) this.propHash.get(OUTPUT_KEY);
	}

	// this returns the xml
	public String getMakeup() {
		return (String) this.propHash.get(INSIGHT_MAKEUP);
	}

	// this sets and then parses the xml
	public void setMakeup(InputStream insightMakeup) {
//		this.propHash.put(INSIGHT_MAKEUP, insightMakeup);
		
		if(insightMakeup != null){
			this.makeupEngine = createMakeupEngine(insightMakeup);
		}
		else{
			System.err.println("Invalid insight. No insight makeup available");
		}
	}

	public String getDescription() {
		return (String) this.propHash.get(DESCRIPTION_KEY);
	}

	public void setDescription(String descr) {
		this.propHash.put(DESCRIPTION_KEY, descr);
	}

	public String getDatabaseID() {
		return (String) this.propHash.get(this.databaseIDkey);
	}

	public void setDatabaseID(String databaseID) {
		this.propHash.put(this.databaseIDkey, databaseID);
	}
	
	public boolean isDbQuery() {
		return (boolean) this.propHash.get(IS_DB_QUERY);
	}

	public void setDbQuery(boolean dbQuery) {
		this.propHash.put(IS_DB_QUERY, dbQuery);
	}
	
	public void setOrder (String order) {
		this.propHash.put(ORDER_KEY, order);
	}

	public String getOrder() {
		return (String) this.propHash.get(ORDER_KEY);
	}
	
	public void setParamHash(Map<String, List<Object>> paramHash){
		this.paramHash = paramHash;
	}
	
	public void appendParamsToDataMakerComponents() {
		if(insightParameters != null) {
			for(int i = 0; i < insightParameters.size(); i++) {
				SEMOSSParam p = insightParameters.get(i);
				String pName = p.getName();
				String componentFilterId = p.getComponentFilterId();
				String[] split = componentFilterId.split(":");
				int compNum = Integer.parseInt(split[0].replace(COMP, ""));
				int filterNum = Integer.parseInt(split[1].replace(PRE_TRANS, ""));
				Map<String, List<Object>> newPHash = new Hashtable<String, List<Object>>();
				newPHash.put(pName, paramHash.get(pName));
				dmComponents.get(compNum).setParamHash(newPHash, filterNum);
			}
		}
	}
	
	public Map<String, List<Object>> getParamHash() {
		return this.paramHash;
	}
	
	public void setInsightParameters(Vector<SEMOSSParam> insightParameters) {
		this.insightParameters = insightParameters;
	}

	public Vector<SEMOSSParam> getInsightParameters() {
		return this.insightParameters;
	}
	
	public InMemorySesameEngine createMakeupEngine(InputStream xml)
	{
		RepositoryConnection rc = null;
		try
		{
//			String nTriplesString = IOUtils.toString(xml); 
//			System.out.println(nTriplesString);

			Repository myRepository = new SailRepository(
							new ForwardChainingRDFSInferencer(
							new MemoryStore()));
				myRepository.initialize();
			rc = myRepository.getConnection();
			rc.add(xml, "semoss.org", RDFFormat.NTRIPLES);
		}catch(RuntimeException ignored) {
			ignored.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		InMemorySesameEngine myEng = new InMemorySesameEngine();
		myEng.setRepositoryConnection(rc);
		return myEng;
	}
	
	private List<DataMakerComponent> digestXML(IEngine makeupEng){
		System.out.println("Creating data component array from makeup engine");
		List<DataMakerComponent> dmCompVec = new Vector<DataMakerComponent>();
//		this.makeupArray = new Vector<Object>();
		
//		String initQuery = "SELECT ?s ?p ?o WHERE { ?s ?p ?o}";
//		ISelectWrapper ss = WrapperManager.getInstance().getSWrapper(makeupEng, initQuery);
//		while(ss.hasNext()){
//			ISelectStatement st = ss.next();
//			String s = st.getRawVar("s")+"";
//			String p = st.getRawVar("p")+"";
//			String o = st.getRawVar("o")+"";
//			System.out.println(s + " ::::::: " + p + "  ::::::: " + o);
//		}
		
		String countQuery = "SELECT (COUNT(DISTINCT(?Component)) AS ?Count) WHERE {?Component a <http://semoss.org/ontologies/Concept/Component>. BIND('x' AS ?x) } GROUP BY ?x";

		ISelectWrapper countss = WrapperManager.getInstance().getSWrapper(makeupEng, countQuery);
		Integer total = 0;
		while(countss.hasNext()){
			ISelectStatement countst = countss.next();
			total = (int) Double.parseDouble(countst.getVar("Count")+"");
			System.out.println(" THERE ARE " + total + " COMPONENTS IN THIS INSIGHT  ");
		}
		//TODO: need to make sure preTrans, postTrans, and actions are all ordered
		String theQuery = "SELECT ?Component ?Engine ?Query ?Metamodel ?DataMakerType ?PreTrans ?PostTrans ?Actions WHERE { {?Component a <http://semoss.org/ontologies/Concept/Component>} {?Engine a <http://semoss.org/ontologies/Concept/Engine>} {?Component <Comp:Eng> ?Engine} OPTIONAL {?Component <http://semoss.org/ontologies/Relation/Contains/Query> ?Query} OPTIONAL {?Component <http://semoss.org/ontologies/Relation/Contains/Metamodel> ?Metamodel} {?Component <http://semoss.org/ontologies/Relation/Contains/Order> ?Order} OPTIONAL {?Component <Comp:PreTrans> ?PreTrans} OPTIONAL {?Component <Comp:PostTrans> ?PostTrans} OPTIONAL {?Component <Comp:Action> ?Actions} } ORDER BY ?Order";
		
//		this.dmComponents = new DataMakerComponent[total];
		int idx = -1;
		ISelectWrapper ss = WrapperManager.getInstance().getSWrapper(makeupEng, theQuery);
		
		String curComponent = null;
		Set<String> preTransSet = new HashSet<String>();
		Set<String> postTransSet = new HashSet<String>();
		Set<String> actionSet = new HashSet<String>();
		while(ss.hasNext()){
			ISelectStatement st = ss.next();
			String component = st.getVar("Component")+"";
			String newComponent = COMP + component;
			if(!newComponent.equals(curComponent)){
				String engine = st.getVar("Engine")+"";
				String query = st.getVar("Query")+"";
				String metamodelString = st.getVar("Metamodel")+"";
				System.out.println(engine + " ::::::: " + component +" ::::::::: " + query + " :::::::::: " + metamodelString);
				
				DataMakerComponent dmc = null;
				if (!query.isEmpty()) {
					dmc = new DataMakerComponent(engine, query); 
					dmCompVec.add(dmc);
				}
				else if (!metamodelString.isEmpty()){
					Map<String, Object> metamodelData = gson.fromJson(metamodelString, Map.class);
					dmc = new DataMakerComponent(engine, metamodelData); 
					dmCompVec.add(dmc);
				}
				curComponent = COMP + component;
				dmc.setId(curComponent);
				idx++;
//				this.dmComponents[idx] = new DataMakerComponent(engine, query);
			}
			
			Object preTransURI = st.getRawVar("PreTrans");
			Object postTransURI = st.getRawVar("PostTrans");
			Object actionsURI = st.getRawVar("Actions");

			if(preTransURI!=null && !preTransSet.contains(preTransURI + "")){
//				addPreTrans(this.dmComponents[idx], makeupEng, preTransURI);
				preTransSet.add(preTransURI + "");
				addPreTrans(dmCompVec.get(idx), makeupEng, preTransURI, curComponent);
			}
			if(postTransURI!=null && !postTransSet.contains(postTransURI + "")){
//				addPostTrans(this.dmComponents[idx], makeupEng, postTransURI);
				postTransSet.add(postTransURI + "");
				addPostTrans(dmCompVec.get(idx), makeupEng, postTransURI, curComponent);
			}
			if(actionsURI!=null && !actionSet.contains(actionsURI + "")) {
				actionSet.add(actionsURI + "");
				addAction(dmCompVec.get(idx), makeupEng, actionsURI, curComponent);
			}
		}
		return dmCompVec;
	}
	
	private void addPreTrans(DataMakerComponent dmc, IEngine makeupEng, Object preTrans, String compId){
		LOGGER.info("adding pre trans :::: " + preTrans);
		Map<String, Object> props = getProperties(preTrans+"", makeupEng);
		String type = props.get(ISEMOSSTransformation.TYPE) + "";
		LOGGER.info("TRANS TYPE IS " + type);
		ISEMOSSTransformation trans = Utility.getTransformation(this.mainEngine, type);
		LOGGER.info("pre trans properties :::: " + props.toString());
		trans.setProperties(props);
		trans.setId(compId + ":" + PRE_TRANS + Utility.getInstanceName(preTrans + ""));
		dmc.addPreTrans(trans);
	}
	
	private void addPostTrans(DataMakerComponent dmc, IEngine makeupEng, Object postTrans, String compId){
		LOGGER.info("adding post trans :::: " + postTrans);
		Map<String, Object> props = getProperties(postTrans+"", makeupEng);
		String type = props.get(ISEMOSSTransformation.TYPE) + "";
		LOGGER.info("TRANS TYPE IS " + type);
		ISEMOSSTransformation trans = Utility.getTransformation(this.mainEngine, type);
		LOGGER.info("post trans properties :::: " + props.toString());
		trans.setProperties(props);
		trans.setId(compId + ":" + POST_TRANS + Utility.getInstanceName(postTrans + ""));
		dmc.addPostTrans(trans);
	}
	
	private void addAction(DataMakerComponent dmc, IEngine makeupEng, Object action, String compId){
		LOGGER.info("adding action :::: " + action);
		Map<String, Object> props = getProperties(action+"", makeupEng);
		String type = props.get(ISEMOSSAction.TYPE) + "";
		LOGGER.info("TRANS TYPE IS " + type);
		ISEMOSSAction actionObj = Utility.getAction(this.mainEngine, type);
		LOGGER.info("action properties :::: " + props.toString());
		actionObj.setProperties(props);
		actionObj.setId(compId + ":" + ACTION + Utility.getInstanceName(action + ""));
		dmc.addAction(actionObj);
	}
	
	private Map<String, Object> getProperties(String uri, IEngine makeupEng){
//		String propQuery = "SELECT ?Prop ?Value ?Type WHERE { BIND(<" + 
//							transURI + 
//							"> AS ?trans) {?Prop a <http://semoss.org/ontologies/Relation/Contains>} {?trans ?Prop ?Value}"
//							+ "{?trans <http://semoss.org/ontologies/Relation/Contains/Type> ?Type } }";
		String propQuery = "SELECT ?Value WHERE { BIND(<" + 
				uri + 
				"> AS ?obj) {?obj <http://semoss.org/ontologies/Relation/Contains/propMap> ?Value}}";
		
		LOGGER.info("Running query to get properties: " + propQuery);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(makeupEng, propQuery);
		Map<String, Object> retMap = new HashMap<String, Object>();
		if(wrap.hasNext()){ // there should only be one prop map associated with each transformation or action
			ISelectStatement ss = wrap.next();
			String jsonPropMap = ss.getVar("Value") + "";
			LOGGER.info(jsonPropMap);
			retMap = gson.fromJson(jsonPropMap, Map.class);
		}
		if(wrap.hasNext()){
			LOGGER.error("More than one prop map has shown up for uri ::::: " + uri);
		}
		return retMap;
	}
	
	public static void main(String [] args){
		String exampleXML = "<http://semoss.org/ontologies/Concept/Engine> <http://www.w3.org/1999/02/22-rdf-syntax-ns#subClassOf> <http://semoss.org/ontologies/Concept>.\n<http://semoss.org/ontologies/Concept/Engine/Movie_DB> <http://www.w3.org/2000/01/rdf-schema#typeOf> <http://semoss.org/ontologies/Concept/Engine>.\n<http://semoss.org/ontologies/Concept/QueryNum> <http://www.w3.org/1999/02/22-rdf-syntax-ns#subClassOf> <http://semoss.org/ontologies/Concept>.\n<http://semoss.org/ontologies/Concept/QueryNum/1> <http://www.w3.org/2000/01/rdf-schema#typeOf> <http://semoss.org/ontologies/Concept/QueryNum>.\n<http://semoss.org/ontologies/Concept/QueryNum/1> <http://semoss.org/ontolgoies/Relation/Contains/Query> \"SELECT DISTINCT  ?Genre (AVG(?Revenue_Domestic) AS ?Revenue_Domestic_AVG) WHERE {{?Title <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Title>} {?Genre <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Genre>}{?Title <http://semoss.org/ontologies/Relation> ?Genre}{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic> ?Revenue_Domestic}} GROUP BY ?Genre ORDER BY ?Genre\".\n";
		
//		Insight mysite = new Insight();
		
//		mysite.setMakeup(exampleXML);
		
//		System.out.println(" Engines : " + mysite.getEngineArray());
//		System.out.println(" Queries : " + mysite.getQueryArray());
//		System.out.println(" Joins : " + mysite.getJoinArray());
	}


	public IDataMaker getDataMaker() {
		if(this.dataMaker == null){
			if(this.dataMakerName != null && !this.dataMakerName.isEmpty()) {
				this.dataMaker = Utility.getDataMaker(this.mainEngine, this.dataMakerName);
			} else {
				if(this.playSheet == null){
					this.playSheet = getPlaySheet();
				}
				if (this.playSheet instanceof IDataMaker){
					this.dataMaker = (IDataMaker) this.playSheet;
				}
				else if (this.playSheet != null){
					this.dataMaker = this.playSheet.getDefaultDataMaker();
				}
			}
		}
		return this.dataMaker;
	}

//	public DataMakerComponent[] getDataMakerComponents() {
//		return this.dmComponents;
//	}
	
	public List<DataMakerComponent> getDataMakerComponents() {
		if(this.dmComponents == null && this.makeupEngine != null){
			this.dmComponents = digestXML(this.makeupEngine);
		} else if(this.dmComponents == null) {
			this.dmComponents = new Vector<DataMakerComponent>();
		}
		return this.dmComponents;
	}
	
//	public void setDataMaker(IDataMaker dm){
//		this.dataMaker = dm;
//	}

//	public void setDataMakerComponents(DataMakerComponent[] dmComponents) {
//		this.dmComponents = dmComponents;
//	}

	public void setDataMakerComponents(Vector<DataMakerComponent> dmComponents) {
		this.dmComponents = dmComponents;
	}
	
	public Map<String, String> getDataTableAlign() {
		return this.dataTableAlign;
	}

	public void setDataTableAlign(Map<String, String> dataTableAlign) {
		this.dataTableAlign = dataTableAlign;
	}

	public void setDataTableAlign(String dataTableAlignJSON) {
		if(dataTableAlignJSON != null && !dataTableAlignJSON.isEmpty()){
			LOGGER.info("Setting json dataTableAlign " + dataTableAlignJSON);
			this.dataTableAlign = gson.fromJson(dataTableAlignJSON, Map.class);
		} else {
			LOGGER.info("data table align is empty");
		}
	}
	
	public IPlaySheet getPlaySheet(){
		if(this.playSheet == null){
			String output = this.getOutput();
			this.playSheet = Utility.getPlaySheet(this.mainEngine, output);
			if(playSheet != null){
				this.playSheet.setQuestionID(this.insightID);
			}
			else {
				LOGGER.error("Broken insight... cannot get playsheet :: " + output);
			}
		}
		return this.playSheet;
	}
	
	public void setPlaySheet(IPlaySheet playSheet){
		this.playSheet = playSheet;
		if(this.playSheet != null) {
			this.playSheet.setQuestionID(insightID);
		}
	}

	public String getDataMakerName() {
		return this.dataMakerName;
	}
	
	public void processDataMakerComponent(DataMakerComponent component) {
		int lastComponent = 0;
		if(this.dmComponents != null) {
			lastComponent = this.dmComponents.size();
		}
		String compId = COMP + lastComponent;
		component.setId(compId);
		List<ISEMOSSTransformation> preTrans = component.getPreTrans();
		for(int i = 0; i < preTrans.size(); i++) {
			preTrans.get(i).setId(compId + ":" + PRE_TRANS + i);
		}
		List<ISEMOSSTransformation> postTrans = component.getPostTrans();
		for(int i = 0; i < postTrans.size(); i++) {
			postTrans.get(i).setId(compId + ":" + POST_TRANS + i);
		}
		List<ISEMOSSAction> actions = component.getActions();
		for(int i = 0; i < actions.size(); i++) {
			actions.get(i).setId(compId + ":" + ACTION + i);
		}
		getDataMaker().processDataMakerComponent(component);
		getDataMakerComponents().add(component);
	}
	
	public void processPostTransformation(List<ISEMOSSTransformation> postTrans, IDataMaker... dataMaker) {
		DataMakerComponent dmc = getDataMakerComponents().get(this.dmComponents.size() - 1);
		getDataMaker().processPostTransformations(dmc, postTrans, dataMaker);
		//TODO: extrapolate in datamakercomponent to take in a list
		int lastPostTrans = dmc.getPostTrans().size() - 1;
		for(int i = 0; i < postTrans.size(); i++) {
			postTrans.get(i).setId(dmc.getId() + ":" + POST_TRANS + (++lastPostTrans));
			dmc.addPostTrans(postTrans.get(i));
		}
	}
	
	public List<Object> processActions(List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		DataMakerComponent dmc = getDataMakerComponents().get(this.dmComponents.size() - 1);
		List<Object> actionResults = getDataMaker().processActions(dmc, actions, dataMaker);
		//TODO: extrapolate in datamakercomponent to take in a list
		int lastAction = dmc.getActions().size() - 1;
		for(int i = 0; i < actions.size(); i++) {
			actions.get(i).setId(dmc.getId() + ":" + ACTION + (++lastAction));
			getDataMakerComponents().get(this.dmComponents.size() - 1).addAction(actions.get(i));
		}
		
		return actionResults;
	}
	
	public void undoProcesses(List<String> processes){
		// traverse backwards and undo everything in the list
		List<Integer> dmcListToRemove = new ArrayList<Integer>();
		for(int i = dmComponents.size()-1; i >= 0; i--) {
			DataMakerComponent dmc = dmComponents.get(i);
			List<ISEMOSSAction> actions = dmc.getActions();
			undoActions(actions, processes);
			List<ISEMOSSTransformation> postTrans = dmc.getPostTrans();
			undoTransformations(postTrans, processes);
			if(postTrans.isEmpty()) {
				dmcListToRemove.add(i);
			}
		}
		
		//note dmcListToRemove is sorted from largest to smallest
		for(int i = 0; i < dmcListToRemove.size(); i++) {
			dmComponents.remove(dmcListToRemove.get(i));
		}
	}
	
	private void undoActions(List<ISEMOSSAction> actions, List<String> processes) {
		Iterator<ISEMOSSAction> actionsIt = actions.iterator();
		while(actionsIt.hasNext()) {
			ISEMOSSAction action = actionsIt.next();
			if(processes.contains(action.getId())) {
				actionsIt.remove();
			}
		}
	}
	
	private void undoTransformations(List<ISEMOSSTransformation> trans, List<String> processes) {
		List<Integer> indicesToRemove = new ArrayList<Integer>();
		for(int i = trans.size()-1; i >= 0; i--) {
			ISEMOSSTransformation transformation = trans.get(i);
			if(processes.contains(transformation.getId())) {
				indicesToRemove.add(i);
			}
		}
		
		//note indicesToRemove is sorted from largest to smallest
		for(int i = 0; i < indicesToRemove.size(); i++) {
			Integer indexToRemove = indicesToRemove.get(i);
			trans.get(indexToRemove).undoTransformation();
			trans.remove(indexToRemove);
		}
	}
	
	public Map<String, Object> getWebData() {
		Map<String, Object> retHash = new HashMap<String, Object>();
		retHash.put("insightID", insightID);
		retHash.put("layout", propHash.get(OUTPUT_KEY));
		retHash.put("title", propHash.get(INSIGHT_NAME));
		// this is only because we don’t currently save data table align
		// in these annoying cases, we need to use the playsheet L 
		if(dataTableAlign == null || dataTableAlign.isEmpty()) {
			getPlaySheet();
			this.playSheet.setDataMaker(getDataMaker());
			dataTableAlign = ((AbstractPlaySheet) this.playSheet).getDataTableAlign();
		}
		if(dataTableAlign != null){ // some playsheets don't require data table align, like grid play sheet. Should probably change this so they all have data table align (like if i want to change the order of my columns)
			retHash.put("dataTableAlign", dataTableAlign);
		}
		// TODO: how do i get this outside of here? we need to return incremental stores during traversing but not when recreating insight
		if(getDataMaker() instanceof GraphDataModel) {
			((GraphDataModel) getDataMaker()).setOverlay(false);
		}
		// this will return the data from the dataMaker
		// if has a do-method, dataMaker is the playsheet and can use those methods but can also use a datamaker
		// i.e.gdm would be used for the method below (to get the edges/nodes)
		
		// right now, assuming only one action is present
		// TODO: should we update the interface to always return a map
		// currently all actions return a map
		if(getDataMaker().getActionOutput() != null && !getDataMaker().getActionOutput().isEmpty()) {
			retHash.putAll( (Map) getDataMaker().getActionOutput().get(0));
		} else {
			retHash.putAll(getDataMaker().getDataMakerOutput());
		}
		return retHash;
	}
}
