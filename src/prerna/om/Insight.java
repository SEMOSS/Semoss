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
import org.openrdf.model.Literal;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
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
//	private boolean multiInsightQuery;											// boolean if the query is a multi-insight query
	
	private Map<String, Object> propHash = new Hashtable<String, Object>();
	private static final String INSIGHT_NAME = "questionName";					// label of the question
	private static final String OUTPUT_KEY = "output";							// output type of the question
//	private static final String INSIGHT_MAKEUP = "insightMakeup";				// defines how the data is created -- engines, queries, joins
	private static final String DESCRIPTION_KEY = "description";				// description of question
	private static final String ORDER_KEY = "order";							// order of the question
	private static final String PERSPECTIVE_KEY = "perspective"; 				// the perspective for the insight
	private static final String RDBMS_ID = "rdbmsId"; 							// the original idea of insight in the rdbms engine
	private static final String IS_DB_QUERY = "isDbQuery";						// is the query should be run on the owl or database

	private transient IEngine mainEngine;										// the main engine where the insight is stored
	private transient IEngine makeupEngine;										// the in-memory engine created to store the data maker components and transformations for the insight
	private transient IPlaySheet playSheet;										// the playsheet for the insight
	private transient Map<String, String> dataTableAlign;									// the data table align for the insight corresponding to the playsheet
	private transient Gson gson = new Gson();
	
	private transient  Boolean append = false;												// currently used to distinguish when performing overlay in gdm data maker 
	
	private transient IDataMaker dataMaker;										// defines how to make the data for the insight
	private transient String dataMakerName;												// the name of the data maker
	private transient List<DataMakerComponent> dmComponents;					// the list of data maker components in order for creation of insight
	private transient List<DataMakerComponent> optimalComponents;
	private transient Map<String, List<Object>> paramHash;						// the parameters selected by user for filtering on insights
	private transient Vector<SEMOSSParam> insightParameters;					// the SEMOSSParam objects for the insight
	
	// database id where this insight is
	// this may be a URL
	// in memory
	// or a file
	String databaseIDkey = "databaseID";
	
	/**
	 * Constructor for the insight
	 * @param mainEngine					The main engine which holds the insight
	 * @param dataMakerName					The name of the data maker
	 * @param layout						The layout to view the insight
	 */
	public Insight(IEngine mainEngine, String dataMakerName, String layout){
		this.mainEngine = mainEngine;
		this.dataMakerName = dataMakerName;
		this.propHash.put(OUTPUT_KEY, layout);
		// assuming all insights are being run on the database itself as default
		// this can be changed through the setter method
		this.propHash.put(IS_DB_QUERY, true);
	}
	
	/**
	 * Exposed another constructor for creating insights to be stored in some MHS custom playsheets
	 * @param playsheet
	 */
	public Insight(IPlaySheet playsheet){
		this.playSheet = playsheet;
	}
	
	/**
	 * Setter for boolean to see if the insight is an append for gdm
	 * @param append
	 */
	public void setAppend(Boolean append){
		this.append = append;
	}
	
	/**
	 * Getter for boolean to see if the insight is an append for gpm
	 * @return
	 */
	public Boolean getAppend(){
		return this.append;
	}
	
	/**
	 * Get the insightID that is used to store insight in InsightStore
	 * @return
	 */
	public String getInsightID() {
		return this.insightID;
	}
	
	/**
	 * Set the insightID for the insight
	 * @param insightID				The insightID used to store in InsightStore
	 */
	public void setInsightID(String insightID) {
		this.insightID = insightID;
		// update the playsheetID if it is not null
		// playsheetID should match the insightID
		if(this.playSheet != null) {
			this.playSheet.setQuestionID(insightID);
		}
	}
	
//	public void setMultiInsightQuery(boolean multiInsightQuery) {
//		this.multiInsightQuery = multiInsightQuery;
//	}
//	
//	public boolean isMultiInsightQuery() {
//		return this.multiInsightQuery;
//	}
	
	/**
	 * Getter for the name of the insight (question name)
	 * @return
	 */
	public String getInsightName() {
		return (String) this.propHash.get(INSIGHT_NAME);
	}
	
	/**
	 * Setter for the name of the insight (question name)
	 * @param insightName			The name of the insight
	 */
	public void setInsightName(String insightName) {
		this.propHash.put(INSIGHT_NAME, insightName);
	}
	
	/**
	 * Getter for the perspective of the insight
	 * @return
	 */
	public String getPerspective() {
		return (String) this.propHash.get(PERSPECTIVE_KEY);
	}
	
	/**
	 * Setter for the perspective of the insight
	 * @param perspective
	 */
	public void setPerspective(String perspective) {
		this.propHash.put(PERSPECTIVE_KEY, perspective);
	}
	
	/**
	 * Setter for the original id of the insight in the mainEngine's rdbms insights engine
	 * Note, this will be different from the insightID which is assigned during storage in the InsightStore
	 * @param origId
	 */
	public void setRdbmsId(String origId){
		this.propHash.put(RDBMS_ID, origId);
	}
	
	/**
	 * Getter for the original id of the insight in the mainEngine's rdbms insights engine
	 * Note, this will be different from the insightID which is assigned during storage in the InsightStore
	 * @return
	 */
	public String getRdbmsId(){
		return (String) propHash.get(RDBMS_ID);
	}
	
	/**
	 * Getter for the name of the layout to view the insight
	 * @return
	 */
	public String getOutput() {
		return (String) this.propHash.get(OUTPUT_KEY);
	}

//	/**
//	 * Getter for the N-Triples string for the insight makeup
//	 * @return
//	 */
//	public String getMakeup() {
//		return (String) this.propHash.get(INSIGHT_MAKEUP);
//	}

	/**
	 * Takes the input stream for the N-Triples string to create the insight makeup database
	 * @param insightMakeup
	 */
	public void setMakeup(InputStream insightMakeup) {
		if(insightMakeup != null){
			this.makeupEngine = createMakeupEngine(insightMakeup);
		} else{
			System.err.println("Invalid insight. No insight makeup available");
		}
	}

	/**
	 * Getter for the description of the insight
	 * @return
	 */
	public String getDescription() {
		return (String) this.propHash.get(DESCRIPTION_KEY);
	}

	/**
	 * Setter for the description of the insight
	 * @param description			
	 */
	public void setDescription(String description) {
		this.propHash.put(DESCRIPTION_KEY, description);
	}

	/**
	 * Getter for the database id of the insight
	 * @return
	 */
	public String getDatabaseID() {
		return (String) this.propHash.get(this.databaseIDkey);
	}

	/**
	 * Setter for the database id of the insight
	 * @param databaseID
	 */
	public void setDatabaseID(String databaseID) {
		this.propHash.put(this.databaseIDkey, databaseID);
	}
	
	/**
	 * Getter for if the database is a dbQuery and should be run on the database
	 * When false, the makeup is run on the OWL
	 * @return
	 */
	public boolean isDbQuery() {
		return (boolean) this.propHash.get(IS_DB_QUERY);
	}

	/**
	 * Setter for if the database is a dbQuery and should be run on the database
	 * When false, the makeup is run on the OWL
	 * @return
	 */
	public void setDbQuery(boolean dbQuery) {
		this.propHash.put(IS_DB_QUERY, dbQuery);
	}
	
	/**
	 * Sets the order the insight should show up when using current thick client UI
	 * @param order
	 */
	public void setOrder (String order) {
		this.propHash.put(ORDER_KEY, order);
	}

	/**
	 * Gets the order the insight show should up when using current thick client UI
	 * @return
	 */
	public String getOrder() {
		return (String) this.propHash.get(ORDER_KEY);
	}
	
	/**
	 * Sets the parameters the user has selected for the insight 
	 * @param paramHash
	 */
	public void setParamHash(Map<String, List<Object>> paramHash){
		this.paramHash = paramHash;
	}
	
	/**
	 * Gets the makeup engine that contains the insight makeup
	 * @return
	 */
	public IEngine getMakeupEngine() {
		return this.makeupEngine;
	}
	
	/**
	 * Append the parameter filters to the correct component
	 * Each parameter stores the component and filter number that it is affecting
	 */
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
				// logic inside setParamHash in each component to append this information to a filter pre-transformation
				// the filter transformation either appends it to the metamodel or fills a query
				dmComponents.get(compNum).setParamHash(newPHash, filterNum);
			}
		}
	}
	
	/**
	 * Getter for the selected parameter values
	 * @return
	 */
	public Map<String, List<Object>> getParamHash() {
		return this.paramHash;
	}
	
	/**
	 * Setter for the SEMOSSParam objects for the insight
	 * @param insightParameters
	 */
	public void setInsightParameters(Vector<SEMOSSParam> insightParameters) {
		this.insightParameters = insightParameters;
	}

	/**
	 * Getter for the SEMOSSParam objects for the insight
	 * @return
	 */
	public Vector<SEMOSSParam> getInsightParameters() {
		return this.insightParameters;
	}
	
	/**
	 * Generates an in-memory database based on the N-Triples makeup input stream for the insight
	 * @param nTriples				The inputstream holding the N-Triples string
	 * @return
	 */
	public InMemorySesameEngine createMakeupEngine(InputStream nTriples)
	{
		// generate the rc
		RepositoryConnection rc = null;
		try {
			Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
				myRepository.initialize();
			rc = myRepository.getConnection();
			rc.add(nTriples, "semoss.org", RDFFormat.NTRIPLES);
		} catch(RuntimeException ignored) {
			ignored.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// set the rc in the in-memory engine
		InMemorySesameEngine myEng = new InMemorySesameEngine();
		myEng.setRepositoryConnection(rc);
		return myEng;
	}
	
	/**
	 * Process the make-up engine storing the -Triples information to get the data maker components and transformations
	 * @param makeupEng
	 * @return
	 */
	public List<DataMakerComponent> digestNTriples(IEngine makeupEng){
		System.out.println("Creating data component array from makeup engine");
		List<DataMakerComponent> dmCompVec = new Vector<DataMakerComponent>();
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
		
		int idx = -1;
		ISelectWrapper ss = WrapperManager.getInstance().getSWrapper(makeupEng, theQuery);
		
		String curComponent = null;
		// Keeping a set of pre/post/action's that exist on each component such that they are not added twice when number of pre/post/actions is not the same
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
				// old insights store information in a query string while new insights store the metamodel information to construct the query
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
			}
			
			Object preTransURI = st.getRawVar("PreTrans");
			Object postTransURI = st.getRawVar("PostTrans");
			Object actionsURI = st.getRawVar("Actions");
			
			// run queries to get information on each transformation/action and append to dmc list
			if(preTransURI!=null && !preTransSet.contains(preTransURI + "")){
				preTransSet.add(preTransURI + "");
				addPreTrans(dmCompVec.get(idx), makeupEng, preTransURI, curComponent);
			}
			if(postTransURI!=null && !postTransSet.contains(postTransURI + "")){
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
	
	/**
	 * Add PreTransformation to the DataMakerComponent
	 * @param dmc					The DataMakerComponent to add the preTransformation
	 * @param makeupEng				The makeupEngine containing the N-Triples information regarding the insight
	 * @param preTrans				The preTransformation URI
	 * @param compId				The name of the component to make sure the preTransforamtion has the correct id
	 */
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
	
	/**
	 * Add PostTransformation to the DataMakerComponent
	 * @param dmc					The DataMakerComponent to add the preTransformation
	 * @param makeupEng				The makeupEngine containing the N-Triples information regarding the insight
	 * @param postTrans				The postTransformation URI
	 * @param compId				The name of the component to make sure the postTransformation has the correct id
	 */
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
	
	/**
	 * Add Action to the DataMakerComponent
	 * @param dmc					The DataMakerComponent to add the preTransformation
	 * @param makeupEng				The makeupEngine containing the N-Triples information regarding the insight
	 * @param preTrans				The Action URI
	 * @param compId				The name of the component to make sure the Action has the correct id
	 */
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
	
	/**
	 * Get the properties associated with a transformation or an action
	 * @param uri				The URI for the transformation or action
	 * @param makeupEng			The makeupEngine containing the N-Triples information regarding the insight
	 * @return
	 */
	private Map<String, Object> getProperties(String uri, IEngine makeupEng){
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
			LOGGER.error("Need to find reason why/how it was stored this way...");
		}
		return retMap;
	}
	
	/**
	 * Get the data maker object from the dataMakerString
	 * If cannot find specified dataMaker, get the playSheet and see if is supposed to be the data maker
	 * @return
	 */
	public IDataMaker getDataMaker() {
		if(this.dataMaker == null){
			if(this.dataMakerName != null && !this.dataMakerName.isEmpty()) {
				this.dataMaker = Utility.getDataMaker(this.mainEngine, this.dataMakerName);
			} else {
				if(this.playSheet == null){
					this.playSheet = getPlaySheet();
				}
				if(this.playSheet != null) {
					if (this.playSheet instanceof IDataMaker){
						this.dataMaker = (IDataMaker) this.playSheet;
					}
					else {
						this.dataMaker = this.playSheet.getDefaultDataMaker();
						this.playSheet.setDataMaker(this.dataMaker);
					}
				}
			}
		}
		return this.dataMaker;
	}

	/**
	 * Getter for the DataMakerComponents
	 * To prevent unnecessary creation of the dmComponents list, we call digestNTriples if it is null
	 * @return
	 */
	public List<DataMakerComponent> getDataMakerComponents() {
		if(this.dmComponents == null && this.makeupEngine != null){
			this.dmComponents = digestNTriples(this.makeupEngine);
		} else if(this.dmComponents == null) {
			this.dmComponents = new Vector<DataMakerComponent>();
		}
		return this.dmComponents;
	}

	public List<DataMakerComponent> getOptimalDataMakerComponents() {
		if(this.optimalComponents == null && this.makeupEngine != null){
			this.dmComponents = digestNTriples(this.makeupEngine);
			this.optimalComponents = optimizeComponents(this.dmComponents);
		} else if(this.optimalComponents == null) {
			this.optimalComponents = new Vector<DataMakerComponent>();
		}
		return this.optimalComponents;
	}
	
	private List<DataMakerComponent> optimizeComponents(List<DataMakerComponent> dmComponents){
		return null;
	}
	
	/**
	 * Setter for the DataMakerComponents of the insight
	 * @param dmComponents
	 */
	public void setDataMakerComponents(Vector<DataMakerComponent> dmComponents) {
		this.dmComponents = dmComponents;
	}
	
	/**
	 * Getter for the data table align used for view
	 * @return
	 */
	public Map<String, String> getDataTableAlign() {
		return this.dataTableAlign;
	}

	/**
	 * Setter for the data table align used for the view
	 * Primary used when no data table align exists and need to get it based on the playsheet
	 * @param dataTableAlign
	 */
	public void setDataTableAlign(Map<String, String> dataTableAlign) {
		this.dataTableAlign = dataTableAlign;
	}

	/**
	 * Setter for the data table align used for the view
	 * Primarily used when data table align is stored in insight makeup
	 * @param dataTableAlignJSON
	 */
	public void setDataTableAlign(String dataTableAlignJSON) {
		if(dataTableAlignJSON != null && !dataTableAlignJSON.isEmpty()){
			LOGGER.info("Setting json dataTableAlign " + dataTableAlignJSON);
			this.dataTableAlign = gson.fromJson(dataTableAlignJSON, Map.class);
		} else {
			LOGGER.info("data table align is empty");
		}
	}
	
	/**
	 * Gets the playsheet based on the layout string in the insight rdbms
	 * @return
	 */
	public IPlaySheet getPlaySheet(){
		if(this.playSheet == null){
			if(this.dataMaker != null && this.dataMaker instanceof IPlaySheet){
				return (IPlaySheet) this.dataMaker;
			}
			String output = this.getOutput();
			this.playSheet = Utility.getPlaySheet(this.mainEngine, output);
			if(playSheet != null){
				// to keep playsheet ID and insight ID in sync
				this.playSheet.setQuestionID(this.insightID);
				this.playSheet.setDataMaker(getDataMaker());
			}
			else {
				LOGGER.error("Broken insight... cannot get playsheet :: " + output);
			}
		}
		return this.playSheet;
	}
	
	/**
	 * Setter for the playsheet of the insight
	 * @param playSheet
	 */
	public void setPlaySheet(IPlaySheet playSheet){
		this.playSheet = playSheet;
		if(this.playSheet != null) {
			// to keep playsheet ID and insight ID in sync
			this.playSheet.setQuestionID(insightID);
		}
	}

	/**
	 * Getter for the data maker name
	 * @return
	 */
	public String getDataMakerName() {
		return this.dataMakerName;
	}
	
	/**
	 * Process a data maker component on the insight
	 * @param component
	 */
	public void processDataMakerComponent(DataMakerComponent component) {
		int lastComponent = this.getDataMakerComponents().size();
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
		DataMakerComponent componentCopy = component.copy();
		getDataMaker().processDataMakerComponent(component);
		getDataMakerComponents().add(componentCopy);
	}
	
	/**
	 * Process a list of post transformation on the last data maker component stored in cmComponents
	 * @param postTrans					The list of post transformation to run
	 * @param dataMaker					Additional dataMakers if required by the transformation
	 */
	public void processPostTransformation(List<ISEMOSSTransformation> postTrans, IDataMaker... dataMaker) {
		DataMakerComponent dmc = getDataMakerComponents().get(this.dmComponents.size() - 1);
		
		int lastPostTrans = dmc.getPostTrans().size() - 1;
		for(int i = 0; i < postTrans.size(); i++) {
			ISEMOSSTransformation transformation = postTrans.get(i);
			transformation.setId(dmc.getId() + ":" + POST_TRANS + (++lastPostTrans));
			dmc.addPostTrans(transformation.copy());
		}
		
		getDataMaker().processPostTransformations(dmc, postTrans, dataMaker);
	}
	
	/**
	 * Process a list of actions on the last data maker component stored in cmComponents
	 * @param postTrans					The list of actions to run
	 * @param dataMaker					Additional dataMakers if required by the actions
	 */
	public List<Object> processActions(List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		DataMakerComponent dmc = getDataMakerComponents().get(this.dmComponents.size() - 1);
		
		//TODO: extrapolate in datamakercomponent to take in a list
		int lastAction = dmc.getActions().size() - 1;
		for(int i = 0; i < actions.size(); i++) {
			ISEMOSSAction action = actions.get(i);
			action.setId(dmc.getId() + ":" + ACTION + (++lastAction));
			getDataMakerComponents().get(this.dmComponents.size() - 1).addAction(action.copy());
		}
		
		List<Object> actionResults = getDataMaker().processActions(dmc, actions, dataMaker);
		
		return actionResults;
	}
	
	/**
	 * Undo a set of processes (components/transformations/actions) based on the IDs
	 * @param processes
	 */
	public void undoProcesses(List<String> processes){
		// traverse backwards and undo everything in the list
		List<DataMakerComponent> dmcListToRemove = new ArrayList<DataMakerComponent>();
		for(int i = dmComponents.size()-1; i >= 0; i--) {
			DataMakerComponent dmc = dmComponents.get(i);
			List<ISEMOSSAction> actions = dmc.getActions();
			undoActions(actions, processes);
			List<ISEMOSSTransformation> trans = dmc.getPostTrans();
			trans.addAll(dmc.getPreTrans());
			boolean joinUndone = undoTransformations(trans, processes);
			if(joinUndone) {
				// assumption that when no transformations exist, to remove the component from the list
				// this assumption is currently valid as first post transformation is a join
				dmcListToRemove.add(dmc);
			}
		}
		
		// note dmcListToRemove is sorted from largest to smallest
		for(int i = 0; i < dmcListToRemove.size(); i++) {
			dmComponents.remove(dmcListToRemove.get(i));
		}
	}
	
	/**
	 * Undo a set of actions
	 * Actions create a new data structure to hold information and do not affect the current data maker
	 * Thus, only need to remove them from the list of actions stored on the component
	 * @param actions				The list of actions on a specific data maker component
	 * @param processes				The master list of processes to udno
	 */
	private void undoActions(List<ISEMOSSAction> actions, List<String> processes) {
		Iterator<ISEMOSSAction> actionsIt = actions.iterator();
		while(actionsIt.hasNext()) {
			ISEMOSSAction action = actionsIt.next();
			if(processes.contains(action.getId())) {
				actionsIt.remove();
			}
		}
	}
	
	/**
	 * Undo a set of transformations on the data maker
	 * @param actions				The list of transformations on a specific data maker component
	 * @param processes				The master list of processes to undo
	 * @return						true if a JoinTransformation was removed, false otherwise. Used to signal whether the component still should be kept or not
	 */
	private boolean undoTransformations(List<ISEMOSSTransformation> trans, List<String> processes) {
		LOGGER.info("Undoing transformations :  " + processes);
		List<Integer> indicesToRemove = new ArrayList<Integer>();
		// loop through and get the indices corresponding to the trans list to undo
		for(int i = trans.size()-1; i >= 0; i--) {
			ISEMOSSTransformation transformation = trans.get(i);
			if(processes.contains(transformation.getId())) {
				indicesToRemove.add(i);
			}
		}
		
		boolean removedJoin = false;
		// note indicesToRemove is sorted from largest to smallest
		// remove from largest to smallest as it is most efficient way to remove from BTreeDataFrame
		for(int i = 0; i < indicesToRemove.size(); i++) {
			Integer indexToRemove = indicesToRemove.get(i);
			ISEMOSSTransformation transToUndo = trans.get(indexToRemove);
			transToUndo.setDataMakers(this.dataMaker);
			transToUndo.undoTransformation();
			trans.remove(indexToRemove);
			if(transToUndo instanceof JoinTransformation){
				removedJoin = true;
			}
		}
		LOGGER.info("Undo transformations complete. Join transformation undone : " + removedJoin);
		return removedJoin;
	}
	
	/**
	 * Get the information after running an insight to send to FE for viewing result
	 * @return
	 */
	public Map<String, Object> getWebData() {
		Map<String, Object> retHash = new HashMap<String, Object>();
		retHash.put("insightID", getInsightID());
		retHash.put("layout", getOutput());
		retHash.put("title", getInsightName());
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
	
	/**
	 * Get the number of components in the query
	 * @return
	 */
	public int getNumComponents() {
		int numComps = 0;
		if(this.dmComponents != null) {
			numComps = this.dmComponents.size();
		} else if(this.makeupEngine != null) {
			String countQuery = "SELECT (COUNT(DISTINCT(?Component)) AS ?Count) WHERE {?Component a <http://semoss.org/ontologies/Concept/Component>. BIND('x' AS ?x) } GROUP BY ?x";
			ISelectWrapper countss = WrapperManager.getInstance().getSWrapper(this.makeupEngine, countQuery);
			while(countss.hasNext()){
				ISelectStatement countst = countss.next();
				numComps = (int) Double.parseDouble(countst.getVar("Count")+"");
			}
		}
		
		return numComps;
	}
	
	/**
	 * Get the full N-Triples makeup for the insight
	 * @return
	 */
	public String getNTriples() {
		StringBuilder returnStrBuilder = new StringBuilder();
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(this.makeupEngine, "SELECT ?S ?P ?O WHERE {?S ?P ?O}");
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			if(ss.getRawVar(names[2]) instanceof Literal) {
				String val = ss.getVar(names[2]).toString();
				// need to escape single quotes for reloading
				if(val.contains("\"")) {
					val = "\"" + val.replaceAll("\"", "\\\\\"") + "\"";
				} else {
					val = ss.getRawVar(names[2]).toString();
				}
				returnStrBuilder.append("<" + ss.getRawVar(names[0]) + "> <" + ss.getRawVar(names[1]) + "> " + val + " .\n");
			} else {
				returnStrBuilder.append("<" + ss.getRawVar(names[0]) + "> <" + ss.getRawVar(names[1]) + "> <" + ss.getRawVar(names[2]) + "> .\n");
			}
		}
		
		return returnStrBuilder.toString();
	}
}
