package prerna.om;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.jdbc.JdbcClob;
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
import com.google.gson.reflect.TypeToken;

import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.QueryBuilderData;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.FilterTransformation;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.ui.components.playsheets.datamakers.PKQLTransformation;
import prerna.util.Constants;
import prerna.util.Utility;

@Deprecated
public class OldInsight extends Insight {

	/*
	 * THIS IS FOR LEGACY INSIGHTS
	 * IF YOU NEED TO USE THIS CLASS... 
	 * YOU ARE PROBABLY DOING SOMETHING WRONG
	 */
	
	private static final Logger logger = LogManager.getLogger(OldInsight.class);
	
	public static final transient String COMP = "Component";
	public static final transient String POST_TRANS = "PostTrans";
	public static final transient String PRE_TRANS = "PreTrans";
	public static final transient String ACTION = "Action";

	@Deprecated
	private String engineId;
	@Deprecated
	private String engineName;
	
	private transient IDatabaseEngine mainEngine;
	private transient IProject mainProject;										// the main engine where the insight is stored
	private transient IDatabaseEngine makeupEngine;										// the in-memory engine created to store the data maker components and transformations for the insight
	private transient IPlaySheet playSheet;										// the playsheet for the insight
	private transient Map<String, String> dataTableAlign;						// the data table align for the insight corresponding to the playsheet
	private transient Gson gson = new Gson();
	
	private transient  Boolean append = false;									// currently used to distinguish when performing overlay in gdm data maker 

	private transient String dataMakerName;												// the name of the data maker
	private transient List<DataMakerComponent> dmComponents;					// the list of data maker components in order for creation of insight
	private transient Map<String, List<Object>> paramHash;						// the parameters selected by user for filtering on insights
	private transient List<SEMOSSParam> insightParameters;					// the SEMOSSParam objects for the insight
	private String uiOptions;
	protected String layout;

	private transient IDataMaker dataMaker;										// defines how to make the data for the insight
	
	public OldInsight() {
		
	}
	
	/**
	 * Constructor for the insight
	 * REQUIRES PROJECT AND APP TO HAVE SAME ID
	 * @param mainEngine					The main engine which holds the insight
	 * @param dataMakerName					The name of the data maker
	 * @param layout						The layout to view the insight
	 */
	public OldInsight(IDatabaseEngine mainEngine, String dataMakerName, String layout){
		this.mainEngine = mainEngine;
		// NEED THE ID TO BE THE SAME IN THIS SITUATION!!!
		// the main engine has the same id as the main project
		this.mainProject = Utility.getProject(mainEngine.getEngineId());
		this.dataMakerName = dataMakerName;
		this.layout = layout;
	}
	
	/**
	 * Constructor for the insight
	 * REQUIRES PROJECT AND APP TO HAVE SAME ID
	 * @param mainProject					The main project which holds the insight
	 * @param dataMakerName					The name of the data maker
	 * @param layout						The layout to view the insight
	 */
	public OldInsight(IProject mainProject, String dataMakerName, String layout){
		this.mainProject = mainProject;
		// NEED THE ID TO BE THE SAME IN THIS SITUATION!!!
		// the main engine has the same id as the main project
		this.mainEngine = Utility.getDatabase(mainProject.getProjectId());
		this.dataMakerName = dataMakerName;
		this.layout = layout;
	}
	
	public OldInsight(String dataMakerName, String layout) {
		this.dataMakerName = dataMakerName;
		this.layout = layout;
	}
	
	/**
	 * Exposed another constructor for creating insights to be stored in some MHS custom playsheets
	 * @param playsheet
	 */
	public OldInsight(IPlaySheet playsheet){
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
	 * Takes the input stream for the N-Triples string to create the insight makeup database
	 * @param insightMakeup
	 */
	public void setMakeup(InputStream insightMakeup) {
		if(insightMakeup != null){
			this.makeupEngine = createMakeupEngine(insightMakeup);
		} else{
			logger.error("Invalid insight. No insight makeup available");
		}
	}
	
	/**
	 * Takes the input stream for the N-Triples string to create the insight makeup database
	 * @param insightMakeup
	 */
	public void setMakeup(String insightMakeup) {
		if(insightMakeup != null){
			this.makeupEngine = createMakeupEngine(insightMakeup);
		} else{
			logger.error("Invalid insight. No insight makeup available");
		}
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
	public IDatabaseEngine getMakeupEngine() {
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
	public void setInsightParameters(List<SEMOSSParam> insightParameters) {
		this.insightParameters = insightParameters;
	}

	/**
	 * Getter for the SEMOSSParam objects for the insight
	 * @return
	 */
	public List<SEMOSSParam> getInsightParameters() {
		return this.insightParameters;
	}
	
	/**
	 * Generates an in-memory database based on the N-Triples makeup input stream for the insight
	 * @param nTriples				The inputstream holding the N-Triples string
	 * @return
	 */
	public InMemorySesameEngine createMakeupEngine(String nTriples) {
		return createMakeupEngine(new ByteArrayInputStream(nTriples.getBytes()));
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
			logger.error(Constants.STACKTRACE, ignored);
		} catch (RDFParseException rpe) {
			logger.error(Constants.STACKTRACE, rpe);
		} catch (RepositoryException re) {
			logger.error(Constants.STACKTRACE, re);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
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
	public List<DataMakerComponent> digestNTriples(IDatabaseEngine makeupEng){
		logger.debug("Creating data component array from makeup engine");
		List<DataMakerComponent> dmCompVec = new Vector<>();
		String countQuery = "SELECT (COUNT(DISTINCT(?Component)) AS ?Count) WHERE {?Component a <http://semoss.org/ontologies/Concept/Component>. BIND('x' AS ?x) } GROUP BY ?x";

		ISelectWrapper countss = WrapperManager.getInstance().getSWrapper(makeupEng, countQuery);
		Integer total = 0;
		while(countss.hasNext()){
			ISelectStatement countst = countss.next();
			total = (int) Double.parseDouble(countst.getVar("Count")+"");
			logger.debug(" THERE ARE " + total + " COMPONENTS IN THIS INSIGHT  ");
		}
		//TODO: need to make sure preTrans, postTrans, and actions are all ordered
		String theQuery = "SELECT ?Component ?Engine ?Query ?Metamodel ?DataMakerType ?PreTrans ?PostTrans ?Actions WHERE { {?Component a <http://semoss.org/ontologies/Concept/Component>} {?EngineURI a <http://semoss.org/ontologies/Concept/Engine>} {?EngineURI <http://semoss.org/ontologies/Relation/Contains/Name> ?Engine } {?Component <Comp:Eng> ?EngineURI} OPTIONAL {?Component <http://semoss.org/ontologies/Relation/Contains/Query> ?Query} OPTIONAL {?Component <http://semoss.org/ontologies/Relation/Contains/Metamodel> ?Metamodel} {?Component <http://semoss.org/ontologies/Relation/Contains/Order> ?Order} OPTIONAL {?Component <Comp:PreTrans> ?PreTrans} OPTIONAL {?Component <Comp:PostTrans> ?PostTrans} OPTIONAL {?Component <Comp:Action> ?Actions} } ORDER BY ?Order";
		
		int idx = -1;
		ISelectWrapper ss = WrapperManager.getInstance().getSWrapper(makeupEng, theQuery);
		
		String curComponent = null;
		// Keeping a set of pre/post/action's that exist on each component such that they are not added twice when number of pre/post/actions is not the same
		Set<String> preTransSet = new HashSet<>();
		Set<String> postTransSet = new HashSet<>();
		Set<String> actionSet = new HashSet<>();
		while(ss.hasNext()){
			ISelectStatement st = ss.next();
			String component = st.getVar("Component")+"";
			String newComponent = COMP + component;
			if(!newComponent.equals(curComponent)){
				String engine = st.getVar("Engine")+"";
				engine = MasterDatabaseUtility.testDatabaseIdIfAlias(engine);
				String query = st.getVar("Query")+"";
				String metamodelString = st.getVar("Metamodel")+"";
				logger.debug(engine + " ::::::: " + component +" ::::::::: " + query + " :::::::::: " + metamodelString);
				
				DataMakerComponent dmc = null;
				// old insights store information in a query string while new insights store the metamodel information to construct the query
				if (!query.isEmpty()) {
					dmc = new DataMakerComponent(engine, query); 
					dmCompVec.add(dmc);
				}
				else if (!metamodelString.isEmpty()){
					logger.info("trying to get QueryBuilderData object");
					QueryBuilderData metamodelData = gson.fromJson(metamodelString, QueryBuilderData.class);
					QueryStruct qsData = null;
					if(metamodelData.getRelTriples() == null){
						qsData = gson.fromJson(metamodelString, QueryStruct.class);
						if(qsData.getSelectors() == null){
							logger.info("failed to get QueryBuilderData.... this must be a legacy insight with metamodel data. Setting metamodel data into QueryBuilderData");
							Hashtable<String, Object> dataHash = gson.fromJson(metamodelString, new TypeToken<Hashtable<String, Object>>() {}.getType());
							metamodelData = new QueryBuilderData(dataHash);
						}
					}
					if(qsData == null) {
						qsData = metamodelData.getQueryStruct(true);
					}
					dmc = new DataMakerComponent(engine, qsData); 
					dmCompVec.add(dmc);
				}
				curComponent = COMP + component;
				if (dmc != null) {
					dmc.setId(curComponent);
				}
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
	private void addPreTrans(DataMakerComponent dmc, IDatabaseEngine makeupEng, Object preTrans, String compId){
		logger.info("adding pre trans :::: " + preTrans);
		Map<String, Object> props = getProperties(preTrans+"", makeupEng);
		String type = props.get(ISEMOSSTransformation.TYPE) + "";
		logger.info("TRANS TYPE IS " + Utility.cleanLogString(type));
		ISEMOSSTransformation trans = Utility.getTransformation(this.mainEngine, type);
		logger.info("pre trans properties :::: " + Utility.cleanLogString(props.toString()));
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
	private void addPostTrans(DataMakerComponent dmc, IDatabaseEngine makeupEng, Object postTrans, String compId){
		logger.info("adding post trans :::: " + postTrans);
		Map<String, Object> props = getProperties(postTrans+"", makeupEng);
		String type = props.get(ISEMOSSTransformation.TYPE) + "";
		logger.info("TRANS TYPE IS " + Utility.cleanLogString(type));
		ISEMOSSTransformation trans = Utility.getTransformation(this.mainEngine, type);
		logger.info("post trans properties :::: " + Utility.cleanLogString(props.toString()));
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
	private void addAction(DataMakerComponent dmc, IDatabaseEngine makeupEng, Object action, String compId){
		logger.info("adding action :::: " + action);
		Map<String, Object> props = getProperties(action+"", makeupEng);
		String type = props.get(ISEMOSSAction.TYPE) + "";
		logger.info("TRANS TYPE IS " + type);
		ISEMOSSAction actionObj = Utility.getAction(this.mainEngine, type);
		logger.info("action properties :::: " + Utility.cleanLogString(props.toString()));
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
	private Map<String, Object> getProperties(String uri, IDatabaseEngine makeupEng){
		String propQuery = "SELECT ?Value WHERE { BIND(<" + 
				uri + 
				"> AS ?obj) {?obj <http://semoss.org/ontologies/Relation/Contains/propMap> ?Value}}";
		
		logger.info("Running query to get properties: " + Utility.cleanLogString(propQuery));
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(makeupEng, propQuery);
		Map<String, Object> retMap = new HashMap<>();
		if(wrap.hasNext()){ // there should only be one prop map associated with each transformation or action
			ISelectStatement ss = wrap.next();
			String jsonPropMap = ss.getVar("Value") + "";
			logger.info(Utility.cleanLogString(jsonPropMap));
			retMap = gson.fromJson(jsonPropMap, Map.class);
		}
		if(wrap.hasNext()){
			logger.error("More than one prop map has shown up for uri ::::: " + Utility.cleanLogString(uri));
			logger.error("Need to find reason why/how it was stored this way...");
		}
		return retMap;
	}
	
	/**
	 * Get the data maker object from the dataMakerString
	 * If cannot find specified dataMaker, get the playSheet and see if is supposed to be the data maker
	 * @return
	 */
	@Override
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
			this.dataMaker.setUserId(this.getUserId());
		}
		return this.dataMaker;
	}
	
	/**
	 * Setter for the data maker
	 * @param dataMaker
	 */
	@Override
	public void setDataMaker(IDataMaker dataMaker) {
		this.dataMaker = dataMaker;
		if(this.dataMaker.getUserId() == null) {
			this.dataMaker.setUserId(this.getUserId());
		}
		this.dataMakerName = dataMaker.getDataMakerName();
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
			this.dmComponents = new Vector<>();
		}
		return this.dmComponents;
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
	 * Primarily used when data table align is coming from rdbms insight
	 * @param dataTableAlignJSON
	 */
	public void setDataTableAlign(String dataTableAlignJSON) {
		if(dataTableAlignJSON != null && !dataTableAlignJSON.isEmpty()){
			logger.info("Setting json dataTableAlign " + Utility.cleanLogString(dataTableAlignJSON));
			this.dataTableAlign = gson.fromJson(dataTableAlignJSON, Map.class);
		} else {
			logger.info("data table align is empty");
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
			this.playSheet = Utility.getPlaySheet(this.mainEngine, this.layout);
			if(playSheet != null){
				// to keep playsheet ID and insight ID in sync
				this.playSheet.setQuestionID(this.insightId);
				this.playSheet.setDataMaker(getDataMaker());
			}
			else {
				logger.error("Broken insight... cannot get playsheet :: " + Utility.cleanLogString(this.layout));
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
			this.playSheet.setQuestionID(insightId);
		}
	}

	/**
	 * Getter for the data maker name
	 * @return
	 */
	@Override
	public String getDataMakerName() {
		if(this.dataMaker == null) {
			return this.dataMakerName;
		} else {
			return this.dataMaker.getClass().getSimpleName();
		}
	}
	
	public String getOutput() {
		return this.layout;
	}
	
	public void setOutput(String output) {
		this.layout = output;
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
		getDataMaker().processDataMakerComponent(componentCopy);
		getDataMakerComponents().add(component);
			
	}
	
	/**
	 * Process a list of post transformation on the last data maker component stored in cmComponents
	 * @param postTrans					The list of post transformation to run
	 * @param dataMaker					Additional dataMakers if required by the transformation
	 */
	public void processPostTransformation(List<ISEMOSSTransformation> postTrans, IDataMaker... dataMaker) throws RuntimeException {
		DataMakerComponent dmc = getLastComponent();
		
		List<ISEMOSSTransformation> postTransCopy = new Vector<ISEMOSSTransformation>(postTrans.size());
		for(ISEMOSSTransformation trans : postTrans) {
			postTransCopy.add(trans.copy());
		}
		getDataMaker().processPostTransformations(dmc, postTransCopy, dataMaker);
		int lastPostTrans = dmc.getPostTrans().size() - 1;
		for(int i = 0; i < postTrans.size(); i++) {
			postTrans.get(i).setId(dmc.getId() + ":" + POST_TRANS + (++lastPostTrans));
			dmc.addPostTrans(postTrans.get(i));
		}
	}
	
	public DataMakerComponent getLastComponent() {
		if(getDataMakerComponents().isEmpty()){
			DataMakerComponent empty = new DataMakerComponent(Constants.LOCAL_MASTER_DB, Constants.EMPTY);
			this.dmComponents.add(empty);
		}
		
		return getDataMakerComponents().get(this.dmComponents.size() - 1);	
	}
	
//	/**
//	 * Process a list of actions on the last data maker component stored in cmComponents
//	 * @param postTrans					The list of actions to run
//	 * @param dataMaker					Additional dataMakers if required by the actions
//	 */
//	public List<Object> processActions(List<ISEMOSSAction> actions, IDataMaker... dataMaker) throws RuntimeException {
//		DataMakerComponent dmc = getDataMakerComponents().get(this.dmComponents.size() - 1);
//		
//		List<ISEMOSSAction> actionsCopy = new Vector<ISEMOSSAction>(actions.size());
//		for(ISEMOSSAction action : actions) {
//			actionsCopy.add(action.copy());
//		}
//		
//		List<Object> actionResults = getDataMaker().processActions(dmc, actionsCopy, dataMaker);
//		//TODO: extrapolate in datamakercomponent to take in a list
//		int lastAction = dmc.getActions().size() - 1;
//		for(int i = 0; i < actions.size(); i++) {
//			actions.get(i).setId(dmc.getId() + ":" + ACTION + (++lastAction));
//			getDataMakerComponents().get(this.dmComponents.size() - 1).addAction(actions.get(i));
//		}
//		
//		return actionResults;
//	}
	
	/**
	 * Undo a set of processes (components/transformations/actions) based on the IDs
	 * @param processes
	 */
	public void undoProcesses(List<String> processes){
		// traverse backwards and undo everything in the list
		List<DataMakerComponent> dmcListToRemove = new ArrayList<>();
		for(int i = dmComponents.size()-1; i >= 0; i--) {
			DataMakerComponent dmc = dmComponents.get(i);
			List<ISEMOSSAction> actions = dmc.getActions();
			undoActions(actions, processes);
			undoTransformations(dmc.getPostTrans(), processes);
			boolean joinUndone = undoTransformations(dmc.getPreTrans(), processes);
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
		logger.info("Undoing transformations :  " + processes);
		List<Integer> indicesToRemove = new ArrayList<>();
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
			trans.remove(indexToRemove.intValue());
			if(transToUndo instanceof JoinTransformation){
				removedJoin = true;
			}
		}
		logger.info("Undo transformations complete. Join transformation undone : " + removedJoin);
		return removedJoin;
	}
	
	/**
	 * Get the information after running an insight to send to FE for viewing result
	 * @return
	 */
	public Map<String, Object> getWebData() {
		Map<String, Object> retHash = new HashMap<>();
		retHash.put("insightID", this.insightId);
		retHash.put("layout", this.layout);
		retHash.put("title", this.insightName);
		retHash.put("dataMakerName", this.dataMakerName);
		if(dataTableAlign != null){ // some playsheets don't require data table align, like grid play sheet. Should probably change this so they all have data table align (like if i want to change the order of my columns)
			retHash.put("dataTableAlign", dataTableAlign);
//			for(String label : dataTableAlign.keySet()) {
//				selectors.add(dataTableAlign.get(label));
//			}
		}
		// TODO: how do i get this outside of here? we need to return incremental stores during traversing but not when recreating insight
		IDataMaker dm = getDataMaker();
		if(dm instanceof GraphDataModel) {
			((GraphDataModel) getDataMaker()).setOverlay(false);
		}
		// this will return the data from the dataMaker
		// if has a do-method, dataMaker is the playsheet and can use those methods but can also use a datamaker
		// i.e.gdm would be used for the method below (to get the edges/nodes)
		
		// right now, assuming only one action is present
		// TODO: should we update the interface to always return a map
		// currently all actions return a map
//		if(dm.getActionOutput() != null && !dm.getActionOutput().isEmpty()) {
//			retHash.putAll( (Map) getDataMaker().getActionOutput().get(0));
//		} else 
		if (this.layout.equals("Graph") || this.layout.equals("VivaGraph")) { //TODO: Remove hardcoded layout values
			if(dm instanceof TinkerFrame) {
				retHash.putAll(((TinkerFrame)getDataMaker()).getGraphOutput());
			} 
//			else if(dm instanceof H2Frame) {
//				TinkerFrame tframe = TableDataFrameFactory.convertToTinkerFrameForGraph((H2Frame)dm);
//				retHash.putAll(tframe.getGraphOutput());
//			} 
			else {
				// this is for insights which are gdm
				retHash.putAll(dm.getDataMakerOutput());
			}
		} else {
//			if(dm instanceof ITableDataFrame) {
//				retHash.putAll(dm.getDataMakerOutput());
//			} 
//			else if(dm instanceof Dashboard) { 
//				Dashboard dash = (Dashboard)dm;
//				Gson gson = new Gson();
//				retHash.put("config", dash.getConfig());
////				dash.setInsightID(insightID);
//				retHash.putAll(dm.getDataMakerOutput());
//			}
//			else {
				retHash.putAll(dm.getDataMakerOutput());
//			}
		}
		String uiOptions = getUiOptions();
		if(!uiOptions.isEmpty()) {
			retHash.put("uiOptions", uiOptions);
		}
		return retHash;
	}
	
	public Map<String, Object> getOutputWebData() {
		Map<String, Object> retHash = new HashMap<>();
		retHash.put("insightID", this.insightId);
		retHash.put("layout", this.layout);
		retHash.put("title", this.insightName);
		retHash.put("dataMakerName", this.dataMakerName);
		if(dataTableAlign != null){ // some playsheets don't require data table align, like grid play sheet. Should probably change this so they all have data table align (like if i want to change the order of my columns)
			retHash.put("dataTableAlign", dataTableAlign);
		}
		// TODO: how do i get this outside of here? we need to return incremental stores during traversing but not when recreating insight
		IDataMaker dm = getDataMaker();
		if(dm instanceof GraphDataModel) {
			((GraphDataModel) getDataMaker()).setOverlay(false);
		}
		String uiOptions = getUiOptions();
		if(!uiOptions.isEmpty()) {
			retHash.put("uiOptions", uiOptions);
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
	
	public String getUiOptions() {
		if(this.uiOptions == null){
			this.uiOptions = "";
			String rdbmsId = getRdbmsId();
			if(rdbmsId != null && !rdbmsId.isEmpty()) {
				String query = "SELECT UI_DATA FROM UI WHERE QUESTION_ID_FK='" + rdbmsId + "'";
				IDatabaseEngine insightDb = this.mainProject.getInsightDatabase();
				ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightDb, query);
				String[] names = wrapper.getVariables();
				while(wrapper.hasNext()) {
					ISelectStatement ss = wrapper.next();
					JdbcClob obj = (JdbcClob) ss.getRawVar(names[0]);
					
					InputStream insightDefinition = null;
					try {
						insightDefinition = obj.getAsciiStream();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
					
					 try {
						uiOptions = IOUtils.toString(insightDefinition);
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					} 
				}
			}
		}
		
		return uiOptions;
	}
	
	public void setUiOptions(String uiOptions){
		this.uiOptions = uiOptions;
	}

	public String getEngineId() {
		if(this.mainEngine != null) {
			return this.mainEngine.getEngineId();
		} else {
			return null;
		}
	}

	public String getProjectName() {
		if(this.mainEngine != null) {
			return this.mainEngine.getEngineName();
		} else {
			return null;
		}
	}
	
	public void setMainEngine(IDatabaseEngine engine) {
		this.mainEngine = engine;
	} 
	
	public void recalcDerivedColumns(){
		// iterate from the first DMC to find where the first derived column is
		List<DataMakerComponent> dmcList = getDataMakerComponents();
		Iterator<DataMakerComponent> dmcIt = dmcList.iterator();
		int firstComp = 0;
		String firstTransId = null;
		for (; firstComp < dmcList.size() && firstTransId == null; firstComp ++){
			DataMakerComponent dmc = dmcIt.next();
			for(ISEMOSSTransformation trans : dmc.getPostTrans()){
				if(!(trans instanceof FilterTransformation) && !(trans instanceof JoinTransformation) && !(trans instanceof PKQLTransformation)){
					firstTransId = trans.getId();
					firstComp--;
					break;
				}
			}
		}
		if(firstTransId == null){ // no derived columns were found! :)
			return;
		}
		
		// now that i've found where my first derived column is, i need to undo everything that comes after that in correct order
		boolean hitFirst = false;
		List<ISEMOSSTransformation> transToRedo = new Vector<>();
		for(int dmcIdx = dmcList.size() - 1; dmcIdx >= firstComp && !hitFirst; dmcIdx -- ){
			DataMakerComponent dmc = dmcList.get(dmcIdx);
			List<ISEMOSSTransformation> postList = dmc.getPostTrans();
			for(int transIdx = postList.size() - 1; transIdx >= 0 && !hitFirst; transIdx -- ){
				ISEMOSSTransformation trans = postList.get(transIdx);
				if(!(trans instanceof JoinTransformation) && !(trans instanceof FilterTransformation) && !(trans instanceof PKQLTransformation)){
					transToRedo.add(trans);
					String id = trans.getId();
					this.undoProcesses(Arrays.asList(new String[]{id}));
					postList.remove(trans);
					if(id.equals(firstTransId)){
						hitFirst = true;
					}
				}
			}
			List<ISEMOSSTransformation> preList = dmc.getPreTrans();
			for(int transIdx = preList.size() - 1; transIdx >= 0 && !hitFirst; transIdx -- ){
				ISEMOSSTransformation trans = preList.get(transIdx);
				if(!(trans instanceof JoinTransformation) && !(trans instanceof FilterTransformation)){
					transToRedo.add(trans);
					String id = trans.getId();
					this.undoProcesses(Arrays.asList(new String[]{id}));
					preList.remove(trans);
					if(id.equals(firstTransId)){
						hitFirst = true;
					}
				}
			}
		}
		
		this.processPostTransformation(transToRedo, dataMaker);
	}
	
	@Override
	public boolean equals(Object insight) {
		if(insight instanceof Insight && insight != null) {
			return this.insightId.equals(((Insight)insight).getInsightId());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.insightId.hashCode();
	}
}
