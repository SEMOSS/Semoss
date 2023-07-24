package prerna.ui.components.playsheets.datamakers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.QueryStruct;
import prerna.ds.util.QueryStructConverter;
import prerna.engine.api.IDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.Utility;

public class DataMakerComponent {

	private static final Logger LOGGER = LogManager.getLogger(DataMakerComponent.class.getName());
	private String id;
	private String query;
	private String dataFrameLocation;
	private QueryStruct qs; // this is now a formal object! :)
	private String engineName;
	private IDatabase engine;
	private List<ISEMOSSTransformation> preTrans = new ArrayList<ISEMOSSTransformation>();
	private List<ISEMOSSTransformation> postTrans = new ArrayList<ISEMOSSTransformation>();
	private List<ISEMOSSAction> actions = new ArrayList<ISEMOSSAction>();
	private boolean isProcessed = false;
	
	private String rdbmsID;
	
	/**
	 * Constructor for the data maker component object
	 * @param databasese					The name of the engine
	 * @param query						The query corresponding to the data maker component to be run on the engine
	 */
	public DataMakerComponent(String engineName, String query){
		this.engineName = engineName;
		this.query = query;
	}
	
	/**
	 * Constructor for the data maker component object
	 * @param engine					The engine object
	 * @param query						The query corresponding to the data maker component to be run on the engine
	 */
	public DataMakerComponent(IDatabase engine, String query){
		this.engine = engine;
		this.engineName = engine.getEngineId();
		this.query = query;
	}

	/**
	 * Constructor for the data maker component object
	 * @paradatabasebase					The name of the engine
	 * @param metamodelData				The map to build the query based on the OWL
	 */
	public DataMakerComponent(String engineName, QueryStruct qs){
		this.engineName = engineName;
		this.qs = qs;
	}
	
	/**
	 * Constructor for the data maker component object
	 * @param engine					The engine object
	 * @param metamodelData				The map to build the query based on the OWL
	 */
	public DataMakerComponent(IDatabase engine, QueryStruct qs){
		this.engine = engine;
		this.engineName = engine.getEngineId();
		this.qs = qs;
	}
	
	/**
	 * Constructor for the data maker component object
	 * @param dataFrameLocation			The string location for the serialized data frame
	 */
	public DataMakerComponent(String dataFrameLocation) {
		this.dataFrameLocation = dataFrameLocation;
	}
	
	/**
	 * Setter for the query on the component
	 * @param query
	 */
	public void setQuery(String query){
		this.query = query;
	}

	/**
	 * Getter for the query on the component
	 * @return
	 */
	public String getQuery() {
		return query;
	}
	
	/**
	 * Setter for the metamodel data of the component
	 * @param builderData The query builder data needed to build the query for this component
	 */
	public void setQueryStruct(QueryStruct qs){
		this.qs = qs;
	}

	/**
	 * Getter for the metamodel data of the component
	 * @return
	 */
	public QueryStruct getQueryStruct() {
		return qs;
	}
	
	/**
	 * Getter for the engine of the component
	 * @return
	 */
	public IDatabase getEngine() {
		if(this.engine == null) {
			this.engine = Utility.getDatabase(this.engineName);
		}
		return this.engine;
	}
	
	public String getEngineName() {
		return this.engineName;
	}
	
	public String getDataFrameLocation() {
		return dataFrameLocation;
	}
	
	/**
	 * Getter for the list of preTransformations on the component
	 * @return
	 */
	public List<ISEMOSSTransformation> getPreTrans() {
		return preTrans;
	}
	
	/**
	 * Setter for the list of preTransformation on the component
	 * @param preTrans
	 */
	public void setPreTrans(List<ISEMOSSTransformation> preTrans) {
		this.preTrans = preTrans;
	}
	
	/**
	 * Append a preTransformation onto the component
	 * @param preTran
	 */
	public void addPreTrans(ISEMOSSTransformation preTran){
		preTran.setTransformationType(true);
		this.preTrans.add(preTran);
	}
	
	/**
	 * Getter for the list of postTransformations on the component
	 * @return
	 */
	public List<ISEMOSSTransformation> getPostTrans() {
		return postTrans;
	}
	
	/**
	 * Setter for the list of postTransformations on the component
	 * @param postTrans
	 */
	public void setPostTrans(List<ISEMOSSTransformation> postTrans) {
		this.postTrans = postTrans;
	}
	
	/**
	 * Append a postTransformation onto the component
	 * @param postTran
	 */
	public void addPostTrans(ISEMOSSTransformation postTran){
		postTran.setTransformationType(false);
		this.postTrans.add(postTran);
	}
	
	/**
	 * Append a postTransformation onto the component
	 * @param postTran
	 */
	public void addPostTrans(ISEMOSSTransformation postTran, int index){
		postTran.setTransformationType(false);
		this.postTrans.add(index, postTran);
	}

	/**
	 * Append an action onto the component
	 * @param action
	 */
	public void addAction(ISEMOSSAction action) {
		this.actions.add(action);
	}
	
	/**
	 * Getter for the list of actions on the component
	 * @return
	 */
	public List<ISEMOSSAction> getActions() {
		return this.actions;
	}

	/**
	 * Setter for the user selected parameters on a component
	 * Finds the specified Filter PreTransformation and appends the "VALUES_KEY" value
	 * @param paramHash
	 * @param filterNum
	 */
	public void setParamHash(Map<String, List<Object>> paramHash, int filterNum) {
		// clean up the params if the engine for the component is an RDBMS engine
		if(getEngine() instanceof RDBMSNativeEngine){
			paramHash = Utility.cleanParamsForRDBMS(paramHash);
		}
		// get the transformation based on the filter number passed in
		// filter number passed in originated from Insight object which gets the information from
		// parameter table in rdbms insights
		ISEMOSSTransformation trans = preTrans.get(filterNum);
		if(trans instanceof FilterTransformation) {
			Map<String, Object> props = trans.getProperties();
			props.put(FilterTransformation.VALUES_KEY, paramHash.get(props.get(FilterTransformation.COLUMN_HEADER_KEY)));
		} else {
			throw new IllegalArgumentException("Filter number for parameter is invalid");
		}
	}
	
	/**
	 * Returns the effective query for the component
	 * @return
	 */
	public String fillQuery() {
		String retQuery = this.query;
		if(retQuery == null) {
			retQuery = buildQuery();
		}
		return retQuery;
	}
	
	/**
	 * Generates the query from the metamodel data
	 * @return
	 */
	private String buildQuery() {
		SelectQueryStruct newQs = QueryStructConverter.convertOldQueryStruct(qs);
		IQueryInterpreter builder = getEngine().getQueryInterpreter();
		builder.setQueryStruct(newQs);
		return builder.composeQuery();
	}
	
	/**
	 * Setter for the id of the component
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * Getter for hte id of the component
	 * @return
	 */
	public String getId() {
		return this.id;
	}
	
	/**
	 * Return a copy of this DataMakerComponent that can be saved by the insight
	 * @return
	 */
	public DataMakerComponent copy() {
		DataMakerComponent copy = new DataMakerComponent(getEngine(), query);
		copy.id = this.id;
		copy.isProcessed = this.isProcessed;
		
		//use gson to make a copy of metamodel data
		if(qs != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String metamodelCopy = gson.toJson(qs);
			QueryStruct newMetaModel = gson.fromJson(metamodelCopy, QueryStruct.class);
			copy.setQueryStruct(newMetaModel);
		}
		
		for(ISEMOSSTransformation preTrans : this.preTrans) {
			copy.preTrans.add(preTrans.copy());
		}
		
		for(ISEMOSSTransformation postTrans : this.postTrans) {
			copy.postTrans.add(postTrans.copy());
		}
		
		for(ISEMOSSAction action : this.actions) {
			copy.actions.add(action.copy());
		}
		
		return copy;
	}
	
	public boolean isProcessed() {
		return isProcessed;
	}

	public void setProcessed(boolean isProcessed) {
		this.isProcessed = isProcessed;
	}
	
	public String getRdbmsId() {
		return this.rdbmsID;
	}
	
	public void setRdbmsId(String id) {
		this.rdbmsID = id;
	}
}
