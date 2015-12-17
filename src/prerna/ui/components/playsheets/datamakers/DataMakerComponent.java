package prerna.ui.components.playsheets.datamakers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.QueryBuilderData;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class DataMakerComponent {

	private static final Logger LOGGER = LogManager.getLogger(DataMakerComponent.class.getName());
	private String id;
	private String query;
	private QueryBuilderData builderData; // this is now a formal object! :)
	private IEngine engine;
	private List<ISEMOSSTransformation> preTrans = new ArrayList<ISEMOSSTransformation>();
	private List<ISEMOSSTransformation> postTrans = new ArrayList<ISEMOSSTransformation>();
	private List<ISEMOSSAction> actions = new ArrayList<ISEMOSSAction>();
	
	/**
	 * Constructor for the data maker component object
	 * @param engine					The name of the engine
	 * @param query						The query corresponding to the data maker component to be run on the engine
	 */
	public DataMakerComponent(String engine, String query){
		IEngine theEngine = (IEngine) DIHelper.getInstance().getLocalProp(engine);
		this.engine = theEngine;
		this.query = query;
	}
	
	/**
	 * Constructor for the data maker component object
	 * @param engine					The engine object
	 * @param query						The query corresponding to the data maker component to be run on the engine
	 */
	public DataMakerComponent(IEngine engine, String query){
		this.engine = engine;
		this.query = query;
	}

	/**
	 * Constructor for the data maker component object
	 * @param engine					The name of the engine
	 * @param metamodelData				The map to build the query based on the OWL
	 */
	public DataMakerComponent(String engine, QueryBuilderData builderData){
		IEngine theEngine = (IEngine) DIHelper.getInstance().getLocalProp(engine);
		this.engine = theEngine;
		this.builderData = builderData;
	}
	
	/**
	 * Constructor for the data maker component object
	 * @param engine					The engine object
	 * @param metamodelData				The map to build the query based on the OWL
	 */
	public DataMakerComponent(IEngine engine, QueryBuilderData builderData){
		this.engine = engine;
		this.builderData = builderData;
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
	public void setBuilderData(QueryBuilderData builderData){
		this.builderData = builderData;
	}

	/**
	 * Getter for the metamodel data of the component
	 * @return
	 */
	public QueryBuilderData getBuilderData() {
		return builderData;
	}
	
	/**
	 * Getter for the engine of the component
	 * @return
	 */
	public IEngine getEngine() {
		return engine;
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
		if(this.engine instanceof RDBMSNativeEngine){
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
		IQueryBuilder builder = engine.getQueryBuilder();
		builder.setBuilderData(builderData);;
		builder.buildQuery();
		return builder.getQuery();
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
		DataMakerComponent copy = new DataMakerComponent(engine, query);
		
		//use gson to make a copy of metamodel data
		if(builderData != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String metamodelCopy = gson.toJson(builderData);
			QueryBuilderData newMetaModel = gson.fromJson(metamodelCopy, QueryBuilderData.class);
			copy.setBuilderData(newMetaModel);
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
	
}
