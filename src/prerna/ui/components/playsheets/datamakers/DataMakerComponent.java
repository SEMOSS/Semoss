package prerna.ui.components.playsheets.datamakers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DataMakerComponent {

	private static final Logger LOGGER = LogManager.getLogger(DataMakerComponent.class.getName());
	private String id;
	private String query;
	private Map<String, Object> metamodelData; // this should be made into a formal object... its gone on too long pasing around that random hashtable
	private IEngine engine;
	private List<ISEMOSSTransformation> preTrans = new ArrayList<ISEMOSSTransformation>();
	private List<ISEMOSSTransformation> postTrans = new ArrayList<ISEMOSSTransformation>();
	private List<ISEMOSSAction> actions = new ArrayList<ISEMOSSAction>();
	
	public DataMakerComponent(String engine, String query){
		IEngine theEngine = (IEngine) DIHelper.getInstance().getLocalProp(engine);
		this.engine = theEngine;
		this.query = query;
	}
	
	public DataMakerComponent(IEngine engine, String query){
		this.engine = engine;
		this.query = query;
	}

	public DataMakerComponent(String engine, Map<String, Object> metamodelData){
		IEngine theEngine = (IEngine) DIHelper.getInstance().getLocalProp(engine);
		this.engine = theEngine;
		this.metamodelData = metamodelData;
	}
	
	public DataMakerComponent(IEngine engine, Map<String, Object> metamodelData){
		this.engine = engine;
		this.metamodelData = metamodelData;
	}
	
	public void setQuery(String query){
		this.query = query;
	}

	public String getQuery() {
		return query;
	}
	
	public void setMetamodelData(Map<String, Object> metamodelData){
		this.metamodelData = metamodelData;
	}

	public Map<String, Object> getMetamodelData() {
		return metamodelData;
	}
	
	public IEngine getEngine() {
		return engine;
	}
	
	public List<ISEMOSSTransformation> getPreTrans() {
		return preTrans;
	}
	
	public void setPreTrans(List<ISEMOSSTransformation> preTrans) {
		this.preTrans = preTrans;
	}
	
	public void addPreTrans(ISEMOSSTransformation preTran){
		preTran.setTransformationType(true);
		this.preTrans.add(preTran);
	}
	
	public List<ISEMOSSTransformation> getPostTrans() {
		return postTrans;
	}
	
	public void setPostTrans(List<ISEMOSSTransformation> postTrans) {
		this.postTrans = postTrans;
	}
	
	public void addPostTrans(ISEMOSSTransformation postTran){
		postTran.setTransformationType(false);
		this.postTrans.add(postTran);
	}

	public void addAction(ISEMOSSAction action) {
		this.actions.add(action);
	}
	
	public List<ISEMOSSAction> getActions() {
		return this.actions;
	}

	public void setParamHash(Map<String, List<Object>> paramHash, int filterNum) {
		if(this.engine instanceof RDBMSNativeEngine){
			paramHash = Utility.cleanParamsForRDBMS(paramHash);
		}
		ISEMOSSTransformation trans = preTrans.get(filterNum);
		if(trans instanceof FilterTransformation) {
			Map<String, Object> props = trans.getProperties();
			props.put(FilterTransformation.VALUES_KEY, paramHash.get(props.get(FilterTransformation.COLUMN_HEADER_KEY)));
		} else {
			throw new IllegalArgumentException("Filter number for parameter is invalid");
		}
	}
	
	public String fillQuery() {
		String retQuery = this.query;
		if(retQuery == null) {
			retQuery = buildQuery();
		}
		return retQuery;
	}
	
	public String buildQuery() {
		IQueryBuilder builder = engine.getQueryBuilder();
		builder.setJSONDataHash(this.metamodelData);
		builder.buildQuery();
		return builder.getQuery();
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getId() {
		return this.id;
	}
	
}
