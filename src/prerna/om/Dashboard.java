package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.AbstractTableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.h2.H2Joiner;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

public class Dashboard implements IDataMaker {

	private static final Logger LOGGER = LogManager.getLogger(Dashboard.class.getName());
	private String insightID;
	private String userID;
	private H2Joiner joiner;
	
	//will need this when same insight can be on two different dashboards
	private Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = new HashMap<>();
	
	// viewTable -> List of Insights
	private Map<String, List<Insight>> insightMap = new HashMap<>();
	
	// insight id -> frame id
	private Map<String, String[]> insight2frameMap = new HashMap<>();
	
	//keeps track of config structure
	Object config = new HashMap<>();
	
	//need to store the output for each insight and return later to capture pkql calls
	private Map<String, Map<String, Object>> insightOutputMap = new HashMap<>();
	
	public Dashboard() {
		joiner = new H2Joiner(this);
	}
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		
		processPreTransformations(component, component.getPreTrans());
		processPostTransformations(component, component.getPostTrans());
		processActions(component, component.getActions());
			
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {

	}
	
	@Override
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		LOGGER.info("We are processing " + transforms.size() + " post transformations");
		// if other data frames present, create new array with this at position 0
		IDataMaker[] extendedArray = new IDataMaker[]{this};
		if(dataFrame.length > 0) {
			extendedArray = new IDataMaker[dataFrame.length + 1];
			extendedArray[0] =  this;
			for(int i = 0; i < dataFrame.length; i++) {
				extendedArray[i+1] = dataFrame[i];
			}
		}
		for(ISEMOSSTransformation transform : transforms) {
			//get the id from the pkql transformation and run in on that join
			transform.setDataMakers(extendedArray);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}


	@Override
	/**
	 * Return the data maker output for each attached insight
	 * 
	 * TODO : Take into account selectors
	 */
	public Map<String, Object> getDataMakerOutput(String... selectors) {
		
		Map<String, Object> returnHash = new HashMap<>();
		List insightList = new ArrayList();
		
		for(String viewTableKey : insightMap.keySet()) {
			
			List<Insight> insights = getInsights(viewTableKey);
			List<String> insightIDs = new ArrayList<>();
			for(Insight insight : insights) {
				insightIDs.add(insight.getInsightID());
			}
			
			for(Insight insight : insights) {
				Map<String, Object> nextInsightMap = new HashMap<>();
				
				nextInsightMap.put("insightID", insight.getInsightID());
				nextInsightMap.put("engine", insight.getEngineName());
				nextInsightMap.put("questionID", insight.getRdbmsId());
				
				String[] ids = this.insight2frameMap.get(insight.getInsightID());
				if(ids != null) {
					nextInsightMap.put("widgetID", ids[0]);
					nextInsightMap.put("panelID", ids[1]);
				} else {
					nextInsightMap.put("widgetID", "");
					nextInsightMap.put("panelID", "");
				}
				
				List<String> joinedInsights = new ArrayList<>();
				
				joinedInsights.addAll(insightIDs);
				joinedInsights.remove(insight.getInsightID());
				nextInsightMap.put("joinedInsights", joinedInsights);
				
				//instead of doing this...have data.open save the output to the insight/dashboard
				//grab that data from the insight/dashboard...then delete
				//this doesn't have sufficient pkql data
				Map<String, Object> insightOutput;
				if(this.insightOutputMap.containsKey(insight.getInsightID())) {
					insightOutput = insightOutputMap.get(insight.getInsightID());
				} else {
					insightOutput = insight.getWebData();
				}
				nextInsightMap.putAll(insightOutput);
				insightList.add(nextInsightMap);
			}
		}
		returnHash.put("Dashboard", insightList);
		this.insightOutputMap.clear();
		return returnHash;
	}
	
	public List<Object> getInsightData() {		
		return null;
	}

	@Override
	public List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		return null;
	}

	@Override
	public List<Object> getActionOutput() {
		return null;
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<>();
		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
		reactorNames.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
		reactorNames.put(PKQLReactor.VAR.toString(), "prerna.sablecc.VarReactor");
		reactorNames.put(PKQLReactor.INPUT.toString(), "prerna.sablecc.InputReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor");
		reactorNames.put(PKQLEnum.DASHBOARD_ADD, "prerna.sablecc.DashboardAddReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.MATH_PARAM, "prerna.sablecc.MathParamReactor");
		reactorNames.put(PKQLEnum.CLEAR_DATA, "prerna.sablecc.DashboardClearDataReactor");
		return reactorNames;
	}

	@Override
	public void updateDataId() {
		
	}

	@Override
	public int getDataId() {
		return 0;
	}

	@Override
	public void setUserId(String userId) {
		this.userID = userId;
	}

	@Override
	public String getUserId() {
		return this.userID;
	}

	@Override
	public String getDataMakerName() {
		return this.getClass().getSimpleName();
	}
	
	private void addInsight(String viewTable, Insight insight) {
		if(this.insightMap.containsKey(viewTable)) {
			List<Insight> insights = this.insightMap.get(viewTable);
			if(!insights.contains(insight)){
				insights.add(insight);
			}
		} else {
			List<Insight> list = new ArrayList<>();
			list.add(insight);
			this.insightMap.put(viewTable, list);
		}
	}
	
	public void removeInsight(String viewTable, Insight insight) {
		if(this.insightMap.containsKey(viewTable)) {
			List<Insight> insights = this.insightMap.get(viewTable);
			insights.remove(insight);
			if(insights.size() == 0) {
				this.insightMap.remove(viewTable);
			}
		}
	}
	
	public List<Insight> getInsights() {
		List<Insight> insightList = new ArrayList<>();
		for(String key : insightMap.keySet()) {
			insightList.addAll(insightMap.get(key));
		}
		return insightList;
	}
	
	public List<Insight> getInsights(String tableName) {
		return insightMap.get(tableName);
	}
	
	public void setInsightID(String insightID) {
		this.insightID = insightID;
	}

	public String getInsightID() {
		return this.insightID;
	}
	
	@Override
	public void resetDataId() {
		
	}
	
	public void setConfig(Object config) {
		this.config = config;
	}
	
	public Object getConfig() {
		return this.config;
	}
	
	public void setInsightOutput(String id, Map<String, Object> map) {
		this.insightOutputMap.put(id, map);
	}
	/************************************* JOINING LOGIC **************************************/
	
	/**
	 * 
	 * @param insights
	 * @param joinColumns
	 * 
	 * first doing the case where we have all unjoined insights
	 */
	public void joinInsights(List<Insight> insights, List<List<String>> joinColumns) {
		H2Frame[] frames = new H2Frame[insights.size()];
		for(int i = 0; i < insights.size(); i++) {
			Insight insight = insights.get(i);
			IDataMaker dm = insight.getDataMaker();
			
			if(dm instanceof H2Frame) {
				frames[i] = (H2Frame)dm;
			} else {
				throw new IllegalArgumentException("Cannot join Insight "+insight.getInsightID()+": Needs to be type 'GRID' to join");
			}
		}
		
		try {
			String table = joiner.joinFrames(joinColumns, frames);
			Insight parentInsight = InsightStore.getInstance().get(this.insightID);
			for(Insight insight : insights) {
				
				//add the insight to the view
				addInsight(table, insight);
				insight.setParentInsight(parentInsight);
				
				//remove the insight as a stand alone insight from the dashboard
				removeInsight(insight.getInsightID(), insight);
			}
			
			insights.get(0).getDataMaker().updateDataId();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param insights
	 * 
	 * Method to add insights
	 */
	public void addInsights(List<Insight> insights) {
		Insight parentInsight = InsightStore.getInstance().get(this.insightID);
		for(Insight insight : insights) {
			addInsight(insight.getInsightID(), insight);
			insight.setParentInsight(parentInsight);
		}
	}
	
	public void unJoinInsights(Insight... insights) {

	}
	
	public List<String> getSaveRecipe(String recipe) {
		
		String[] recipeArr = recipe.split(System.getProperty("line.separator"));
		List<String> curRecipe = new ArrayList<>();
		for(String recipePkql : recipeArr) {
			curRecipe.add(recipePkql);
		}
		
		List<String> saveRecipe = new ArrayList<>();
		Map<String, String> varHash = new HashMap<>();
		
		//create all the pkqls that open insights and assign them to variables
		int i = 1; 
		List<Insight> insights = getInsights();//(List<Insight>)joinData[0];
		List<String> varNames = new ArrayList<>();
		for(Insight insight : insights) {
			String nextVar = "insightVar"+i;
			String nextPkql = createVarPkql(nextVar, insight.getEngineName(), insight.getRdbmsId());
			varHash.put(nextVar, insight.getInsightID());
			varNames.add(nextVar);
			i++;
			saveRecipe.add(nextPkql);
		}
		
		//add the joins
		for(String joinPkql : curRecipe) {
			if(joinPkql.startsWith("data.join") || joinPkql.startsWith("dashboard.add")) {
				String newJoinPkql = createJoinPkql(varHash, joinPkql);
				if(newJoinPkql != null) {
					saveRecipe.add(newJoinPkql);
				} else {
					
				}
			} else if(!joinPkql.isEmpty()) {
				saveRecipe.add(joinPkql);
			}
		}
		
		return saveRecipe;
	}
	
	private String createVarPkql(String varName, String engine, String id) {
		varName = "v:"+varName;
		String varPkql = varName+" = data.open('"+engine+"', '"+id+"');";
		return varPkql;
	}
	
//	private String createJoinPkql(String[] varNames, List<List<String>> joinCols) {
//		String joinPkql = "data.join([";
//		
//		for(int i = 0; i < varNames.length; i++) {
//			if(i == 0) {
//				joinPkql += "v:"+varNames[i];
//			} else {
//				joinPkql += ", "+"v:"+varNames[i];
//			}
//		}
//		
//		joinPkql += "],[";
//		for(int i = 0; i < joinCols.size(); i++) {
//			List<String> joinCol = joinCols.get(i);
//			joinPkql += "[";
//			for(int j = 0; j < joinCol.size(); j++) {
//				String jc = "c:"+joinCol.get(j);
//				if(j==0) {
//					joinPkql += jc;
//				} else {
//					joinPkql += ", "+jc;
//				}
//			}
//			joinPkql += "]";
//		}
//		joinPkql += "]);";
//		
//		return joinPkql;
//	}
	
	private String createJoinPkql(Map<String, String> varHash, String pkql) {
		String newPkql = pkql;
		for(String varKey : varHash.keySet()) {
			String insightID = varHash.get(varKey);
			String insightID1 = "'"+insightID+"'";
			String insightID2 = "\""+insightID+"\"";
			if(pkql.contains(insightID1)) {
				newPkql = newPkql.replace(insightID1, "v:"+varKey);
			} else if(pkql.contains(insightID2)) {
				newPkql = newPkql.replace(insightID2, "v:"+varKey);
			}
		}
		
		return newPkql;
	}

	public void setWidgetId(String insightId, String[] ids) {
		this.insight2frameMap.put(insightId, ids);
		
	}
	
	public void removeWidgetId(String insightId, String[] ids) {
		this.insight2frameMap.remove(insightId, ids);
		
	}
	
	public void dropDashboard() {
		for(String view : insightMap.keySet()) {
			List<Insight> insights = insightMap.get(view);
			for(int i = 0; i < insights.size(); i++) {
				Insight insight = insights.get(i);
				
				//this will mean the insight is no longer joined
				insight.setParentInsight(null);
				
				H2Frame frame = (H2Frame)insight.getDataMaker();
				this.joiner.unJoinFrame(frame);
				
				//we have hit the last frame, drop the view
				if(i == insights.size() - 1) {
					this.joiner.dropView(view, frame);
				}
			}
		}
	}
	
	public void clearData() {
		filterHash = new HashMap<>();
		insightMap = new HashMap<>();
		insight2frameMap = new HashMap<>();
		config = new HashMap<>();
		insightOutputMap = new HashMap<>();
		joiner = new H2Joiner(this);
	}
	
	/************************************* END JOINING LOGIC **************************************/
}
