package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

public class Dashboard implements IDataMaker {

	private static final Logger LOGGER = LogManager.getLogger(Dashboard.class.getName());
	Map<String, Insight> attachedInsights = new HashMap<>(4);
	private static String delimiter = ":::";
	private String insightID;
	
	public Dashboard() {
		
	}
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		
		Insight insight = null;
		String insightID = component.getRdbmsId();
		String engine = component.getEngine().getEngineName();
		
		String insightKey = engine+delimiter+insightID;
		if(insightID != null) {
//			Insight insight = InsightStore.getInstance().get(insightID);
//			String engine = insight.getEngineName();
//			String id = insight.getRdbmsId();
//			String insightKey = engine+delimiter+id;
		
			//i know this is duplicative to the above code but we want to ensure the insight we are running on is in fact joined to this dashboard
			insight = this.attachedInsights.get(insightKey);
			insight.getDataMaker().processDataMakerComponent(component);
		} else {
			//if join then need to add open insight and param pkql
			processPreTransformations(component, component.getPreTrans());
			processPostTransformations(component, component.getPostTrans());
			processActions(component, component.getActions());
			
		}
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {
//		LOGGER.info("We are processing " + transforms.size() + " pre transformations");
//		for(ISEMOSSTransformation transform : transforms) { 
//			transform.setDataMakers(this);
//			transform.setDataMakerComponent(dmc);
//			transform.runMethod();
//		}
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
		for(ISEMOSSTransformation transform : transforms){
			//get the id from the pkql transformation and run in on that join
			transform.setDataMakers(extendedArray);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
		
		for(String key : attachedInsights.keySet()) {
			Insight insight = attachedInsights.get(key);
			insight.setParentInsight(InsightStore.getInstance().get(this.insightID));
//			insight.getDataMaker().processPostTransformations(dmc, transforms, dataFrame);
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
		for(String insightID : attachedInsights.keySet()) {
			Insight insight = attachedInsights.get(insightID);
			ITableDataFrame frame = (ITableDataFrame)insight.getDataMaker();
			Object output = insight.getDataMaker().getDataMakerOutput(frame.getColumnHeaders());
			returnHash.put(insight.getInsightID(), output);
		}
		return returnHash;
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
		
	}

	@Override
	public String getUserId() {
		return null;
	}

	@Override
	public String getDataMakerName() {
		return "DashBoard";
	}
	
	public void addInsight(Insight insight) {
		this.attachedInsights.put(insight.getInsightID(), insight);
	}
	
	public List<Insight> getInsights() {
		List<Insight> insightList = new ArrayList<>();
		for(String key : attachedInsights.keySet()) {
			insightList.add(attachedInsights.get(key));
		}
		return insightList;
	}
	
	public void setInsightID(String insightID) {
		this.insightID = insightID;
	}

	@Override
	public void resetDataId() {
		// TODO Auto-generated method stub
		
	}
}
