package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

public class DashBoard implements IDataMaker {

	private static final Logger LOGGER = LogManager.getLogger(DashBoard.class.getName());
	List<Insight> attachedInsights;
	Map<Insight, List<DataMakerComponent>> componentMap;
	
	public DashBoard() {
		attachedInsights = new ArrayList<>(4);
		componentMap = new HashMap<>();
	}
	
	public DashBoard(String insight1, String insight2) {
		this();
	}
	
	public DashBoard(DataMakerComponent component) {
		//this must be a pkql transformation, must be a join
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		//process this component to each
		//how do i know where this component came from?
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {
		LOGGER.info("We are processing " + transforms.size() + " pre transformations");
		for(ISEMOSSTransformation transform : transforms) { 
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
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
		for(Insight insight : attachedInsights) {
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
		return null;
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
}
