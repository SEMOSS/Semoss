package prerna.ui.helpers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JDesktopPane;

import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.FilterTransformation;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class OldInsightProcessor implements Runnable{

	OldInsight insight = null;

	/**
	 * Constructor for PlaysheetCreateRunner.
	 * @param playSheet IPlaySheet
	 */
	public OldInsightProcessor(OldInsight insight)
	{
		this.insight = insight;
	}

	public Map<String, Object> runWeb()
	{
		createData();
		Map<String, String> tableDataAlign = insight.getDataTableAlign();
		// previous insights did not save the table data align
		// if it is not present, we get the table data align by setting the data maker in the playsheet and grabbing it
		if((tableDataAlign == null || tableDataAlign.isEmpty())) {
			IPlaySheet playSheet = insight.getPlaySheet();
			// as we move towards a more generic FE with many different viz's
			// we do not want to build out playsheets on the BE 
			// just keep the view separate
			if(playSheet != null) {
				tableDataAlign = playSheet.getDataTableAlign();
				insight.setDataTableAlign(tableDataAlign);
			}
		}
		Map<String, Object> retData = insight.getWebData();
		return retData;
	}

	private IDataMaker createData(){
		IDataMaker dm = insight.getDataMaker();

		// get the list of data maker components from the insight
		List<DataMakerComponent> dmComps = insight.getDataMakerComponents();
		// logic to append the parameter information that was selected on view into the data maker components
		// this either fills the query if the parameter was saved as a string using @INPUT_NAME@ taxonomy or
		// it fills in the values in a Filtering PreTransformation which appends the metamodel
		insight.appendParamsToDataMakerComponents();

		// TODO: there shouldn't be any PKQLTransformations in the future
		// but for the time being, need to set a runner in each one if there are
		//		PKQLRunner runner = insight.getPKQLRunner();

		for(DataMakerComponent dmComp : dmComps){
			DataMakerComponent copyDmc = dmComp.copy();
			//			setRunnerInPKQLTrans(copyDmc, runner);
			dm.processDataMakerComponent(copyDmc);
		}
		// set the data maker in the insight
		insight.setDataMaker(dm);
		if(needToRecalc(insight.getDataMakerComponents())) {
			insight.recalcDerivedColumns();
		}

		return dm;
	}

	//	/**
	//	 * this is just to get pkql runner data to fe through create
	//	 * need to have the same runner for full create process
	//	 * @param copyDmc
	//	 * @param runner
	//	 */
	//	private void setRunnerInPKQLTrans(DataMakerComponent copyDmc, PKQLRunner runner) {
	//		for(ISEMOSSTransformation trans : copyDmc.getPreTrans()){
	//			if(trans instanceof PKQLTransformation){
	//				((PKQLTransformation) trans).setRunner(runner);
	//			}
	//		}
	//		for(ISEMOSSTransformation trans : copyDmc.getPostTrans()){
	//			if(trans instanceof PKQLTransformation){
	//				((PKQLTransformation) trans).setRunner(runner);
	//			}
	//		}
	//	}

	private boolean needToRecalc(List<DataMakerComponent> dmComps) {
		if(dmComps != null && dmComps.size() > 0) {
			DataMakerComponent lastComponent = dmComps.get(dmComps.size() - 1);
			if(lastComponent != null) {
				List<ISEMOSSTransformation> transformations = lastComponent.getPostTrans();
				if(transformations != null && transformations.size() > 0) {
					ISEMOSSTransformation lastTrans = transformations.get(transformations.size() - 1);
					if(lastTrans instanceof FilterTransformation) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@Override
	public void run() {
		createData();
		IPlaySheet playSheet = insight.getPlaySheet();
		Map<String, String> tableDataAlign = insight.getDataTableAlign();
		if(!(tableDataAlign == null || tableDataAlign.isEmpty())) {
			playSheet.setTableDataAlign(tableDataAlign);
		}
		preparePlaySheet(playSheet, insight);

		if(!insight.getAppend()){
			playSheet.runAnalytics();
			playSheet.processQueryData();
			playSheet.createView();
		}
		else {
			playSheet.runAnalytics();
			playSheet.overlayView();
		}
	}
	
	private void preparePlaySheet(IPlaySheet playSheet, OldInsight insight){
		// SET THE DESKTOP PANE
		playSheet.setJDesktopPane((JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE));
		
		// CREATE INSIGHT ID AND STORE IT
		String insightID = InsightStore.getInstance().put(insight);
		playSheet.setQuestionID(insightID);
		
		// CREATE THE INSIGHT TITLE
		String playSheetTitle = "";
		Map<String, List<Object>> paramHash = insight.getParamHash();
		if(paramHash != null && !paramHash.isEmpty())
		{
			// loops through and appends the selected parameters in the play sheet title
			Iterator<String> enumKey = paramHash.keySet().iterator();
			while (enumKey.hasNext())
			{
				String key = (String) enumKey.next();
				List<Object> value = paramHash.get(key);
				for(int i = 0; i < value.size(); i++) {
					Object val = value.get(i);
					if(val instanceof String || val instanceof Double ) {
						playSheetTitle = playSheetTitle + Utility.getInstanceName(val+"") + " - ";
					}
				}
			}
		}
		String name = insight.getInsightName();
		if (name == null){
			name = "Custom";
		}
		System.out.println("Param Hash is " + paramHash);
		playSheetTitle = playSheetTitle+name.trim();
		playSheet.setTitle(playSheetTitle);
	}

}
