package prerna.ui.components.specific.ousd;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.OrderedBTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.PlaySheetRDFMapBasedEnum;

public class RiskVisualizationPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(RiskVisualizationPlaySheet.class.getName());
	private String cleanActInsightString = "What clean groups can activities be put in?";
	private double failureRate = 0.05;

	public RiskVisualizationPlaySheet(){
		super();
	}

	@Override
	public void setQuery(String query){
		this.failureRate = Double.parseDouble(query);
	}

	@Override
	public void createData(){
//		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
//		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
//		proc.processQuestionQuery(this.engine, cleanActInsightString, emptyTable);
//		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) OUSDPlaysheetHelper.getPlaySheetFromName(cleanActInsightString, this.engine);

		Map<Integer, List<List<Integer>>> decomGroups = activitySheet.collectData();
		Object[] groupData = activitySheet.getResults(decomGroups);

		ActivityGroupRiskCalculator calc = new ActivityGroupRiskCalculator();
		calc.setFailure(this.failureRate);
		calc.setGroupData((Map<String, List<String>>)groupData[0], (Map<String, List<String>>)groupData[1], (List<String>)groupData[2]);

		// set the systems
		List<String> owners = new ArrayList<String>();
		owners.add("DFAS");
		List<String> systems = OUSDQueryHelper.getSystemsByOwners(this.engine, owners);
		calc.setData(systems);

		// set the blu map
		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(systems);
		String sysBindingsString = (String) systemReturn[1];
		Map<String, Map<String, List<String>>> bluMap = OUSDQueryHelper.getActivityGranularBluSystemMap(this.engine, sysBindingsString);
		calc.setBluMap(bluMap);

		// run the calculator
		Map<String, Double> bluActResults = calc.runRiskCalculations();

		// create table
		System.out.println(bluActResults.toString());

		Map<String, Map<String, List<String>>> dataMap = OUSDQueryHelper.getActivityDataSystemMap(this.engine, sysBindingsString);
		calc.setBluMap(dataMap);

		Map<String, Double> dataActResults = calc.runRiskCalculations();

		Map<String, Map<String, List<String>>> bluDataSystemMap = OUSDPlaysheetHelper.combineMaps(bluMap, dataMap);
		calc.setBluMap(bluDataSystemMap);

		Map<String, Double> bluDataActResults = calc.runRiskCalculations();

		// create table
		createTable(bluActResults, dataActResults, bluDataActResults, (Map<String, List<String>>)groupData[0]);
	}

	private void createTable(Map<String, Double> results, Map<String, List<String>> activityMap){
		this.dataFrame = new OrderedBTreeDataFrame(new String[]{"Activity", "Activity Group", "Risk Score"});
		//		this.list = new ArrayList<Object[]>();
		Iterator<String> keyIt = results.keySet().iterator();
		while(keyIt.hasNext()){
			String myGroup = keyIt.next();
			List<String> acts = activityMap.get(myGroup);
			for(String act : acts){
				Object[] newRow = new Object[3];
				newRow[0] = act;
				newRow[1] = myGroup;
				newRow[2] = results.get(myGroup);
				this.dataFrame.addRow(newRow);
			}
		}
		//		this.names = new String[]{"Activity", "Activity Group", "Risk Score"};
	}

	private void createTable(Map<String, Double> bluSysResults, Map<String, Double> dataSysResults, Map<String, Double> bluDataSysResults, Map<String, List<String>> activityMap){
		this.dataFrame = new OrderedBTreeDataFrame(new String[]{"Activity", "Activity Group", "BLU Risk Score", "Data Risk Score", "BLU-Data Risk Score"});
		Iterator<String> keyIt = bluSysResults.keySet().iterator();
		while(keyIt.hasNext()){
			String myGroup = keyIt.next();
			List<String> acts = activityMap.get(myGroup);
			for(String act : acts){
				Object[] newRow = new Object[5];

				BigDecimal bluRes = new BigDecimal(bluSysResults.get(myGroup)).setScale(5, BigDecimal.ROUND_HALF_UP);
				BigDecimal dataSysRes = new BigDecimal(dataSysResults.get(myGroup)).setScale(5, BigDecimal.ROUND_HALF_UP);
				BigDecimal bluDataSysRes = new BigDecimal(bluDataSysResults.get(myGroup)).setScale(5, BigDecimal.ROUND_HALF_UP);

				newRow[0] = act;
				newRow[1] = myGroup;
				newRow[2] = bluRes;
				newRow[3] = dataSysRes;
				newRow[4] = bluDataSysRes;
				this.dataFrame.addRow(newRow);
			}
		}
		//this.names = new String[]{"Activity", "Activity Group", "BLU Risk Score", "Data Risk Score", "BLU-Data Risk Score"};
	}

	@Override
	public Hashtable getDataMakerOutput(String... selectors){
		Hashtable returnHash = OUSDPlaysheetHelper.getData(this.title, this.questionNum, this.dataFrame, PlaySheetRDFMapBasedEnum.getSheetName("Grid"));
		return returnHash;
	}

}
