package prerna.ui.components.specific.ousd;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.OrderedBTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.PlaySheetRDFMapBasedEnum;

public class SystemRiskVisualizationPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(SystemRiskVisualizationPlaySheet.class.getName());
	private String cleanActInsightString = "What clean groups can activities be put in?";
	protected String baselineTotal = "BASELINE TOTAL:::::::::::::::::::::";
	private double failureRate = 0.05;

	public SystemRiskVisualizationPlaySheet(){
		super();
	}

	@Override
	public Hashtable getDataMakerOutput(String... selectors){
		Hashtable returnHash = OUSDPlaysheetHelper.getData(this.title, this.questionNum, this.dataFrame, PlaySheetRDFMapBasedEnum.getSheetName("Grid"));
		return returnHash;
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
		calc.setGroupData((Map<String, List<String>>)groupData[0], (Map<String, List<String>>)groupData[1], (List<String>)groupData[2]);
		calc.setFailure(this.failureRate);

		// set the systems
		List<String> owners = new ArrayList<String>();
		owners.add("DFAS");
		List<String> systems = OUSDQueryHelper.getSystemsByOwners(this.engine, owners);

		// set the blu map
		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(systems);
		String sysBindingsString = (String) systemReturn[1];
		Map<String, Map<String, List<String>>> bluMap = OUSDQueryHelper.getActivityGranularBluSystemMap(this.engine, sysBindingsString);
		calc.setBluMap(bluMap);

		Map<String, Double> bluSysResults = new HashMap<String, Double>();
		systems.add(baselineTotal);
		for(String system: systems){
			List<String> notSystem = new ArrayList<String>();
			for(String sys: systems){
				if(!sys.equals(system)){
					notSystem.add(sys);
				}
			}
			calc.setData(notSystem);
			Map<String, Double> systemResults = calc.runRiskCalculations();
			double systemMax = 0.0;
			for(String key: systemResults.keySet()){
				double value = systemResults.get(key);
				if(value > systemMax){
					systemMax = value;
				}
			}
			bluSysResults.put(system, systemMax);
		}

		// create table
		System.out.println(bluSysResults.toString());

		Map<String, Map<String, List<String>>> dataMap = OUSDQueryHelper.getActivityDataSystemMap(this.engine, sysBindingsString);
		calc.setBluMap(dataMap);

		Map<String, Double> dataSysResults = new HashMap<String, Double>();

		for(String system: systems){
			List<String> notSystem = new ArrayList<String>();
			for(String sys: systems){
				if(!sys.equals(system)){
					notSystem.add(sys);
				}
			}
			calc.setData(notSystem);
			Map<String, Double> systemResults = calc.runRiskCalculations();
			double systemMax = 0.0;
			for(String key: systemResults.keySet()){
				double value = systemResults.get(key);
				if(value > systemMax){
					systemMax = value;
				}
			}
			dataSysResults.put(system, systemMax);
		}

		// create table
		System.out.println(dataSysResults.toString());

		Map<String, Map<String, List<String>>> bluDataSystemMap = combineMaps(bluMap, dataMap);
		calc.setBluMap(bluDataSystemMap);

		Map<String, Double> bluDataSysResults = new HashMap<String, Double>();

		for(String system: systems){
			List<String> notSystem = new ArrayList<String>();
			for(String sys: systems){
				if(!sys.equals(system)){
					notSystem.add(sys);
				}
			}
			calc.setData(notSystem);
			Map<String, Double> systemResults = calc.runRiskCalculations();
			double systemMax = 0.0;
			for(String key: systemResults.keySet()){
				double value = systemResults.get(key);
				if(value > systemMax){
					systemMax = value;
				}
			}
			bluDataSysResults.put(system, systemMax);
		}

		// create table
		System.out.println(bluDataSysResults.toString());
		
		
		createTable(bluSysResults, dataSysResults, bluDataSysResults, calc.getCriticalSystems());
	}
	
	private Map<String, Map<String, List<String>>> combineMaps(Map<String, Map<String, List<String>>> dataMap, Map<String, Map<String, List<String>>> bluMap){
		Map<String, Map<String, List<String>>> totalMap = new HashMap<String, Map<String, List<String>>>();
		totalMap.putAll(dataMap);
		for(String key : bluMap.keySet()) {
		    Map<String, List<String>> actMapBlu = bluMap.get(key);
		    Map<String, List<String>> actMapData = totalMap.get(key);
		    if(actMapData != null) {
		    	actMapData.putAll(actMapBlu);
		    } else {
		    	totalMap.put(key,actMapBlu);
		    }
		}
		return totalMap;
	}

	private void createTable(Map<String, Double> bluSysResults, Map<String, Double> dataSysResults, Map<String, Double> bluDataSysResults, Map<String, List<String>> criticalSysMap){
//		this.list = new ArrayList<Object[]>();
		this.dataFrame = new OrderedBTreeDataFrame(new String[]{"System", "BLU Risk Score", "BLU Percent Difference", "Data Risk Score", "Data Percent Difference", "BLU-Data Risk Score", "BLU-Data Percent Difference", "Lost BLU / Data"});
		Iterator<String> keyIt = bluSysResults.keySet().iterator();
		while(keyIt.hasNext()){
			String myGroup = keyIt.next();

			BigDecimal one = new BigDecimal(1.00000);
			BigDecimal oneHundred = new BigDecimal(100.00000);
			BigDecimal bluRes = new BigDecimal(bluSysResults.get(myGroup)).setScale(40, BigDecimal.ROUND_HALF_UP);
			BigDecimal dataSysRes = new BigDecimal(dataSysResults.get(myGroup)).setScale(40, BigDecimal.ROUND_HALF_UP);
			BigDecimal bluDataSysRes = new BigDecimal(bluDataSysResults.get(myGroup)).setScale(40, BigDecimal.ROUND_HALF_UP);
			BigDecimal bluBase = new BigDecimal(bluSysResults.get(this.baselineTotal)).setScale(40, BigDecimal.ROUND_HALF_UP);
			BigDecimal dataBase = new BigDecimal(dataSysResults.get(this.baselineTotal)).setScale(40, BigDecimal.ROUND_HALF_UP);
			BigDecimal bluDataBase = new BigDecimal(bluDataSysResults.get(this.baselineTotal)).setScale(40, BigDecimal.ROUND_HALF_UP);
			
			Object[] newRow = new Object[8];
			newRow[0] = myGroup;
			newRow[1] = bluRes.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
			newRow[2] = (((bluRes).divide(bluBase, 40, BigDecimal.ROUND_HALF_UP).subtract(one)).multiply(oneHundred)).setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
			newRow[3] = dataSysRes.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
			newRow[4] = (((dataSysRes).divide(dataBase, 40, BigDecimal.ROUND_HALF_UP).subtract(one)).multiply(oneHundred)).setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
			newRow[5] = bluDataSysRes.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
			newRow[6] = (((bluDataSysRes).divide(bluDataBase, 40, BigDecimal.ROUND_HALF_UP).subtract(one)).multiply(oneHundred)).setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
			List<String> criticalObjects = criticalSysMap.get(myGroup);
			if(criticalObjects!=null) {
				newRow[7] = criticalObjects.toString();
			}
			this.dataFrame.addRow(newRow);
		}
//		this.names = new String[]{"System", "BLU Risk Score", "BLU Percent Difference", "Data Risk Score", "Data Percent Difference", "BLU-Data Risk Score", "BLU-Data Percent Difference", "Lost BLU / Data"};
	}

}
