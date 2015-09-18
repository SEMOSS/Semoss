package prerna.ui.components.specific.ousd;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.Utility;

public final class OUSDPlaysheetHelper {

	protected static final Logger LOGGER = LogManager.getLogger(OUSDPlaysheetHelper.class.getName());

	private OUSDPlaysheetHelper(){

	}

	public static Hashtable getData(String title, String questionNumber, ITableDataFrame dataFrame, String playsheetName){
		String playSheetClassName = PlaySheetEnum.getClassFromName(playsheetName);
		BasicProcessingPlaySheet playSheet = null;
		try {
			playSheet = (BasicProcessingPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (InstantiationException e) {
			e.printStackTrace();
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (IllegalAccessException e) {
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (SecurityException e) {
			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		}
		playSheet.setDataFrame(dataFrame);
		playSheet.setQuestionID(questionNumber);
		playSheet.setTitle(title);
		playSheet.processQueryData();
		return playSheet.getData();
	}

	/**
	 * @param costDbName
	 * @return
	 */
	public static String getCostDatabase(String costDbName){

		String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);

		String delimiters = "[;]";
		String[] engines = engineNames.split(delimiters);

		String updatedEngine = null;
		String highestVersion = null;

		for(String engine: engines){
			if(engine == null || engine.isEmpty()){
				continue;
			}else if(!engine.contains(costDbName)){
				continue;
			}

			String versionId = engine.substring(costDbName.length()+1);

			if(highestVersion == null || highestVersion.compareToIgnoreCase(versionId) < 0){
				highestVersion = versionId;
				updatedEngine = engine;
			}else{
				continue;
			}
		}

		return updatedEngine;

	}

	/**
	 * @param table
	 * @param keyIdx
	 * @param valueIdx
	 * @return
	 */
	public static Map<String, List<String>> mapSetup(List<Object[]> table, Integer keyIdx, Integer valueIdx){

		Map<String, List<String>> returnMap = new HashMap<String, List<String>>();
		for(Object[] row: table){
			String key = row[keyIdx].toString();
			String value = row[valueIdx].toString();
			if(returnMap.keySet().contains(key)){
				returnMap.get(key).add(value);					
			}else{
				List<String> newList = new ArrayList<String>();
				newList.add(value);
				returnMap.put(key, newList);
			}
		}
		return returnMap;
	}

	public static Object[] createSysLists(List<String> returnList){
		String sysBindingsString = "";
		List<String> sysArray = new ArrayList<String>();
		for(String system: returnList){
			if(!sysArray.contains(system)){
				sysBindingsString = sysBindingsString + "(<http://semoss.org/ontologies/Concept/System/" + system + ">)";
				sysArray.add(system);
			}
		}
		String[] systems = new String[sysArray.size()];
		systems = sysArray.toArray(systems);

		return new Object[]{systems, sysBindingsString};
	}

	/**
	 * @param bluTable
	 * @return
	 */
	public static String bluBindingStringMaker(Map<String, List<String>> bluMap){
		String bluBindingString = "";
		List<String> bluArray = new ArrayList<String>();

		for(String blu: bluMap.keySet()){
			if(!bluArray.contains(blu)){
				bluBindingString = bluBindingString + "(<http://semoss.org/ontologies/Concept/BusinessLogicUnit/" + blu + ">)";
				bluArray.add(blu);
			}
		}

		return bluBindingString;
	}

	/**
	 * @param bluData
	 * @param dataMap
	 * @param bluMap
	 * @return
	 */
	public static Map<String, List<String>> systemToGranularBLU(List<Object[]> bluData, Map<String, List<String>> dataMap, Map<String, List<String>> bluMap){

		Map<String, List<String>> granularReturnMap = new HashMap<String, List<String>>();
		for(Object[] row: bluData){
			String blu = Utility.getInstanceName(row[0].toString());
			String dataObj = Utility.getInstanceName(row[1].toString());

			List<String> bluValues = new ArrayList<String>();
			List<String> dataValues = new ArrayList<String>();
			List<String> systems = new ArrayList<String>();

			if(bluMap.keySet().contains(blu)){
				bluValues = bluMap.get(blu);
			}
			if(dataMap.keySet().contains(dataObj)){
				dataValues = dataMap.get(dataObj);
			}

			if(bluValues != null && dataValues != null){
				if(!bluValues.isEmpty() && !dataValues.isEmpty()){
					for(String system: bluValues){
						if(dataValues.contains(system)){
							systems.add(system);
						}
					}
					if(!systems.isEmpty()){
						granularReturnMap.put(blu+"||"+dataObj, systems);
					}
				}
			}
		}

		return granularReturnMap;
	}

	public static OUSDTimeline buildTimeline(IEngine engine, String timelineName) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		return buildTimeline(engine, timelineName, null);
	}

	public static OUSDTimeline buildTimeline(IEngine engine, String timelineName, String owner) throws ClassNotFoundException, InstantiationException, IllegalAccessException{

		timelineName = timelineName.toUpperCase().replaceAll(" ", "_");

		String timelineClassName = (String) engine.getProperty(timelineName);

		Class<?> timeClass = Class.forName(timelineClassName);
		ITimelineGenerator time = (ITimelineGenerator) timeClass.newInstance();

		if(owner==null){
			time.createTimeline(engine, owner);
		}
		else {
			time.createTimeline(engine, owner);
		}
		
		OUSDTimeline timeline = time.getTimeline();
		timeline.setName(timelineName);

		timeline = fillTimeline(timeline, engine, owner);

		return timeline;
	}

	private static OUSDTimeline fillTimeline(OUSDTimeline timeline, IEngine engine, String owner){

		List<String> systemList = new ArrayList<String>();

		Map<String, Double> sysBudget = new HashMap<String, Double>();
		Map<String, List<String>> bluMap = new HashMap<String, List<String>>();
		Map<String, List<String>> dataSystemMap = new HashMap<String, List<String>>();
		Map<String, List<String>> granularBLUMap = new HashMap<String, List<String>>();
		Map<String, List<List<String>>> sdsMap = new HashMap<String, List<List<String>>>();
		Map<String, List<String>> targetMap = new HashMap<String, List<String>>();

		List<String> owners = new ArrayList<String>();
		if(owner == null){
			owner = "DFAS";
		}
		owners.add(owner);
		List<String> osystems = OUSDQueryHelper.getSystemsByOwners(engine, owners);

		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(osystems);
		String[] sysList = (String[]) systemReturn[0];
		String sysBindingsString = (String) systemReturn[1];

		if(timeline.getSystemDownstreamMap().isEmpty()){
			sdsMap = OUSDQueryHelper.getSystemToSystemData(engine);
			timeline.setSystemDownstream(sdsMap);
		}
		if(timeline.getBudgetMap().isEmpty()){
			sysBudget = OUSDQueryHelper.getBudgetData(engine, sysList);
			timeline.setBudgetMap(sysBudget);
		}
		if(timeline.getDataSystemMap().isEmpty()){
			dataSystemMap = OUSDQueryHelper.getDataCreatedBySystem(engine, sysBindingsString);
			timeline.setDataSystemMap(dataSystemMap);
		}
		if(timeline.getGranularBLUMap().isEmpty()){
			bluMap = OUSDQueryHelper.getBLUtoSystem(engine, sysBindingsString);
			String bluBindings = OUSDPlaysheetHelper.bluBindingStringMaker(bluMap);


			List<Object[]> bluDataList = OUSDQueryHelper.getDataConsumedByBLU(engine, bluBindings);

			granularBLUMap = OUSDPlaysheetHelper.systemToGranularBLU(bluDataList, dataSystemMap, bluMap);

			timeline.setGranularBLUMap(granularBLUMap);
		}

		if(timeline.getTargetMap().isEmpty()){
			targetMap = OUSDQueryHelper.getSystemToTarget(engine, sysBindingsString);
			timeline.setTargetMap(targetMap);
			timeline.updateTargetSystems();

		}

		if(timeline.getTreeMaxList() == null){	
			buildRiskList(timeline, sysList, sysBindingsString, engine);
		}

		timeline.verifyYears();

		return timeline;
	}

	public static void buildRiskList(OUSDTimeline timeline, String[] sysList, String systemBindings, IEngine roadmapEngine){
		ActivityGroupRiskCalculator calc = new ActivityGroupRiskCalculator();
		String cleanActInsightString = "What clean groups can activities supporting a given E2E be put into?";
		double failureRate = 0.001;

		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
		proc.processQuestionQuery(roadmapEngine, cleanActInsightString, emptyTable);
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();

		Map<Integer, List<List<Integer>>> decomGroups = activitySheet.collectData();
		Object[] groupData = activitySheet.getResults(decomGroups);

		calc.setGroupData((Map<String, List<String>>)groupData[0], (Map<String, List<String>>)groupData[1], (List<String>)groupData[2]);
		calc.setFailure(failureRate);

		List<String> systemList = new ArrayList<String>();
		for(String system: sysList){
			systemList.add(system);
		}

		Map<String, Map<String, List<String>>> activityDataSystemMap = OUSDQueryHelper.getActivityDataSystemMap(roadmapEngine, systemBindings);
		Map<String, Map<String, List<String>>> activityBluSystemMap = OUSDQueryHelper.getActivityGranularBluSystemMap(roadmapEngine, systemBindings);

		Map<String, Map<String, List<String>>> bluDataSystemMap = OUSDPlaysheetHelper.combineMaps(activityDataSystemMap, activityBluSystemMap);

		List<String> decomList = new ArrayList<String>();
		List<Double> treeList = new ArrayList<Double>();
		calc.setBluMap(bluDataSystemMap);
		calc.setData(systemList);

		for(Map<String, List<String>> yearMap: timeline.getTimeData()){
			Map<String, Double> systemResults = calc.runRiskCalculations();
			double treeMax = 0.0;
			for(String key: systemResults.keySet()){
				double value = systemResults.get(key);
				if(value > treeMax){
					treeMax = value;
				}
			}
			//			this.treeMax = (double) ((1)/(1 - treeMax));
			treeList.add(treeMax);
			timeline.setTreeMaxList(treeList);
			List<String> updatedSystems = systemList;
			for(String system: yearMap.keySet()){
				decomList.add(system);
				updatedSystems.remove(system);
			}
			systemList = updatedSystems;
			updateBluDataSystemMap(calc, decomList, bluDataSystemMap);
		}
	}

	private static void updateBluDataSystemMap(ActivityGroupRiskCalculator calc, List<String> decomList, Map<String, Map<String, List<String>>> bluDataSystemMap){
		for(String activity: bluDataSystemMap.keySet()){
			Map<String, List<String>> bluDataSystem = bluDataSystemMap.get(activity);
			for(String bluData: bluDataSystem.keySet()){
				List<String> systems = bluDataSystem.get(bluData);
				for(String system: systems){
					if(decomList.contains(system)){
						List<String> updatedSystems = new ArrayList<String>(systems);
						updatedSystems.remove(system);
						systems = updatedSystems;
						bluDataSystem.put(bluData, updatedSystems);
					}
				}
			}
			bluDataSystemMap.put(activity, bluDataSystem);
		}
		calc.setBluMap(bluDataSystemMap);
	}

	public static Map<String, Map<String, List<String>>> combineMaps(Map<String, Map<String, List<String>>> mapOne, Map<String, Map<String, List<String>>> mapTwo){
		Map<String, Map<String, List<String>>> combinedMap = new HashMap<String, Map<String, List<String>>>();
		combinedMap.putAll(mapOne);
		for(String key : mapTwo.keySet()) {
			Map<String, List<String>> subMapTwo = mapTwo.get(key);
			Map<String, List<String>> subMapOne = combinedMap.get(key);
			if(subMapOne != null) {
				subMapOne.putAll(subMapTwo);
			} else {
				combinedMap.put(key,subMapTwo);
			}
		}
		return combinedMap;
	}
}
