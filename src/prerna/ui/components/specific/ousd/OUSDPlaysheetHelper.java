package prerna.ui.components.specific.ousd;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.annotations.BREAKOUT;
import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetRDFMapBasedEnum;
import prerna.util.Utility;

@BREAKOUT
public final class OUSDPlaysheetHelper {

	protected static final Logger logger = LogManager.getLogger(OUSDPlaysheetHelper.class);

	private static final String STACKTRACE = "StackTrace: ";
	private static final String NO_PLAY_SHEET = "No such PlaySheet: ";

	private OUSDPlaysheetHelper(){

	}

	public static Hashtable getData(String title, String questionNumber, ITableDataFrame dataFrame, String playsheetName){
		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName(playsheetName);
		TablePlaySheet playSheet = null;
		try {
			playSheet = (TablePlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			logger.error(STACKTRACE, ex);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		} catch (InstantiationException ie) {
			logger.error(STACKTRACE, ie);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		} catch (IllegalAccessException iae) {
			logger.error(STACKTRACE, iae);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		} catch (IllegalArgumentException iare) {
			logger.error(STACKTRACE, iare);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		} catch (InvocationTargetException ite) {
			logger.error(STACKTRACE, ite);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		} catch (NoSuchMethodException nsme) {
			logger.error(STACKTRACE, nsme);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		} catch (SecurityException se) {
			logger.error(STACKTRACE, se);
			logger.fatal(NO_PLAY_SHEET+ playSheetClassName);
		}

		if (playSheet == null) {
			throw new NullPointerException("playSheet can't be null here.");
		}

		playSheet.setDataMaker(dataFrame);
		playSheet.setQuestionID(questionNumber);
		playSheet.setTitle(title);
		playSheet.processQueryData();

		return (Hashtable) playSheet.getDataMakerOutput();
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
			} else if(!engine.contains(costDbName)) {
				continue;
			}

			String versionId = engine.substring(costDbName.length()+1);

			if(highestVersion == null || highestVersion.compareToIgnoreCase(versionId) < 0){
				highestVersion = versionId;
				updatedEngine = engine;
			} else {
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

		Map<String, List<String>> returnMap = new HashMap<>();
		for(Object[] row: table){
			String key = row[keyIdx].toString();
			String value = row[valueIdx].toString();
			if(returnMap.keySet().contains(key)){
				returnMap.get(key).add(value);					
			}else{
				List<String> newList = new ArrayList<>();
				newList.add(value);
				returnMap.put(key, newList);
			}
		}
		return returnMap;
	}

	public static Object[] createSysLists(List<String> returnList){
		String sysBindingsString = "";
		List<String> sysArray = new ArrayList<>();
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
		List<String> bluArray = new ArrayList<>();

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

		Map<String, List<String>> granularReturnMap = new HashMap<>();
		for(Object[] row: bluData){
			String blu = Utility.getInstanceName(row[0].toString());
			String dataObj = Utility.getInstanceName(row[1].toString());

			List<String> bluValues = new ArrayList<>();
			List<String> dataValues = new ArrayList<>();
			List<String> systems = new ArrayList<>();

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

	public static OUSDTimeline buildTimeline(IDatabaseEngine engine, String timelineName) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		return buildTimeline(engine, timelineName, null);
	}

	public static OUSDTimeline buildTimeline(IDatabaseEngine engine, String timelineName, String owner) throws ClassNotFoundException, InstantiationException, IllegalAccessException{

		timelineName = timelineName.toUpperCase().replace(" ", "_");

		String timelineClassName = engine.getProperty(timelineName);

		Class<?> timeClass = Class.forName(timelineClassName);
		ITimelineGenerator time = (ITimelineGenerator) timeClass.newInstance();

		if(owner==null){
			time.createTimeline(engine);
		}
		else {
			time.createTimeline(engine, owner);
		}
		
		OUSDTimeline timeline = time.getTimeline();
		timeline.setName(timelineName);

		timeline = fillTimeline(timeline, engine, owner);

		return timeline;
	}

	private static OUSDTimeline fillTimeline(OUSDTimeline timeline, IDatabaseEngine engine, String owner){

		List<String> systemList = new ArrayList<>();

		Map<String, Double> sysBudget = new HashMap<>();
		Map<String, List<String>> bluMap = new HashMap<>();
		Map<String, List<String>> dataSystemMap = new HashMap<>();
		Map<String, List<String>> granularBLUMap = new HashMap<>();
		Map<String, List<List<String>>> sdsMap = new HashMap<>();
		Map<String, List<String>> targetMap = new HashMap<>();
		List<String> owners = new ArrayList<>();

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

	public static void buildRiskList(OUSDTimeline timeline, String[] sysList, String systemBindings, IDatabaseEngine roadmapEngine){
		ActivityGroupRiskCalculator calc = new ActivityGroupRiskCalculator();
		String cleanActInsightString = "What clean groups can activities supporting a given E2E be put into?";
		double failureRate = 0.001;

//		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
//		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
//		proc.processQuestionQuery(roadmapEngine, cleanActInsightString, emptyTable);
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet)  OUSDPlaysheetHelper.getPlaySheetFromName(cleanActInsightString, roadmapEngine);;

		Map<Integer, List<List<Integer>>> decomGroups = activitySheet.collectData();
		Object[] groupData = activitySheet.getResults(decomGroups);

		calc.setGroupData((Map<String, List<String>>)groupData[0], (Map<String, List<String>>)groupData[1], (List<String>)groupData[2]);
		calc.setFailure(failureRate);

		List<String> systemList = new ArrayList<>();
		for(String system: sysList){
			systemList.add(system);
		}

		Map<String, Map<String, List<String>>> activityDataSystemMap = OUSDQueryHelper.getActivityDataSystemMap(roadmapEngine, systemBindings);
		Map<String, Map<String, List<String>>> activityBluSystemMap = OUSDQueryHelper.getActivityGranularBluSystemMap(roadmapEngine, systemBindings);

		Map<String, Map<String, List<String>>> bluDataSystemMap = OUSDPlaysheetHelper.combineMaps(activityDataSystemMap, activityBluSystemMap);

		List<String> decomList = new ArrayList<>();
		List<Double> treeList = new ArrayList<>();
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
			// this.treeMax = (double) ((1)/(1 - treeMax));
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
						List<String> updatedSystems = new ArrayList<>(systems);
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
		Map<String, Map<String, List<String>>> combinedMap = new HashMap<>();
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
	
	public static IPlaySheet getPlaySheetFromName(String insightName, IDatabaseEngine mainEngine){
		return null;
		
//		IDatabase qEng = mainEngine.getInsightDatabase();
//		String query = "SELECT ID FROM QUESTION_ID WHERE QUESTION_NAME = '"+insightName+"'";
//		ISelectWrapper it = WrapperManager.getInstance().getSWrapper(qEng, query);
////		Integer qID = (Integer) frame.getAllData().get(0)[0];
//		String qID = "";
//		if(it.hasNext()) {
//			ISelectStatement val = it.next();
//			qID = val.getValues()[0] + "";
//		}
//		OldInsight in = (OldInsight) mainEngine.getInsight(qID).get(0);
//
////		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
////		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
////		proc.processQuestionQuery(roadmapEngine, cleanActInsightString, emptyTable);
//		IPlaySheet activitySheet = in.getPlaySheet();
//		activitySheet.setQuery(in.getDataMakerComponents().get(0).getQuery());
//		activitySheet.setRDFEngine(in.getDataMakerComponents().get(0).getEngine());
//		
//		return activitySheet;
	}
}
