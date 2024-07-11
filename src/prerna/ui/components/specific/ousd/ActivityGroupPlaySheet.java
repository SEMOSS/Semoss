//package prerna.ui.components.specific.ousd;
//
//import java.lang.reflect.InvocationTargetException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.ds.h2.H2Frame;
//import prerna.ui.components.playsheets.GridPlaySheet;
//import prerna.util.PlaySheetRDFMapBasedEnum;
//
//public class ActivityGroupPlaySheet extends GridPlaySheet{
//
//	private static final Logger LOGGER = LogManager.getLogger(ActivityGroupPlaySheet.class.getName());
//	String insightName;
//	private static List<String> procActivityGroups;
//	private static List<String> procSystemGroups;
//	private static ArrayList<Object[]> groupList = new ArrayList<Object[]>();
//
//	/**
//	 * 
//	 */
//	public ActivityGroupPlaySheet(){
//		super();
//	}
//
//	/* (non-Javadoc)
//	 * @see prerna.ui.components.playsheets.AbstractRDFPlaySheet#setQuery(java.lang.String)
//	 */
//	@Override
//	public void setQuery(String query){
//		String delimiters = "[,]";
//		String[] insights = query.split(delimiters);
//		insightName = insights[0];
//	}
//
//	/* (non-Javadoc)
//	 * @see prerna.ui.components.playsheets.BasicProcessingPlaySheet#createData()
//	 */
//	@Override
//	public void createData(){
//
////		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
////		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
////		proc.processQuestionQuery(this.engine, insightName, emptyTable);
//		ActivitySystemGroupPlaySheet activitySheet = (ActivitySystemGroupPlaySheet)  OUSDPlaysheetHelper.getPlaySheetFromName(insightName, engine);
//
//		//createData makes the table...
//		activitySheet.createData();
//		List<Object[]> homeTable = activitySheet.getList();
//		String[] names = activitySheet.getNames();
//
//		Map<String, Integer> namesMap = new HashMap<String, Integer>();
//
//		for(int i=0; i < names.length; i++){
//			namesMap.put(names[i], i);
//			LOGGER.info(names[i]+"--"+i);
//		}
//
//		String[] columnNames = new String[3];
//		columnNames[0] = "Group";
//		if(namesMap.containsKey("Activity")){
//			columnNames[1] = "Activity Group";
//		}
//		if(namesMap.containsKey("System Group")){
//			columnNames[2] = "System Group";
//		}
//
//		procActivityGroups = new ArrayList<String>();
//		procSystemGroups = new ArrayList<String>();
//
//		findSequence(homeTable, namesMap, new ArrayList<String>(), new ArrayList<String>(), 0, procActivityGroups, procSystemGroups);
//
//		this.dataFrame = new H2Frame(columnNames);
//		for(Object[] row : groupList){
//			this.dataFrame.addRow(row, columnNames);
//		}
////		this.names = columnNames;
////		this.list = groupList;
//	}
//
//	//	/**
//	//	 * @param homeTable
//	//	 */
//	//	private void processActivityGroups(ArrayList<Object[]> homeTable){
//	//
//	//		list = new ArrayList<Object[]>();
//	//		HashMap<String, String> systemGroupToHighest = new HashMap<String, String>();
//	//		for(Object[] row: homeTable){
//	//			if(row[5]==null || row[5].toString().isEmpty()){
//	//				continue;
//	//			}else{
//	//				systemGroupToHighest.put(row[5].toString(), "");
//	//			}
//	//		}
//	//
//	//
//	//		double activityGroup = 0.0;
//	//		for(String key: systemGroupToHighest.keySet()){
//	//			Object[] newRow = new Object[2];
//	//			for(Object[] row: homeTable){
//	//				if(row[1]==null){
//	//					continue;
//	//				}else if(!(row[5]==null)){
//	//					if(row[5].toString().equals(key)){
//	//						if(Double.parseDouble(row[1].toString()) >= activityGroup){
//	//							activityGroup = Double.parseDouble(row[1].toString());
//	//						}
//	//					}
//	//				}
//	//			}
//	//			systemGroupToHighest.put(key, new String(""+activityGroup));
//	//			newRow[0] = key;
//	//			newRow[1] = systemGroupToHighest.get(key);
//	//			list.add(newRow);
//	//			activityGroup = 0.0;
//	//		}
//	//
//	//	}
//
//	/**
//	 * @param homeTable
//	 * @param nameMap
//	 * @param columns
//	 * @param groupNumber
//	 * @param procActivityGroups
//	 * @param procSystemGroups
//	 */
//	private static void findSequence(List<Object[]> homeTable, Map<String, Integer> nameMap, List<String> actGroup, List<String> sysGroup, Integer groupNumber, List<String> procActivityGroups, List<String> procSystemGroups){
//
//		homeTable = tableCleanup(homeTable, nameMap);
//
//		for(Object[] row: homeTable){
//			String activityGroup = row[nameMap.get("Activity Group")].toString();
//			String systemGroup = row[nameMap.get("System Group")].toString();
//
//			if(procActivityGroups.contains(activityGroup) && procSystemGroups.contains(systemGroup)){
//				continue;
//			}
//
//			if(actGroup.isEmpty()){
//				LOGGER.info("No current group. Adding "+activityGroup);
//				actGroup.add(activityGroup);
//			}
//
//			//run through activities to find systems
//			List<List<String>> groups = findMatchingSet(actGroup, sysGroup, true, homeTable, procActivityGroups, procSystemGroups, nameMap);
//
//			actGroup = groups.get(0);
//			sysGroup = groups.get(1);
//
//			//mark groups initially added
//			updateProcessedGroups(actGroup, sysGroup, procActivityGroups, procSystemGroups);
//
//			//run through systems to find activities
//			groups = findMatchingSet(actGroup, sysGroup, false, homeTable, procActivityGroups, procSystemGroups, nameMap);
//
//			actGroup = groups.get(0);
//			sysGroup = groups.get(1);
//
//			//mark new groups added after running
//			updateProcessedGroups(actGroup, sysGroup, procActivityGroups, procSystemGroups);
//
//			List<String> activities = new ArrayList<String>();
//			activities.addAll(actGroup);
//			List<String> systems = new ArrayList<String>();
//			systems.addAll(sysGroup);
//
//			List<String> actDependencies = new ArrayList<String>();
//			List<String> sysDependencies = new ArrayList<String>();
//
//			//find missing activities
//			dependencyLocator(activities, homeTable, nameMap, actDependencies, actGroup, sysGroup, true);
//
//			//find missing systems
//			dependencyLocator(systems, homeTable, nameMap, sysDependencies, actGroup, sysGroup, false);
//
//			//update groups 
//			for(String act: actGroup){
//				if(!activities.contains(act)){
//					activities.add(act);
//				}
//			}
//			for(String sys: sysGroup){
//				if(!systems.contains(sys)){
//					systems.add(sys);
//				}
//			}
//
//			/** rerun algorithm for missing dependencies */
//			List<Object[]> actSubGroup = new ArrayList<Object[]>();
//			List<Object[]> sysSubGroup = new ArrayList<Object[]>();
//
//			//rerun for missing activity dependencies
//			for(String group: activities){
//				for(Object[] missingAct: homeTable){
//					if(group.equals(missingAct[nameMap.get("Activity Group")].toString())){
//						if(!sysGroup.contains(missingAct[nameMap.get("System Group")].toString())){
//							if(!procSystemGroups.contains(missingAct[nameMap.get("System Group")].toString())){
//								actSubGroup.add(missingAct);
//							}
//						}
//					}
//				}
//				if(!actSubGroup.isEmpty()){
//					LOGGER.info("::::::Missing systems");
//					findSequence(homeTable, nameMap, actGroup, sysGroup, groupNumber, procActivityGroups, procSystemGroups);
//				}
//			}
//
//			//rerun for missing system dependencies
//			for(String group: systems){
//				for(Object[] missingSys: homeTable){
//					if(missingSys[nameMap.get("System Group")] != null && !missingSys[nameMap.get("System Group")].toString().isEmpty()){
//						if(group.equals(missingSys[nameMap.get("System Group")].toString())){
//							if(!actGroup.contains(missingSys[nameMap.get("Activity Group")].toString())){
//								if(!procActivityGroups.contains(missingSys[nameMap.get("Activity Group")].toString())){
//									sysSubGroup.add(missingSys);
//								}
//							}
//						}
//					}
//				}
//				if(!sysSubGroup.isEmpty()){
//					LOGGER.info("::::::Missing activities");
//					findSequence(homeTable, nameMap, actGroup, sysGroup, groupNumber, procActivityGroups, procSystemGroups);
//				}
//			}			
//
//			//mark new groups added after running
//			updateProcessedGroups(actGroup, sysGroup, procActivityGroups, procSystemGroups);
//
//
//
//			for(String activity: actGroup){
//				Object[] activityRow = new Object[3];
//				activityRow[0] = groupNumber;
//				activityRow[1] = activity;
//				activityRow[2] = "";
//				groupList.add(activityRow);
//			}
//			for(String system: sysGroup){
//				Object[] systemRow = new Object[3];
//				systemRow[0] = groupNumber;
//				systemRow[1] = "";
//				systemRow[2] = system;
//				groupList.add(systemRow);
//			}
//
//			groupNumber++;
//			actGroup.clear();
//			sysGroup.clear();
//		}
//	}
//
//	/**
//	 * @param columns
//	 * @param isActivity
//	 * @param homeTable
//	 * @param procActivityGroups
//	 * @param procSystemGroups
//	 * @param nameMap
//	 * @return
//	 */
//	private static List<List<String>> findMatchingSet(List<String> actGroup, List<String> sysGroup, boolean isActivity, List<Object[]> homeTable, List<String> procActivityGroups, List<String> procSystemGroups, Map<String, Integer> nameMap){
//
//		List<String> currentGroup = new ArrayList<String>();
//		List<String> depGroup = new ArrayList<String>();
//		List<String> processingGroup = new ArrayList<String>();
//		List<String> discoveredGroup = new ArrayList<String>();
//		List<String> alreadyProcessed = new ArrayList<String>();
//		List<String> alreadyDiscProcessed = new ArrayList<String>();
//		List<String> dependencies = new ArrayList<String>();
//		List<String> foundDep = new ArrayList<String>();
//		int procIdx = 0;
//		int discIdx = 0;
//		String logging = "";
//
//		//setup variables for different runs
//		if(isActivity){
//			currentGroup = actGroup;
//			depGroup = sysGroup;
//			logging = "ACTIVITY RUN: ";
//			procIdx = nameMap.get("Activity Group");
//			discIdx = nameMap.get("System Group");
//			alreadyProcessed = procActivityGroups;
//			alreadyDiscProcessed = procSystemGroups;
//		}else{
//			currentGroup = sysGroup;
//			depGroup= actGroup;
//			logging = "SYSTEM RUN: ";
//			discIdx = nameMap.get("Activity Group");
//			procIdx = nameMap.get("System Group");
//			alreadyDiscProcessed = procActivityGroups;
//			alreadyProcessed = procSystemGroups;
//		}
//
//		LOGGER.info(logging);
//		LOGGER.info("Current group size is: "+currentGroup.size());
//
//		for(String groupNo: currentGroup){
//			for(Object[] row: homeTable){				
//				if(row[procIdx].toString().equals(groupNo)){
//					if(row[procIdx+1] != null){
//						foundDep = dependencyStringParser(row[procIdx+1].toString());
//					}
//					for(String dep: foundDep){
//						if(!dep.equals("___") && !dependencies.contains(dep)){
//							LOGGER.info(logging+"Adding dependency "+dep+" for group "+groupNo);
//							dependencies.add(dep);
//						}
//					}
//				}else{
//					continue;
//				}
//			}
//		}
//
//		for(String dep: dependencies){
//			if(!alreadyProcessed.contains(dep)){
//				currentGroup.add(dep);
//			}
//		}
//
//		for(String groupNo: currentGroup){
//			processingGroup.add(groupNo);
//			if(isActivity){
//				LOGGER.info("Current activity is: "+groupNo);
//			}else{
//				LOGGER.info("Current system is: "+groupNo);
//			}
//
//			for(Object[] row: homeTable){
//				String rowProcValue = row[procIdx].toString();					
//				String rowDiscValue = row[discIdx].toString();
//				if(currentGroup.contains(rowProcValue)){
//					if(!rowProcValue.contains("_")){
//						//don't add processed groups
//						if(!alreadyProcessed.contains(rowProcValue)){
//							LOGGER.info(logging+rowProcValue+" has not been processed.");
//							processingGroup.add(rowProcValue);
//							if(rowDiscValue.contains("_")){
//								continue;
//							}else{
//								discoveredGroup.add(rowDiscValue);
//							}
//						}else if(alreadyProcessed.contains(rowProcValue) && !alreadyDiscProcessed.contains(rowDiscValue)){
//							discoveredGroup.add(rowDiscValue);
//						}
//					}
//				}
//			}				
//		}
//		for(String processedGroup: processingGroup){
//			if(!currentGroup.contains(processedGroup)){					
//				currentGroup.add(processedGroup);
//			}
//		}
//		for(String discGroup: discoveredGroup){
//			if(!depGroup.contains(discGroup)){					
//				depGroup.add(discGroup);
//			}
//
//		}
//
//		List<List<String>> groups = new ArrayList<List<String>>();
//
//		if(isActivity){
//			groups.add(currentGroup);
//			groups.add(depGroup);
//		}else{
//			groups.add(depGroup);
//			groups.add(currentGroup);
//		}
//
//		return groups;
//	}
//
//	/**
//	 * @param groups
//	 * @param homeTable
//	 * @param nameMap
//	 * @param groupDependencies
//	 * @param actGroup
//	 * @param sysGroup
//	 * @param activity
//	 */
//	private static void dependencyLocator(List<String> groups, List<Object[]> homeTable, Map<String, Integer> nameMap, List<String> groupDependencies, List<String> actGroup, List<String> sysGroup, boolean activity){
//
//		String groupValue ="";
//		String groupDependency = "";
//		List<String> newGroup = new ArrayList<String>();
//		List<String> processedGroup = new ArrayList<String>();
//
//		if(activity){
//			groupValue = "Activity Group";
//			groupDependency = "Activity Group Dependencies";
//			newGroup = actGroup;
//			processedGroup = procActivityGroups;
//		}else{
//			groupValue = "System Group";
//			groupDependency = "System Group Dependencies";
//			newGroup = sysGroup;
//			processedGroup = procSystemGroups;
//		}
//
//		//build list of dependencies for activities
//		for(String group: groups){
//			for(Object[] potentialDependencyMatch: homeTable){
//				String rowMatch = potentialDependencyMatch[nameMap.get(groupValue)].toString();
//				if(rowMatch.equals(group)){
//					if(potentialDependencyMatch[nameMap.get(groupDependency)] != null){
//						groupDependencies = dependencyStringParser(potentialDependencyMatch[nameMap.get(groupDependency)].toString());
//					}
//				}else{
//					continue;
//				}
//				if(!rowMatch.contains("_")){
//					if(!groupDependencies.isEmpty()){
//						for(String potentialMatch: groupDependencies){
//							if(!newGroup.contains(potentialMatch)){
//								if(!processedGroup.contains(potentialMatch)){
//									LOGGER.info("::::::Found missing dependency: "+potentialMatch);
//									newGroup.add(potentialMatch);
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//	}
//
//	/**
//	 * @param columns
//	 * @param procActivityGroups
//	 * @param procSystemGroups
//	 */
//	private static void updateProcessedGroups(List<String> actGroup, List<String> sysGroup, List<String> procActivityGroups, List<String> procSystemGroups){
//		for(String activity: actGroup){
//			if(!procActivityGroups.contains(activity)){
//				LOGGER.info("Added activity group "+activity+" to processed list");
//				procActivityGroups.add(activity);
//			}
//		}
//
//		for(String system: sysGroup){
//			if(!procSystemGroups.contains(system)){
//				LOGGER.info("Added system group "+system+" to processed list");
//				procSystemGroups.add(system);
//			}
//		}
//	}
//
//	/**
//	 * @param group
//	 * @return
//	 */
//	private static List<String> dependencyStringParser(String group){
//
//		List<String> groupDependencies = new ArrayList<String>();
//
//		if(group != null && !group.isEmpty()){
//			group = group.substring(1);
//			group = group.substring(0, group.length()-1);
//			String[] dependencies = group.split(", ");
//
//
//			for(String dependency: dependencies){
//				groupDependencies.add(dependency);
//			}
//		}
//		return groupDependencies;
//
//	}
//
//	/**
//	 * @param homeTable
//	 * @param nameMap
//	 * @return
//	 */
//	private static List<Object[]> tableCleanup(List<Object[]> homeTable, Map<String,Integer> nameMap){
//
//		List<Object[]> updatedHomeTable = new ArrayList<Object[]>();
//
//		for(Object[] row: homeTable){
//			if(row[nameMap.get("System Group")] != null && !row[nameMap.get("System Group")].toString().isEmpty()){
//				updatedHomeTable.add(row);
//
//			}
//		}
//
//		return updatedHomeTable;
//	}
//
//	/**
//	 * @param homeTable
//	 * @param nameMap
//	 * @return
//	 */
//	private static ArrayList<Object[]> tableCleanup(List<Object[]> homeTable, HashMap<String,Integer> nameMap){
//
//		ArrayList<Object[]> updatedHomeTable = new ArrayList<Object[]>();
//
//		for(Object[] row: homeTable){
//			if(row[nameMap.get("System Group")] != null && !row[nameMap.get("System Group")].toString().isEmpty()){
//				updatedHomeTable.add(row);
//			}
//		}
//
//		return updatedHomeTable;
//	}
//
//	/**
//	 * @param group
//	 * @return
//	 */
//	private static int waveFinder(ArrayList<String> group){
//
//		int highestWave = 0;
//
//		for(String groupNo: group){
//			int groupInt = (int)Double.parseDouble(groupNo);
//			if(groupInt > highestWave){
//				highestWave = groupInt;
//			}
//		}
//
//		return highestWave;
//
//	}
//
//	//	public static void main(String args[]){
//	//
//	//		ArrayList<Object[]> test = new ArrayList<Object[]>();
//	//
//	//		Object[] rowOne = new Object[6];
//	//		rowOne[1] = 0.0;
//	//		rowOne[5] = 0.1;
//	//		test.add(rowOne);
//	//
//	//		Object[] rowTwo = new Object[6];
//	//		rowTwo[1] = 1.1;
//	//		rowTwo[5] = 1.1;
//	//		test.add(rowTwo);
//	//
//	//		Object[] rowThree = new Object[6];
//	//		rowThree[1] = 2.2;
//	//		rowThree[5] = 2.2;
//	//		test.add(rowThree);
//	//
//	//		Object[] rowFour = new Object[6];
//	//		rowFour[1] = 3.3;
//	//		rowFour[5] = 3.3;
//	//		test.add(rowFour);
//	//
//	//		Object[] rowFive = new Object[6];
//	//		rowFive[1] = 4.4;
//	//		rowFive[5] = 2.4;
//	//		test.add(rowFive);
//	//
//	//		Object[] rowSix = new Object[6];
//	//		rowSix[1] = 2.3;
//	//		rowSix[5] = "";
//	//		test.add(rowSix);
//	//
//	//		HashMap<String, Integer> names = new HashMap<String, Integer>();
//	//		names.put("Activity Group", 1);
//	//		names.put("System Group", 5);
//	//
//	//		procActivityGroups = new ArrayList<String>();
//	//		procSystemGroups = new ArrayList<String>();
//	//
//	//		findSequence(test, names, new ArrayList<ArrayList<String>>(), 0, procActivityGroups, procSystemGroups);
//	//
//	//		for(Object[] row: groupList){
//	//			LOGGER.info("ROW: "+row[0]+" | "+row[1]+" | "+row[2]);
//	//		}
//	//	}
//
//	@Override
//	public Hashtable getDataMakerOutput(String... selectors){
//		List<Object[]> theList = new ArrayList<Object[]>();
//		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName("Grid");
//		GridPlaySheet playSheet = null;
//		try {
//			playSheet = (GridPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
//		} catch (ClassNotFoundException ex) {
//			classLogger.error(Constants.STACKTRACE, ex);
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//		} catch (InstantiationException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//		} catch (IllegalAccessException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (IllegalArgumentException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (InvocationTargetException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (NoSuchMethodException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (SecurityException e) {
//			LOGGER.fatal("No such PlaySheet: "+ playSheetClassName);
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		playSheet.setTitle(this.title);
//		playSheet.setQuestionID(this.questionNum);//
//		Hashtable retHash = (Hashtable) playSheet.getDataMakerOutput();
//		ArrayList<Object[]> myList = groupList;
//		if(myList.size() > 1001){
//			theList = myList.subList(0, 1000);
//		}else{
//			theList = myList;
//		}
//		for(Object[] myRow : theList){
//			for(int i = 0; i < myRow.length; i++){
//				if(myRow[i] == null){
//					myRow[i] = "";
//				}
//				else {
//					myRow[i] = myRow[i].toString();
//				}
//			}
//		}
//		retHash.put("data", theList);
//		retHash.put("headers", new String[]{"Group", "Activity Group", "System Group"});
//		return retHash;
//	}
//
//}
