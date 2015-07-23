package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GridPlaySheet;

public class ActivityGroupPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(ActivityGroupPlaySheet.class.getName());
	String insightName;
	private static List<String> procActivityGroups;
	private static List<String> procSystemGroups;
	private static String[] columnNames;
	private static Map<String, List<String>> actGroupMap = new HashMap<String, List<String>>();
	private static Map<String, List<String>> sysGroupMap = new HashMap<String, List<String>>();

	/**
	 * 
	 */
	public ActivityGroupPlaySheet(){
		super();
	}

	/* (non-Javadoc)
	 * @see prerna.ui.components.playsheets.AbstractRDFPlaySheet#setQuery(java.lang.String)
	 */
	@Override
	public void setQuery(String query){
		String delimiters = "[,]";
		String[] insights = query.split(delimiters);
		insightName = insights[0];
	}

	/* (non-Javadoc)
	 * @see prerna.ui.components.playsheets.BasicProcessingPlaySheet#createData()
	 */
	@Override
	public void createData(){

		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
		proc.processQuestionQuery(this.engine, insightName, emptyTable);
		ActivitySystemGroupPlaySheet activitySheet = (ActivitySystemGroupPlaySheet) proc.getPlaySheet();

		//createData makes the table...
		activitySheet.createData();
		ITableDataFrame frame =  activitySheet.getDataFrame();
		List<Object[]> homeTable = frame.getData();
		String[] names = activitySheet.getNames();

		Map<String, Integer> namesMap = new HashMap<String, Integer>();

		for(int i=0; i < names.length; i++){
			namesMap.put(names[i], i);
			System.out.println(names[i]+"--"+i);
		}

		columnNames = new String[3];
		columnNames[0] = "Group";
		if(namesMap.containsKey("Activity")){
			columnNames[1] = "Activity Group";
		}
		if(namesMap.containsKey("System Group")){
			columnNames[2] = "System Group";
		}

		procActivityGroups = new ArrayList<String>();
		procSystemGroups = new ArrayList<String>();

		findSequence(homeTable, namesMap, new ArrayList<String>(), new ArrayList<String>(), 0, procActivityGroups, procSystemGroups);

		this.dataFrame = new BTreeDataFrame(columnNames);

		createTable();

	}

	private void createTable(){
		this.dataFrame = new BTreeDataFrame(columnNames);

		for(String key: actGroupMap.keySet()){
			Map<String, Object> hashRow = new HashMap<String, Object>();
			for(String act: actGroupMap.get(key)){
				hashRow.put(columnNames[0], key);
				hashRow.put(columnNames[1], act);
				hashRow.put(columnNames[2], "");
				dataFrame.addRow(hashRow, hashRow);
			}
			for(String sys: sysGroupMap.get(key)){
				hashRow.put(columnNames[0], key);
				hashRow.put(columnNames[1], "");
				hashRow.put(columnNames[2], sys);
				dataFrame.addRow(hashRow, hashRow);
			}
		}
	}

	/**
	 * @param homeTable
	 * @param nameMap
	 * @param columns
	 * @param groupNumber
	 * @param procActivityGroups
	 * @param procSystemGroups
	 */
	private static void findSequence(List<Object[]> homeTable, Map<String, Integer> nameMap, List<String> actGroup, List<String> sysGroup, Integer groupNumber, List<String> procActivityGroups, List<String> procSystemGroups){

		homeTable = tableCleanup(homeTable, nameMap);

		for(Object[] row: homeTable){
			String activityGroup = row[nameMap.get("Activity Group")].toString();
			String systemGroup = row[nameMap.get("System Group")].toString();

			if(procActivityGroups.contains(activityGroup) && procSystemGroups.contains(systemGroup)){
				continue;
			}

			if(actGroup.isEmpty()){
				System.out.println("No current group. Adding "+activityGroup);
				actGroup.add(activityGroup);
			}

			//run through activities to find systems
			List<List<String>> groups = findMatchingSet(actGroup, sysGroup, true, homeTable, procActivityGroups, procSystemGroups, nameMap);

			actGroup = groups.get(0);
			sysGroup = groups.get(1);

			//mark groups initially added
			updateProcessedGroups(actGroup, sysGroup, procActivityGroups, procSystemGroups);

			//run through systems to find activities
			groups = findMatchingSet(actGroup, sysGroup, false, homeTable, procActivityGroups, procSystemGroups, nameMap);

			actGroup = groups.get(0);
			sysGroup = groups.get(1);

			//mark new groups added after running
			updateProcessedGroups(actGroup, sysGroup, procActivityGroups, procSystemGroups);

			List<String> activities = new ArrayList<String>();
			activities.addAll(actGroup);
			List<String> systems = new ArrayList<String>();
			systems.addAll(sysGroup);

			List<String> actDependencies = new ArrayList<String>();
			List<String> sysDependencies = new ArrayList<String>();

			//find missing activities
			dependencyLocator(activities, homeTable, nameMap, actDependencies, actGroup, sysGroup, true);

			//find missing systems
			dependencyLocator(systems, homeTable, nameMap, sysDependencies, actGroup, sysGroup, false);

			//update groups 
			for(String act: actGroup){
				if(!activities.contains(act)){
					activities.add(act);
				}
			}
			for(String sys: sysGroup){
				if(!systems.contains(sys)){
					systems.add(sys);
				}
			}

			/** rerun algorithm for missing dependencies */
			List<Object[]> actSubGroup = new ArrayList<Object[]>();
			List<Object[]> sysSubGroup = new ArrayList<Object[]>();

			//rerun for missing activity dependencies
			for(String group: activities){
				for(Object[] missingAct: homeTable){
					if(group.equals(missingAct[nameMap.get("Activity Group")].toString())){
						if(!sysGroup.contains(missingAct[nameMap.get("System Group")].toString())){
							if(!procSystemGroups.contains(missingAct[nameMap.get("System Group")].toString())){
								actSubGroup.add(missingAct);
							}
						}
					}
				}
				if(!actSubGroup.isEmpty()){
					System.out.println("::::::Missing systems");
					findSequence(homeTable, nameMap, actGroup, sysGroup, groupNumber, procActivityGroups, procSystemGroups);
				}
			}

			//rerun for missing system dependencies
			for(String group: systems){
				for(Object[] missingSys: homeTable){
					if(missingSys[nameMap.get("System Group")] != null && !missingSys[nameMap.get("System Group")].toString().isEmpty()){
						if(group.equals(missingSys[nameMap.get("System Group")].toString())){
							if(!actGroup.contains(missingSys[nameMap.get("Activity Group")].toString())){
								if(!procActivityGroups.contains(missingSys[nameMap.get("Activity Group")].toString())){
									sysSubGroup.add(missingSys);
								}
							}
						}
					}
				}
				if(!sysSubGroup.isEmpty()){
					System.out.println("::::::Missing activities");
					findSequence(homeTable, nameMap, actGroup, sysGroup, groupNumber, procActivityGroups, procSystemGroups);
				}
			}			

			//mark new groups added after running
			updateProcessedGroups(actGroup, sysGroup, procActivityGroups, procSystemGroups);

			mapBuilder(actGroup, sysGroup, groupNumber);
			
			groupNumber++;
			System.out.println("Updated group number: "+groupNumber);
			actGroup.clear();
			sysGroup.clear();
		}
	}

	/**
	 * @param columns
	 * @param isActivity
	 * @param homeTable
	 * @param procActivityGroups
	 * @param procSystemGroups
	 * @param nameMap
	 * @return
	 */
	private static List<List<String>> findMatchingSet(List<String> actGroup, List<String> sysGroup, boolean isActivity, List<Object[]> homeTable, List<String> procActivityGroups, List<String> procSystemGroups, Map<String, Integer> nameMap){

		List<String> currentGroup = new ArrayList<String>();
		List<String> depGroup = new ArrayList<String>();
		List<String> processingGroup = new ArrayList<String>();
		List<String> discoveredGroup = new ArrayList<String>();
		List<String> alreadyProcessed = new ArrayList<String>();
		List<String> alreadyDiscProcessed = new ArrayList<String>();
		List<String> dependencies = new ArrayList<String>();
		int procIdx = 0;
		int discIdx = 0;
		String logging = "";

		//setup variables for different runs
		if(isActivity){
			currentGroup = actGroup;
			depGroup = sysGroup;
			logging = "ACTIVITY RUN: ";
			procIdx = nameMap.get("Activity Group");
			discIdx = nameMap.get("System Group");
			alreadyProcessed = procActivityGroups;
			alreadyDiscProcessed = procSystemGroups;
		}else{
			currentGroup = sysGroup;
			depGroup= actGroup;
			logging = "SYSTEM RUN: ";
			discIdx = nameMap.get("Activity Group");
			procIdx = nameMap.get("System Group");
			alreadyDiscProcessed = procActivityGroups;
			alreadyProcessed = procSystemGroups;
		}

		System.out.println(logging);
		System.out.println("Current group size is: "+currentGroup.size());

		for(String groupNo: currentGroup){
			for(Object[] row: homeTable){				
				if(row[procIdx].toString().equals(groupNo)){
					List<String> foundDep = dependencyStringParser(row[procIdx+1].toString());
					for(String dep: foundDep){
						if(!dep.equals("___") && !dependencies.contains(dep)){
							System.out.println(logging+"Adding dependency "+dep+" for group "+groupNo);
							dependencies.add(dep);
						}
					}
				}else{
					continue;
				}
			}
		}

		for(String dep: dependencies){
			if(!alreadyProcessed.contains(dep)){
				currentGroup.add(dep);
			}
		}

		for(String groupNo: currentGroup){
			processingGroup.add(groupNo);
			if(isActivity){
				System.out.println("Current activity is: "+groupNo);
			}else{
				System.out.println("Current system is: "+groupNo);
			}

			for(Object[] row: homeTable){
				String rowProcValue = row[procIdx].toString();					
				String rowDiscValue = row[discIdx].toString();
				if(currentGroup.contains(rowProcValue)){
					if(!rowProcValue.contains("_")){
						//don't add processed groups
						if(!alreadyProcessed.contains(rowProcValue)){
							System.out.println(logging+rowProcValue+" has not been processed.");
							processingGroup.add(rowProcValue);
							if(rowDiscValue.contains("_")){
								continue;
							}else{
								discoveredGroup.add(rowDiscValue);
							}
						}else if(alreadyProcessed.contains(rowProcValue) && !alreadyDiscProcessed.contains(rowDiscValue)){
							discoveredGroup.add(rowDiscValue);
						}
					}
				}
			}				
		}
		for(String processedGroup: processingGroup){
			if(!currentGroup.contains(processedGroup)){					
				currentGroup.add(processedGroup);
			}
		}
		for(String discGroup: discoveredGroup){
			if(!depGroup.contains(discGroup)){					
				depGroup.add(discGroup);
			}

		}

		List<List<String>> groups = new ArrayList<List<String>>();

		if(isActivity){
			groups.add(currentGroup);
			groups.add(depGroup);
		}else{
			groups.add(depGroup);
			groups.add(currentGroup);
		}

		return groups;
	}

	/**
	 * @param groups
	 * @param homeTable
	 * @param nameMap
	 * @param groupDependencies
	 * @param actGroup
	 * @param sysGroup
	 * @param activity
	 */
	private static void dependencyLocator(List<String> groups, List<Object[]> homeTable, Map<String, Integer> nameMap, List<String> groupDependencies, List<String> actGroup, List<String> sysGroup, boolean activity){
		
		String groupValue ="";
		String groupDependency = "";
		List<String> newGroup = new ArrayList<String>();
		List<String> processedGroup = new ArrayList<String>();
		
		if(activity){
			groupValue = "Activity Group";
			groupDependency = "Activity Group Dependencies";
			newGroup = actGroup;
			processedGroup = procActivityGroups;
		}else{
			groupValue = "System Group";
			groupDependency = "System Group Dependencies";
			newGroup = sysGroup;
			processedGroup = procSystemGroups;
		}
		
		//build list of dependencies for activities
		for(String group: groups){
			for(Object[] potentialDependencyMatch: homeTable){
				String rowMatch = potentialDependencyMatch[nameMap.get(groupValue)].toString();
				if(rowMatch.equals(group)){
					groupDependencies = dependencyStringParser(potentialDependencyMatch[nameMap.get(groupDependency)].toString()); 							
				}else{
					continue;
				}
				if(!rowMatch.contains("_")){
					if(!groupDependencies.isEmpty()){
						for(String potentialMatch: groupDependencies){
							if(!newGroup.contains(potentialMatch)){
								if(!processedGroup.contains(potentialMatch)){
									System.out.println("::::::Found missing dependency: "+potentialMatch);
									newGroup.add(potentialMatch);
								}
							}
						}
					}
				}
			}
		}
	}

	private static void mapBuilder(List<String> actGroup, List<String> sysGroup, Integer groupNumber){
		System.out.println("-----Updating map!-----");
		List<String> actGroupClone = new ArrayList<String>();
		for(String act: actGroup){
			actGroupClone.add(act);
		}
		List<String> sysGroupClone = new ArrayList<String>();
		for(String sys: sysGroup){
			sysGroupClone.add(sys);
		}

		if(actGroupMap.containsKey(groupNumber.toString())){
			for(String act: actGroupMap.get(groupNumber.toString())){
				if(!actGroupClone.contains(act)){
					actGroupClone.add(act);
				}
			}
		}
		actGroupMap.put(groupNumber.toString(), actGroupClone);


		if(sysGroupMap.containsKey(groupNumber.toString())){
			for(String sys: sysGroupMap.get(groupNumber.toString())){
				if(!sysGroupClone.contains(sys)){
					sysGroupClone.add(sys);
				}
			}
		}
		sysGroupMap.put(groupNumber.toString(), sysGroupClone);

	}
	
	/**
	 * @param columns
	 * @param procActivityGroups
	 * @param procSystemGroups
	 */
	private static void updateProcessedGroups(List<String> actGroup, List<String> sysGroup, List<String> procActivityGroups, List<String> procSystemGroups){
		for(String activity: actGroup){
			if(!procActivityGroups.contains(activity)){
				System.out.println("Added activity group "+activity+" to processed list");
				procActivityGroups.add(activity);
			}
		}

		for(String system: sysGroup){
			if(!procSystemGroups.contains(system)){
				System.out.println("Added system group "+system+" to processed list");
				procSystemGroups.add(system);
			}
		}
	}

	/**
	 * @param group
	 * @return
	 */
	private static List<String> dependencyStringParser(String group){

		List<String> groupDependencies = new ArrayList<String>();

		if(group != null && !group.isEmpty()){
			group = group.substring(1);
			group = group.substring(0, group.length()-1);
			String[] dependencies = group.split(", ");


			for(String dependency: dependencies){
				groupDependencies.add(dependency);
			}
		}
		return groupDependencies;

	}

	/**
	 * @param homeTable
	 * @param nameMap
	 * @return
	 */
	private static List<Object[]> tableCleanup(List<Object[]> homeTable, Map<String,Integer> nameMap){

		List<Object[]> updatedHomeTable = new ArrayList<Object[]>();

		for(Object[] row: homeTable){
			if(row[nameMap.get("System Group")] != null && !row[nameMap.get("System Group")].toString().isEmpty()){
				updatedHomeTable.add(row);

			}
		}

		return updatedHomeTable;
	}

	public static void main(String args[]){

		ArrayList<Object[]> test = new ArrayList<Object[]>();

		Object[] rowOne = new Object[7];
		rowOne[1] = 0.0;
		rowOne[2] = "";
		rowOne[5] = 4.0;
		rowOne[6] = "";
		test.add(rowOne);

		Object[] rowTwo = new Object[7];
		rowTwo[1] = 0.1;
		rowTwo[2] = "";
		rowTwo[5] = 0.1;
		rowTwo[6] = "";
		test.add(rowTwo);

		Object[] rowThree = new Object[7];
		rowThree[1] = 0.2;
		rowThree[2] = "";
		rowThree[5] = 0.2;
		rowThree[6] = "[0.1]";
		test.add(rowThree);

		Object[] rowFour = new Object[7];
		rowFour[1] = 0.3;
		rowFour[2] = "";
		rowFour[5] = 0.3;
		rowFour[6] = "[0.2]";
		test.add(rowFour);

		Object[] rowFive = new Object[7];
		rowFive[1] = 4.0;
		rowFive[2] = "";
		rowFive[5] = 4.0;
		rowFive[6] = "[0.3]";
		test.add(rowFive);

		Object[] rowSix = new Object[7];
		rowSix[1] = 6.6;
		rowSix[2] = "";
		rowSix[5] = 6.3;
		rowSix[6] = "";
		test.add(rowSix);

		HashMap<String, Integer> names = new HashMap<String, Integer>();
		names.put("Activity Group", 1);
		names.put("System Group", 5);
		names.put("Activity Group Dependencies", 2);
		names.put("System Group Dependencies", 6);

		procActivityGroups = new ArrayList<String>();
		procSystemGroups = new ArrayList<String>();

		findSequence(test, names, new ArrayList<String>(), new ArrayList<String>(), 0, procActivityGroups, procSystemGroups);

		System.out.println("---------------");
		for(String key: actGroupMap.keySet()){
			for(String act: actGroupMap.get(key)){
				System.out.println("group is: "+key+". act is: "+act);
			}
			for(String sys: sysGroupMap.get(key)){
				System.out.println("group is: "+key+". sys is: "+sys);
			}
			System.out.println("---------------");
		}
	}

	//	/**
	//	 * @param homeTable
	//	 */
	//	private void processActivityGroups(ArrayList<Object[]> homeTable){
	//
	//		list = new ArrayList<Object[]>();
	//		HashMap<String, String> systemGroupToHighest = new HashMap<String, String>();
	//		for(Object[] row: homeTable){
	//			if(row[5]==null || row[5].toString().isEmpty()){
	//				continue;
	//			}else{
	//				systemGroupToHighest.put(row[5].toString(), "");
	//			}
	//		}
	//
	//
	//		double activityGroup = 0.0;
	//		for(String key: systemGroupToHighest.keySet()){
	//			Object[] newRow = new Object[2];
	//			for(Object[] row: homeTable){
	//				if(row[1]==null){
	//					continue;
	//				}else if(!(row[5]==null)){
	//					if(row[5].toString().equals(key)){
	//						if(Double.parseDouble(row[1].toString()) >= activityGroup){
	//							activityGroup = Double.parseDouble(row[1].toString());
	//						}
	//					}
	//				}
	//			}
	//			systemGroupToHighest.put(key, new String(""+activityGroup));
	//			newRow[0] = key;
	//			newRow[1] = systemGroupToHighest.get(key);
	//			list.add(newRow);
	//			activityGroup = 0.0;
	//		}
	//
	//	}

}
