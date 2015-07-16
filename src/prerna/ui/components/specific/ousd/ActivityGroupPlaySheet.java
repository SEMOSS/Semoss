package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GridPlaySheet;

public class ActivityGroupPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(ActivityGroupPlaySheet.class.getName());
	String insightName;
	private static ArrayList<String> procActivityGroups;
	private static ArrayList<String> procSystemGroups;
	private static ArrayList<Object[]> groupList = new ArrayList<Object[]>();;

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
		ArrayList<Object[]> homeTable = activitySheet.getList();
		String[] names = activitySheet.getNames();

		HashMap<String, Integer> namesMap = new HashMap<String, Integer>();

		for(int i=0; i < names.length; i++){
			namesMap.put(names[i], i);
			System.out.println(names[i]+"--"+i);
		}

		String[] columnNames = new String[3];
		columnNames[0] = "Group";
		if(namesMap.containsKey("Activity")){
			columnNames[1] = "Activity Group";
		}
		if(namesMap.containsKey("System Group")){
			columnNames[2] = "System Group";
		}

		this.names = columnNames;

		procActivityGroups = new ArrayList<String>();
		procSystemGroups = new ArrayList<String>();

		findSequence(homeTable, namesMap, new ArrayList<ArrayList<String>>(), 0, procActivityGroups, procSystemGroups);

		list = new ArrayList<Object[]>();
		list = groupList;

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

	/**
	 * @param homeTable
	 * @param nameMap
	 * @param columns
	 * @param groupNumber
	 * @param procActivityGroups
	 * @param procSystemGroups
	 */
	private static void findSequence(ArrayList<Object[]> homeTable, HashMap<String, Integer> nameMap, ArrayList<ArrayList<String>> columns, Integer groupNumber, ArrayList<String> procActivityGroups, ArrayList<String> procSystemGroups){

		homeTable = tableCleanup(homeTable, nameMap);

		ArrayList<String> activityGroups = new ArrayList<String>();
		ArrayList<String> systemGroups = new ArrayList<String>();		
		ArrayList<Object[]> rowList = new ArrayList<Object[]>();

		columns.add(activityGroups);
		columns.add(systemGroups);

		for(Object[] row: homeTable){
			String activityGroup = row[nameMap.get("Activity Group")].toString();
			String systemGroup = row[nameMap.get("System Group")].toString();

			if(procActivityGroups.contains(activityGroup) && procSystemGroups.contains(systemGroup)){
				continue;
			}

			if(columns.get(0).isEmpty()){
				System.out.println("No current group. Adding "+activityGroup);
				columns.get(0).add(activityGroup);
			}

			//run through activities to find systems
			columns = findMatchingSet(columns, true, homeTable, procActivityGroups, procSystemGroups, nameMap);

			//mark groups initially added
			updateProcessedGroups(columns, procActivityGroups, procSystemGroups);

			//run through systems to find activities
			columns = findMatchingSet(columns, false, homeTable, procActivityGroups, procSystemGroups, nameMap);

			//
			ArrayList<String> activities = new ArrayList<String>();
			activities.addAll(columns.get(0));
			ArrayList<String> systems = new ArrayList<String>();
			systems.addAll(columns.get(1));

			ArrayList<Object[]> actSubGroup = new ArrayList<Object[]>();

			for(String group: activities){
				for(Object[] missingAct: homeTable){
					if(group.equals(missingAct[nameMap.get("Activity Group")].toString())){
						if(!columns.get(1).contains(missingAct[nameMap.get("System Group")].toString())){
							actSubGroup.add(missingAct);
						}
					}
				}
				if(!actSubGroup.isEmpty()){
					findSequence(homeTable, nameMap, columns, groupNumber, procActivityGroups, procSystemGroups);
				}
			}

			int highestWave = waveFinder(activities);

			for(Object[] lowerWave: homeTable){
				if((int)Double.parseDouble(lowerWave[nameMap.get("Activity Group")].toString()) <= highestWave){
					if(!columns.get(0).contains(lowerWave[nameMap.get("Activity Group")].toString())){
						if(!procActivityGroups.contains(lowerWave[nameMap.get("Activity Group")].toString())){							
							columns.get(0).add(lowerWave[nameMap.get("Activity Group")].toString());
						}
					}
				}
			}

			if(columns.get(0).size()>activities.size()){					
				findSequence(homeTable, nameMap, columns, groupNumber, procActivityGroups, procSystemGroups);
			}

			highestWave = waveFinder(systems);

			for(Object[] lowerWave: homeTable){
				if((int)Double.parseDouble(lowerWave[nameMap.get("System Group")].toString()) <= highestWave){
					if(!columns.get(1).contains(lowerWave[nameMap.get("System Group")].toString())){
						if(!procSystemGroups.contains(lowerWave[nameMap.get("System Group")].toString())){							
							columns.get(1).add(lowerWave[nameMap.get("System Group")].toString());
						}
					}
				}
			}


			if(columns.get(1).size()>systems.size()){					
				findSequence(homeTable, nameMap, columns, groupNumber, procActivityGroups, procSystemGroups);
			}

			//mark new groups added after running
			updateProcessedGroups(columns, procActivityGroups, procSystemGroups);

			for(String activity: columns.get(0)){
				Object[] activityRow = new Object[3];
				activityRow[0] = groupNumber;
				activityRow[1] = activity;
				activityRow[2] = "";
				groupList.add(activityRow);
			}
			for(String system: columns.get(1)){
				Object[] systemRow = new Object[3];
				systemRow[0] = groupNumber;
				systemRow[1] = "";
				systemRow[2] = system;
				groupList.add(systemRow);
			}

			groupNumber++;
			columns.get(0).clear();
			columns.get(1).clear();
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
	private static ArrayList<ArrayList<String>> findMatchingSet(ArrayList<ArrayList<String>> columns, boolean isActivity, ArrayList<Object[]> homeTable, ArrayList<String> procActivityGroups, ArrayList<String> procSystemGroups, HashMap<String, Integer> nameMap){

		int highestWave = 0;
		ArrayList<String> currentGroup = new ArrayList<String>();
		ArrayList<String> processingGroup = new ArrayList<String>();
		ArrayList<String> discoveredGroup = new ArrayList<String>();
		ArrayList<String> alreadyProcessed = new ArrayList<String>();
		ArrayList<String> alreadyDiscProcessed = new ArrayList<String>();
		int procIdx = 0;
		int discIdx = 0;
		int columnIdx = 0;
		int returnIdx = 0;
		String logging = "";

		if(isActivity){
			columnIdx = 0;
			returnIdx = 1;
			currentGroup = columns.get(0);
			logging = "ACTIVITY RUN: ";
			procIdx = nameMap.get("Activity Group");
			discIdx = nameMap.get("System Group");
			alreadyProcessed = procActivityGroups;
			alreadyDiscProcessed = procSystemGroups;
		}else{
			columnIdx = 1;
			returnIdx = 0;
			currentGroup = columns.get(1);
			logging = "SYSTEM RUN: ";
			discIdx = nameMap.get("Activity Group");
			procIdx = nameMap.get("System Group");
			alreadyDiscProcessed = procActivityGroups;
			alreadyProcessed = procSystemGroups;
		}

		highestWave = waveFinder(currentGroup);
		System.out.println(logging);
		System.out.println("Current group size is: "+currentGroup.size());
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

				if((int) Double.parseDouble(rowProcValue) <= highestWave){
					//don't add processed groups
					if(!alreadyProcessed.contains(rowProcValue)){
						System.out.println(logging+rowProcValue+" has not been processed.");
						processingGroup.add(rowProcValue);
						discoveredGroup.add(rowDiscValue);
					}else if(alreadyProcessed.contains(rowProcValue) && !alreadyDiscProcessed.contains(rowDiscValue)){
						discoveredGroup.add(rowDiscValue);
					}
				}
			}				
		}
		for(String processedGroup: processingGroup){
			if(!columns.get(columnIdx).contains(processedGroup)){					
				columns.get(columnIdx).add(processedGroup);
			}
		}
		for(String discGroup: discoveredGroup){
			if(!columns.get(returnIdx).contains(discGroup)){					
				columns.get(returnIdx).add(discGroup);
			}

		}
		return columns;
	}

	/**
	 * @param columns
	 * @param procActivityGroups
	 * @param procSystemGroups
	 */
	private static void updateProcessedGroups(ArrayList<ArrayList<String>> columns, ArrayList<String> procActivityGroups, ArrayList<String> procSystemGroups){
		for(String activity: columns.get(0)){
			if(!procActivityGroups.contains(activity)){
				System.out.println("Added activity group "+activity+" to processed list");
				procActivityGroups.add(activity);
			}
		}

		for(String system: columns.get(1)){
			if(!procSystemGroups.contains(system)){
				System.out.println("Added system group "+system+" to processed list");
				procSystemGroups.add(system);
			}
		}
	}

	/**
	 * @param homeTable
	 * @param nameMap
	 * @return
	 */
	private static ArrayList<Object[]> tableCleanup(ArrayList<Object[]> homeTable, HashMap<String,Integer> nameMap){

		ArrayList<Object[]> updatedHomeTable = new ArrayList<Object[]>();

		for(Object[] row: homeTable){
			if(row[nameMap.get("System Group")] != null && !row[nameMap.get("System Group")].toString().isEmpty()){
				updatedHomeTable.add(row);
			}
		}

		return updatedHomeTable;
	}

	/**
	 * @param group
	 * @return
	 */
	private static int waveFinder(ArrayList<String> group){

		int highestWave = 0;

		for(String groupNo: group){
			int groupInt = (int)Double.parseDouble(groupNo);
			if(groupInt > highestWave){
				highestWave = groupInt;
			}
		}

		return highestWave;

	}

	public static void main(String args[]){

		ArrayList<Object[]> test = new ArrayList<Object[]>();

		Object[] rowOne = new Object[6];
		rowOne[1] = 0.0;
		rowOne[5] = 0.1;
		test.add(rowOne);

		Object[] rowTwo = new Object[6];
		rowTwo[1] = 1.1;
		rowTwo[5] = 1.1;
		test.add(rowTwo);

		Object[] rowThree = new Object[6];
		rowThree[1] = 2.2;
		rowThree[5] = 2.2;
		test.add(rowThree);

		Object[] rowFour = new Object[6];
		rowFour[1] = 3.3;
		rowFour[5] = 3.3;
		test.add(rowFour);

		Object[] rowFive = new Object[6];
		rowFive[1] = 4.4;
		rowFive[5] = 2.4;
		test.add(rowFive);

		Object[] rowSix = new Object[6];
		rowSix[1] = 2.3;
		rowSix[5] = "";
		test.add(rowSix);

		HashMap<String, Integer> names = new HashMap<String, Integer>();
		names.put("Activity Group", 1);
		names.put("System Group", 5);

		procActivityGroups = new ArrayList<String>();
		procSystemGroups = new ArrayList<String>();

		findSequence(test, names, new ArrayList<ArrayList<String>>(), 0, procActivityGroups, procSystemGroups);

		for(Object[] row: groupList){
			System.out.println("ROW: "+row[0]+" | "+row[1]+" | "+row[2]);
		}
	}

}
