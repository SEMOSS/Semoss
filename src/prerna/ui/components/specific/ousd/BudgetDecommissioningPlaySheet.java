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
//public class BudgetDecommissioningPlaySheet extends GridPlaySheet{
//
//	String insightName;
//	static Integer groupNumber = 0;
//	private static Map<String, List<String>> dependencyMap;
//	private static Map<String, Integer> processedRows = new HashMap<String, Integer>();
//	private static Map<String, Double> budgets = new HashMap<String, Double>();
//	private static Map<String, Integer> namesMap = new HashMap<String, Integer>();
//
//	//some variables for retrieving specific values out of the Object[]. the values are set in the updateColumnNames method below.
//	//these three are from the systems simple table. we are expecting these to exist.
//	private static int system;
//	private static int systemGroup;
//	private static int systemDependencies;
//	//these two are hard coded in updateColumnNames. we are adding these as new columns.
//	private static int systemBudget;
//	private static int groupOrder;
//
//	private static final Logger LOGGER = LogManager.getLogger(BudgetDecommissioningPlaySheet.class.getName());
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
////		SequencingDecommissioningPlaySheet sdSheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
//		SequencingDecommissioningPlaySheet sdSheet = (SequencingDecommissioningPlaySheet) OUSDPlaysheetHelper.getPlaySheetFromName(insightName, this.engine);
//
//		//getting the table from the retrieved sheet
//		sdSheet.createData();
////		ITableDataFrame frame =  sdSheet.getDataFrame();
//		List<Object[]> sequence = sdSheet.getList();
//		String[] names = sdSheet.getNames();
//		
//		//retrieve the budget numbers from the helper class
//		budgets = OUSDQueryHelper.getBudgetData(this.engine, null);
//
//		createTable(sequence, budgets, names);
//	}
//
//	/**
//	 * @param sequence
//	 * @param systemToBudget
//	 * @param names
//	 */
//	private void createTable(List<Object[]> sequenceList, Map<String, Double> systemToBudget, String[] names){
//
//		String[] updatedNames = updateColumnNames(names);
//		List<Object[]> budgetSystemList = new ArrayList<Object[]>();
//
//		this.dataFrame = new H2Frame(updatedNames);
////		this.names = updatedNames;
////		this.list = new ArrayList<Object[]>();
//
//		for(Object[] row : sequenceList){
//			Object[] updatedRow = new Object[names.length+2];
//			for(int i = 0; i < names.length; i++){
//				updatedRow[i] = row[i];
//			}
//
//			if(systemToBudget.keySet().contains(row[system].toString())){				
//				updatedRow[names.length] = systemToBudget.get(row[system]);				
//			}else{
//				updatedRow[names.length] = new Double(1000000000.00);
//			}
//
//			budgetSystemList.add(updatedRow);
//
//		}
//
//		LOGGER.debug("Total systems with budgets found: "+budgetSystemList.size());
//		LOGGER.debug("Ordering Systems");
//
//		List<Object[]> orderedSystems = orderBudgetList(budgetSystemList, namesMap);
//
//		for(Object[] row: orderedSystems){
//			this.dataFrame.addRow(row, updatedNames);
////			list.add(row);
//		}		
//	}
//
//	/**
//	 * @param systemBudgetList
//	 */
//	private static List<Object[]> orderBudgetList(List<Object[]> systemBudgetList, Map<String, Integer> namesMap){
//
//		dependencyMap = createDependencyArrays(systemBudgetList, namesMap);
//		List<String> groupList = new ArrayList<String>();
//
//		for(Object[] rows: systemBudgetList){
//			if(!groupList.contains(rows[systemGroup].toString())){
//				groupList.add(rows[systemGroup].toString());
//			}
//		}
//		LOGGER.debug("Number of system groups: "+groupList.size());
//
//		//main loop for order the system groups by budget
//		while(processedRows.size() < groupList.size()){
//			LOGGER.debug("Number of processed system groups: "+processedRows.size());
//			List<Object[]> currentRows = availableGroupBuilder(dependencyMap, systemBudgetList, namesMap);
//			List<Object[]> maxRows = maxBudgetFinder(currentRows, namesMap);
//
//			for(Object[] row: maxRows){
//				//add groups to the processed list so loop eventually ends
//				if(processedRows.keySet().contains(row[systemGroup].toString())){
//					LOGGER.debug("System group "+row[systemGroup].toString()+" was already processed. Setting group order to "+processedRows.get(row[systemGroup].toString()));
//					row[groupOrder] = processedRows.get(row[systemGroup].toString());
//				}else{
//					LOGGER.debug("System group "+row[systemGroup].toString()+" hasn't been processed. Adding it to the processed list with group order number "+groupNumber);
//					row[groupOrder] = groupNumber;
//					processedRows.put(row[systemGroup].toString(), groupNumber);
//					groupNumber++;
//				}
//
//				//update the rows to include the group numbers we just added
//				for(Object[] systemRow: systemBudgetList){
//					if(systemRow[groupOrder] != null && !systemRow[groupOrder].toString().isEmpty())
//						if(row[systemGroup].toString().equals(systemRow[systemGroup].toString())){
//							systemRow = row;
//						}
//				}
//			}
//
//			dependencyMap = updateDependencyArrays(dependencyMap, maxRows, namesMap);
//		}
//
//		return systemBudgetList;
//
//	}
//
//	/**
//	 * @param systemBudgetMap
//	 * @param systemList
//	 * @return
//	 */
//	private static List<Object[]> availableGroupBuilder(Map<String, List<String>> systemBudgetMap, List<Object[]> systemList, Map<String, Integer> namesMap){
//
//		List<String> availableSystems = new ArrayList<String>();
//		List<Object[]> systemRows = new ArrayList<Object[]>();
//
//		for(String key: systemBudgetMap.keySet()){
//			if(!processedRows.keySet().contains(key)){
//				if(systemBudgetMap.get(key).isEmpty()){
//					availableSystems.add(key);
//				}
//			}
//		}
//
//		for(Object[] row: systemList){
//			if(availableSystems.contains(row[systemGroup].toString())){
//				systemRows.add(row);
//			}
//		}
//
//		return systemRows;
//
//	}
//
//	/**
//	 * @param systemList
//	 * @param namesMap
//	 * @return
//	 */
//	private static List<Object[]> maxBudgetFinder(List<Object[]> systemList, Map<String, Integer> namesMap){
//
//		List<Object[]> maxBudgetSystems = new ArrayList<Object[]>();
//		Map<String, Double> systemGroupBudgets = new HashMap<String, Double>();
//		double maxBudget = 0.0;
//
//		for(Object[] row: systemList){
//			String key = row[systemGroup].toString();
//			if(systemGroupBudgets.keySet().contains(key)){
//				systemGroupBudgets.put(key, (double)row[systemBudget] + systemGroupBudgets.get(key));
//			}else{
//				systemGroupBudgets.put(key, (double)row[systemBudget]);
//			}
//		}
//
//		for(Object[] row: systemList){
//			double groupBudget = systemGroupBudgets.get(row[systemGroup].toString());
//			LOGGER.debug("System group "+row[systemGroup].toString()+" has aggregated budget "+groupBudget);
//			if(!maxBudgetSystems.isEmpty() && maxBudgetSystems.get(0)[systemGroup]==row[systemGroup]){
//				LOGGER.debug("System group "+row[systemGroup].toString()+" has current highest total. Adding system "+row[system].toString());
//				maxBudgetSystems.add(row);
//			}else if(groupBudget > maxBudget){ 
//				LOGGER.debug("System group "+row[systemGroup].toString()+" has new highest total. Reseting lists. Adding "+row[system].toString());
//				maxBudget = groupBudget;
//				maxBudgetSystems.clear();
//				maxBudgetSystems.add(row);
//			}else if(groupBudget == maxBudget){
//				LOGGER.debug("System group "+row[systemGroup].toString()+" matches highest total. Multiple system groups now included. Adding "+row[system].toString());
//				maxBudgetSystems.add(row);
//			}
//		}
//
//		return maxBudgetSystems;
//
//	}
//
//	/**
//	 * @param systemList
//	 * @param namesMap
//	 * @return
//	 */
//	private static Map<String, List<String>> createDependencyArrays(List<Object[]> systemList, Map<String, Integer> namesMap){
//
//		Map<String, List<String>> dependencyMap = new HashMap<String, List<String>>();
//		String dependencies = "";
//	
//		LOGGER.debug("Converting dependecy strings to dependency arrays");
//
//		for(Object[] row: systemList){
//			List<String> deps = new ArrayList<String>();
//			String systemName = row[systemGroup].toString();
//			if(row[systemDependencies] != null && !row[systemDependencies].toString().isEmpty()){
//				dependencies = row[systemDependencies].toString();
//			}
//			if(dependencies != null && !dependencies.isEmpty()){
//				if(!dependencies.contains("_")){
//					dependencies = dependencies.substring(1);
//					dependencies = dependencies.substring(0, dependencies.length()-1);
//					String[] dependencyList = dependencies.split(", ");
//					for(String dependency: dependencyList){
//						deps.add(dependency);
//						dependencyMap.put(systemName, deps);
//					}
//				}else{
//					dependencyMap.put(systemName, new ArrayList<String>());
//				}
//			}else{
//				dependencyMap.put(systemName, new ArrayList<String>());
//			}
//		}
//
//		LOGGER.debug("Completed conversion of dependencies");
//
//		return dependencyMap;
//	}
//
//
//	/**
//	 * @param systemList
//	 * @param system
//	 * @param namesMap
//	 * @return
//	 */
//	private static Map<String, List<String>> updateDependencyArrays(Map<String, List<String>> systemList, List<Object[]> system, Map<String, Integer> namesMap){
//
//		Map<String, List<String>> dependencyMap = new HashMap<String, List<String>>();
//
//		LOGGER.debug("Begin updating dependency arrays");
//
//		for(String key : systemList.keySet()){
//			LOGGER.debug("System group is "+key);
//			List<String> updatedDeps = systemList.get(key);
//			for(Object[] row: system){
//				String sysGroup = row[systemGroup].toString();
//				if(systemList.get(key).size() > 0){
//					if(systemList.get(key).contains(sysGroup)){
//						LOGGER.debug("Removing "+sysGroup+" from dependencies for group "+key);
//						updatedDeps.remove(sysGroup);
//					}
//				}
//				dependencyMap.put(key, updatedDeps);
//			}
//		}
//		return dependencyMap;
//
//	}
//
//	/**
//	 * @param names
//	 * @return
//	 */
//	private String[] updateColumnNames(String[] names){
//
//		//create column names
//		String[] updatedNames = new String[names.length + 2];
//		for(int i=0; i<names.length; i++){
//			updatedNames[i]=names[i];
//			namesMap.put(names[i], i);
//		}
//		updatedNames[names.length] = "System Budget";
//		updatedNames[names.length+1] = "Group Order";
//
//		namesMap.put("System Budget", names.length);
//		namesMap.put("Group Order", names.length+1);
//
//		system = namesMap.get("System");
//		systemGroup = namesMap.get("System Group");
//		systemDependencies = namesMap.get("System Group Dependencies");
//		systemBudget = namesMap.get("System Budget");
//		groupOrder = namesMap.get("Group Order");
//
//		LOGGER.debug("Total columns: "+namesMap.size());
//
//		return updatedNames;
//	}
//
//	/**
//	 * main method for testing the grouping logic. does not test the dataFrame setting.
//	 * @param args
//	 */
//	//	public static void main(String args[]){
//	//
//	//		List<Object[]> test = new ArrayList<Object[]>();
//	//
//	//		Object[] rowOne = new Object[5];
//	//		rowOne[0] = "System1";
//	//		rowOne[1] = "0.0";
//	//		rowOne[2] = "_";
//	//		rowOne[3] = new Double(10000);
//	//
//	//		Object[] rowTwo = new Object[5];
//	//		rowTwo[0] = "System2";
//	//		rowTwo[1] = "0.1";
//	//		rowTwo[2] = "_";
//	//		rowTwo[3] = new Double(12000);
//	//
//	//		Object[] rowThree = new Object[5];
//	//		rowThree[0] = "System3";
//	//		rowThree[1] = "1.0";
//	//		rowThree[2] = "[0.0, 0.1]";
//	//		rowThree[3] = new Double(13000);
//	//
//	//		Object[] rowFour = new Object[5];
//	//		rowFour[0] = "System4";
//	//		rowFour[1] = "1.1";
//	//		rowFour[2] = "[0.1]";
//	//		rowFour[3] = new Double(15000);
//	//
//	//		test.add(rowOne);
//	//		test.add(rowTwo);
//	//		test.add(rowThree);
//	//		test.add(rowFour);
//	//
//	//		Map<String, Integer> namesMap = new HashMap<String, Integer>();
//	//
//	//		namesMap.put("System", 0);
//	//		namesMap.put("System Group", 1);
//	//		namesMap.put("System Group Dependencies", 2);
//	//		namesMap.put("System Budget", 3);
//	//		namesMap.put("Group Order", 4);
//	//
//	//		List<Object[]> results = orderBudgetList(test, namesMap);
//	//
//	//		for(Object[] row: results){
//	//			System.out.println("SYSTEM IS: "+row[0].toString());
//	//			if(row[2] != null){
//	//				System.out.println("SYSTEM DEPENDENCIES ARE: "+row[2].toString());
//	//			}
//	//			System.out.println("GROUP ORDER IS: "+row[4].toString());
//	//		}
//	//
//	//	}
//
//	@Override
//	public Hashtable getDataMakerOutput(String... selectors){
//		String playSheetClassName = PlaySheetRDFMapBasedEnum.getClassFromName("Grid");
//		GridPlaySheet playSheet = null;
//		try {
//			playSheet = (GridPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
//		} catch (ClassNotFoundException ex) {
//			ex.printStackTrace();
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
////		playSheet.setNames(this.names);
//		playSheet.setTitle(this.title);
//		playSheet.setQuestionID(this.questionNum);//
//		Hashtable retHash = (Hashtable) playSheet.getDataMakerOutput();
//		List<Object[]> myList = this.dataFrame.getData();
//		for(Object[] myRow : myList){
//			for(int i = 0; i < myRow.length; i++){
//				if(myRow[i] == null){
//					myRow[i] = "";
//				}
//				else {
//					myRow[i] = myRow[i].toString();
//				}
//			}
//		}
//		retHash.put("data", myList);
//		return retHash;
//	}
//	
//}
