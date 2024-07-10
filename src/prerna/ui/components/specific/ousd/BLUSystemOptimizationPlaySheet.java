//package prerna.ui.components.specific.ousd;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import lpsolve.LpSolveException;
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.ds.h2.H2Frame;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.ui.components.playsheets.GridPlaySheet;
//
//public class BLUSystemOptimizationPlaySheet extends GridPlaySheet{
//
//	protected static final Logger LOGGER = LogManager.getLogger(BLUSystemOptimizationPlaySheet.class.getName());
//	
//	String sysGroupingInsight;
//	String systemColumnName = "System";
//	
//	public BLUSystemOptimizationPlaySheet(){
//		super();
//	}
//	
//	@Override
//	public void setQuery(String query){
//		String delimiters = "[,]";
//		String[] parts = query.split(delimiters);
//		sysGroupingInsight = parts[0];
//		this.query = parts[1];
//	}
//	
//	@Override
//	public void createData(){
////		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
////		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
////		proc.processQuestionQuery(this.engine, sysGroupingInsight, emptyTable);
////		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();
//		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) OUSDPlaysheetHelper.getPlaySheetFromName(sysGroupingInsight, this.engine);
//
//		//createData makes the table...
//		activitySheet.createData();
//		List<Object[]> sysGroupTable = activitySheet.getList();
//		String[] names = activitySheet.getNames();
//		int sysIdx = -1;
//		int groupIdx = -1;
//		for(int i = 0; i <names.length; i++){
//			String name = names[i];
//			if(name.equals("System")){
//				sysIdx = i;
//			}
//			else if(name.equals("System Group")){
//				groupIdx = i;
//			}
//		}
//		Object[] waveReturn = createSysWaveMap(sysGroupTable, sysIdx, groupIdx);
//		Map<String, Integer> waveHash = (Map<String, Integer>) waveReturn[0];
//		String[] sysList = (String[]) waveReturn[1];
//		String sysBindingsString = (String) waveReturn[2];
//		
//		Object[] sysBudget = getBudgetData(sysList);
//		// run system blu query
//		this.query = this.query.replaceAll("!SYSTEMS!", sysBindingsString);
//		super.createData();
//		
//		BLUSystemOptimizer opt = new BLUSystemOptimizer();
//		opt.setSystemData(sysList, (Double[])sysBudget[0], (Double)sysBudget[1], this.dataFrame.getData(), waveHash);
//		try {
//			opt.setupModel();
//		} catch (LpSolveException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		opt.execute();
//		List<String> keptSystems = opt.getKeptSystems();
//		List<String> bluSystemsPlusSupporters = getSupportingSystems(keptSystems);
//		
//		addResultsToTable(keptSystems);
//		
//		opt.deleteModel();
//	}
//	
//	private void addResultsToTable(List<String> keptSystems){
//		ArrayList<Object[]> newList = new ArrayList<Object[]>();
//		
//		int sysCol = 0;
//		String[] names = this.dataFrame.getColumnHeaders();
//		for(String colHeader: names){
//			if(colHeader.equals(this.systemColumnName)) {
//				break;
//			}
//			sysCol++;
//		}
//		
//		String[] newNames = new String[names.length+1];
//		for(int i = 0; i < names.length; i++){
//			newNames[i] = names[i];
//		}
//		newNames[names.length] = "Kept";
//		ITableDataFrame newFrame = new H2Frame(newNames);
//		
////		for(Object[] curRow : this.list){
//		Iterator<IHeadersDataRow> rowIt = this.dataFrame.iterator();
//		while (rowIt.hasNext()){
//			Object[] curRow = rowIt.next().getValues();
//			Object[] newRow = new Object[curRow.length+1];
//			String sysName = curRow[sysCol] + "";
//			int i = 0;
//			for( ; i < curRow.length; i++){
//				newRow[i] = curRow[i];
//			}
//			boolean kept = keptSystems.contains(sysName);
//			newRow[i] = kept;
//			newFrame.addRow(newRow, newNames);
//		}
//		this.dataFrame = newFrame;
//	}
//	
//	private List<String> getSupportingSystems(List<String> bluSystems){
//		
//		
//		return new ArrayList<String>();
//	}
//	
//	private Object[] getBudgetData(String[] sysList){
//		//retrieve the budget numbers from the helper class
//		Map<String, Double> budgetMap = OUSDQueryHelper.getBudgetData(this.engine, null);
//
//		Double[] budList = new Double[sysList.length];
//		double maxBudget = -1.;
//		for(int i = 0; i<sysList.length; i++){
//			String sys = sysList[i];
//			Double budget = budgetMap.get(sys); 
//			if(budget == null){
//				System.err.println("MISSING BUDGET FOR SYSTEM :::::::: " + sys + " . SETTING TO 1000000000");
//				budget = 1000000000.;
//			}
//			else{
//				System.out.println("got budget data!  :::::::: " + sys + " . it is " + budget);
//			}
//			budList[i] = budget;
//			if(budget > maxBudget){
//				maxBudget = budget;
//			}
//		}
//		
//		Object[] retArray = new Object[]{budList, maxBudget};
//		return retArray;
//	}
//
//	private Object[] createSysWaveMap(List<Object[]> table, int sysIdx, int groupIdx){
//		String sysBindingsString = "";
//		Map<String, Integer> retMap = new HashMap<String, Integer>();
//		List<String> sysArray = new ArrayList<String>();
//		for(Object[] row: table){
//			String sys = (String) row[sysIdx];
//			if(!sysArray.contains(sys)){
//				sysBindingsString = sysBindingsString + "(<http://semoss.org/ontologies/Concept/System/" + sys + ">)";
//				sysArray.add(sys);
//				String doubleStr = row[groupIdx]+"";
//				String intStr = doubleStr.substring(0, (row[groupIdx] + "").indexOf("."));
//				retMap.put(sys, Integer.parseInt(intStr)); 
//			}
//		}
//		String[] systems = new String[sysArray.size()];
//		systems = sysArray.toArray(systems);
//		
//		return new Object[]{retMap, systems, sysBindingsString};
//	}
//}
