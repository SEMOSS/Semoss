package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GridPlaySheet;

public class BLUSystemOptimizationPlaySheet extends GridPlaySheet{

	protected static final Logger LOGGER = LogManager.getLogger(BLUSystemOptimizationPlaySheet.class.getName());
	
	String sysGroupingInsight;
	
	public BLUSystemOptimizationPlaySheet(){
		super();
	}
	
	@Override
	public void setQuery(String query){
		String delimiters = "[,]";
		String[] parts = query.split(delimiters);
		sysGroupingInsight = parts[0];
		this.query = parts[1];
	}
	
	@Override
	public void createData(){
		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
		proc.processQuestionQuery(this.engine, sysGroupingInsight, emptyTable);
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();

		//createData makes the table...
		activitySheet.createData();
		ITableDataFrame frame =  activitySheet.getDataFrame();
		List<Object[]> sysGroupTable = frame.getData();
		String[] names = activitySheet.getNames();
		int sysIdx = -1;
		int groupIdx = -1;
		for(int i = 0; i <names.length; i++){
			String name = names[i];
			if(name.equals("System")){
				sysIdx = i;
			}
			else if(name.equals("System Group")){
				groupIdx = i;
			}
		}
		Object[] waveReturn = createSysWaveMap(sysGroupTable, sysIdx, groupIdx);
		Map<String, Integer> waveHash = (Map<String, Integer>) waveReturn[0];
		String[] sysList = (String[]) waveReturn[1];
		String sysBindingsString = (String) waveReturn[2];
		
		Object[] sysBudget = getBudgetData(sysList);
		// run system blu query
		this.query = this.query.replaceAll("!SYSTEMS!", sysBindingsString);
		super.createData();
		
		BLUSystemOptimizer opt = new BLUSystemOptimizer();
		opt.setSystemData(sysList, (Double[])sysBudget[0], (Double)sysBudget[1], this.dataFrame.getData(), waveHash);
		try {
			opt.setupModel();
		} catch (LpSolveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		opt.execute();
		
		opt.deleteModel();
	}
	
	private Object[] getBudgetData(String[] sysList){
		Map<String, Double> budgetMap = OUSDPlaysheetHelper.getBudgetData();

		Double[] budList = new Double[sysList.length];
		double maxBudget = -1.;
		for(int i = 0; i<sysList.length; i++){
			String sys = sysList[i];
			Double budget = budgetMap.get(sys); 
			if(budget == null){
				System.err.println("MISSING BUDGET FOR SYSTEM :::::::: " + sys + " . SETTING TO 1000000000");
				budget = 1000000000.;
			}
			else{
				System.out.println("got budget data!  :::::::: " + sys + " . it is " + budget);
			}
			budList[i] = budget;
			if(budget > maxBudget){
				maxBudget = budget;
			}
		}
//		
//		Double[] budList = new Double[budgetMap.size()];
//		Iterator<String> sysIt = budgetMap.keySet().iterator();
//		int idx = 0;
//		while(sysIt.hasNext()){
//			String sys = sysIt.next();
//			sysList[idx] = sys;
//			double budget =budgetMap.get(sys); 
//			budList[idx] = budget;
//			if(budget > maxBudget){
//				maxBudget = budget;
//			}
//			idx++;
//		}
		
		Object[] retArray = new Object[]{budList, maxBudget};
		return retArray;
	}

	private Object[] createSysWaveMap(List<Object[]> table, int sysIdx, int groupIdx){
		String sysBindingsString = "";
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		List<String> sysArray = new ArrayList<String>();
		for(Object[] row: table){
			String sys = (String) row[sysIdx];
			if(!sysArray.contains(sys)){
				sysBindingsString = sysBindingsString + "(<http://semoss.org/ontologies/Concept/System/" + sys + ">)";
				sysArray.add(sys);
				String doubleStr = row[groupIdx]+"";
				String intStr = doubleStr.substring(0, (row[groupIdx] + "").indexOf("."));
				retMap.put(sys, Integer.parseInt(intStr)); 
			}
		}
		String[] systems = new String[sysArray.size()];
		systems = sysArray.toArray(systems);
		
		return new Object[]{retMap, systems, sysBindingsString};
	}
}
