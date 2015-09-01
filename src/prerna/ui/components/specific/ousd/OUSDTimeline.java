package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class OUSDTimeline {

	private static final Logger LOGGER = LogManager.getLogger(OUSDTimeline.class.getName());

	private List<Map<String, List<String>>> timeData = new ArrayList<Map<String, List<String>>>(); // a map for each year containing decomSystem -> list of systems replacing it
	private List<Integer> fyIndexArray = new ArrayList<Integer>(); // what fiscal years exist and what location they exist in in timeData
	private Map<String, List<List<String>>> systemDownstreamMap; // SystemName -> a list of pairs [downstreamSystemName, dataPassed]
	private Map<String, Double> systemBudgetMap = new HashMap<String, Double>(); //map of systems to their respective budgets.
	private Map<String, List<String>> dataSystemMap = new HashMap<String, List<String>>(); //map of data objects to supporting systems
	private Map<String, List<String>> granularBLUMap = new HashMap<String, List<String>>(); //map of granular blu to supporting systems
	private Map<String, List<String>> retirementMap = new HashMap<String, List<String>>(); //map of granular blu to supporting systems
	private List<Map<String, Double>> systemInvestmentMap;//map of year->map of system->investment amount (new interface cost)
	private List<Map<String, Double>> interfaceSustainmentMap;//map of year->map of system->investment amount (new interface cost)
	
	public List<Map<String, Double>> getInterfaceSustainmentMap() {
		return interfaceSustainmentMap;
	}

	public void setInterfaceSustainmentMap(
			List<Map<String, Double>> interfaceSustainmentMap) {
		this.interfaceSustainmentMap = interfaceSustainmentMap;
	}

	public void setSystemInvestmentMap (List<Map<String, Double>> investmentMap){
		this.systemInvestmentMap = investmentMap;
	}
	
	public List<Map<String, Double>> getSystemInvestmentMap(){
		return this.systemInvestmentMap;
	}
	
	public void setTimeData(List<Map<String, List<String>>> timeData){
		this.timeData = timeData;
	}

	public List<Map<String, List<String>>> getTimeData(){
		return this.timeData;
	}

	public Map<String, List<String>> getRetirementMap() {
		return retirementMap;
	}

	public void setRetirementMap(Map<String, List<String>> retirementMap) {
		this.retirementMap = retirementMap;
	}

	public void setFyIndexArray(List<Integer> fyIndexArray){
		this.fyIndexArray = fyIndexArray;
	}

	public List<Integer> getFyIdxArray(){
		return this.fyIndexArray;
	}
	
	public void setBudgetMap(Map<String, Double> budgetMap){
		this.systemBudgetMap = budgetMap;
	}
	
	public Map<String, Double> getBudgetMap(){
		return this.systemBudgetMap;
	}
	
	public void setSystemDownstream(Map<String, List<List<String>>> systemDownstreamMap){
		this.systemDownstreamMap = systemDownstreamMap;
	}
	
	public Map<String, List<List<String>>> getSystemDownstreamMap(){
		return this.systemDownstreamMap;
	}
	
	public void setGranularBLUMap(Map<String, List<String>> granularBLUMap){
		this.granularBLUMap = granularBLUMap;
	}

	public Map<String, List<String>> getGranularBLUMap(){
		return this.granularBLUMap;
	}
	
	public void setDataSystemMap(Map<String, List<String>> dataSystemMap) {
		this.dataSystemMap = dataSystemMap;
	}
	
	public Map<String, List<String>> getDataSystemMap() {
		return dataSystemMap;
	}
	
	public void setFyMap(Map<String, List<String>> timeData, Integer fy){
		int idx = getFyIndex(fy);
		this.timeData.remove(idx);
		this.timeData.add(idx, timeData);
	}

	public void addSystemTransition(Integer fy, String decomSys, String endureSys){
		LOGGER.info("Adding transition to timeline::::::::");
		LOGGER.info(decomSys + "  getting replaced by " + endureSys + "  in the year " + fy);
		int idx = getFyIndex(fy);
		Map<String, List<String>> fyMap = timeData.get(idx);
		List<String> endureSysList = new ArrayList<String>();
		if(fyMap.containsKey(decomSys)){
			endureSysList = fyMap.get(decomSys);
		}
		endureSysList.add(endureSys);
		fyMap.put(decomSys, endureSysList);
	}

	public void addSystemTransition(Integer fy, String decomSys, List<String> endureSys){
		LOGGER.info("Adding transition to timeline::::::::");
		LOGGER.info(decomSys + "  getting replaced by " + endureSys + "  in the year " + fy);
		int idx = getFyIndex(fy);
		Map<String, List<String>> fyMap = timeData.get(idx);
		List<String> endureSysList = new ArrayList<String>();
		if(fyMap.containsKey(decomSys)){
			endureSysList = fyMap.get(decomSys);
		}
		endureSysList.addAll(endureSys);
		fyMap.put(decomSys, endureSysList);
	}

	public int getFyIndex(Integer fy){
		if(fyIndexArray == null){
			LOGGER.error("Cannot add specific fy before setting fy index array");
			return -1;
		}
		int idx = fyIndexArray.indexOf(fy);
		return idx;
	}

	public List<Integer> getFiscalYears(){
		List<Integer> fiscalYears = new ArrayList<Integer>();
		if(!fyIndexArray.contains(1)){
			return this.fyIndexArray;
		}
		else {
			for(Integer year: fyIndexArray){
				fiscalYears.add((year+2014));
			}
			return fiscalYears;
		}
	}
	
	public int getFyIndexFiscalYear(Integer year){
		if(fyIndexArray == null){
			LOGGER.error("Cannot add specific fy before setting fy index array");
			return -1;
		}
		if(!fyIndexArray.contains(1)){
			return fyIndexArray.indexOf(year);
		}
		else {
			int idx = fyIndexArray.indexOf(year-2014);
			return idx;
		}
	}
	
	public List<String> getSystems(){
		List<String> systems = new ArrayList<String>();
		for(Map<String, List<String>> map: timeData){
			for(String key: map.keySet()){
				if(!systems.contains(key)){
					systems.add(key);
				}
			}
		}
		return systems;
	}

	public Integer getYearFromIdx(Integer index){
		return fyIndexArray.get(index);
	}
	
	public void insertFy(Integer fy){
		int i = 0;
		for (; i < fyIndexArray.size(); i++) {
			int value = fyIndexArray.get(i);
			if(value < fy){
				continue;
			}else if (value == fy){
				return;
			}
			break;
		}
		fyIndexArray.add(i, fy);
		timeData.add(i, new HashMap<String, List<String>>());
	}
	
	//Flattens all timeline data into most logical table
	//We may need variations on this for certain situations
	public void getTable(){
		//Iterate through main list
		//Make most logical table
		//Fist need to understand what data has been set in here so i can create header array
		List<String> names = new ArrayList<String>();
		names.add("Year");
		names.add("Decommissioned System");
		names.add("Replacing System");
		if(this.systemDownstreamMap!=null){
			names.add("Downstream System");
			names.add("Data Passed");
		}
		String[] tableNames = new String[names.size()];
		tableNames = names.toArray(tableNames);
		
		List<Integer> fys = this.fyIndexArray;
		if(fys == null){
			fys = new ArrayList<Integer>();
			for(int i = 0; i < this.timeData.size(); i ++){
				fys.add(i, i);
			}
		}
		
		for(int yearIdx = 0 ; yearIdx < fys.size(); yearIdx ++){
			
			//add basic system information
			Map<String, List<String>> fyMap = this.timeData.get(yearIdx);
			Iterator<String> sysIt = fyMap.keySet().iterator();
			
			// for each system getting decommissioned
			while(sysIt.hasNext()){
				String decomSystem = sysIt.next();
				// for each system replacing it
				List<String> replacingSystems = fyMap.get(decomSystem);
				for(String replacingSystem : replacingSystems){
					Object[] newRow = new Object[tableNames.length];
					newRow[0] = fys.get(yearIdx);
					newRow[1] = decomSystem;
					newRow[2] = replacingSystem;
					
					int offset = 0;
					// get any additional information we have about this system getting decommissioned
					if(this.systemDownstreamMap != null){
						flattenDownstreamData(newRow, offset, decomSystem);
					}
//					else if (budgetData != null){
//						flattenBudgetData(newRow, offset + 2, decomSystem);
//					}
				}
			}
		}
	}
	
	private void flattenDownstreamData(Object[] newRow, int offset, String decomSystem){
		List<List<String>> sysMap = this.systemDownstreamMap.get(decomSystem);
		if(sysMap != null) { // if we have downstream info for this specific system
			for(List<String> icd : sysMap){
				Object[] newRow2 = Arrays.copyOf(newRow, newRow.length);
				newRow2[3 + offset] = icd.get(0);
				newRow2[4 + offset] = icd.get(1);
//				if(budgetData != null){
//					flattenBudgetData(newRow2, offset + 2, decomSystem);
//				}
			}
		}
	}
	
	public List<Object[]> getGanttData(){
		List<Object[]> ganttData = new ArrayList<Object[]>();
		
		for(int fyIdx = 0; fyIdx <  this.timeData.size(); fyIdx ++ ){
			
			Map<String, List<String>> fyData = this.timeData.get(fyIdx);
			Integer year = this.fyIndexArray.get(fyIdx);
			
			Iterator<String> fyDataIt = fyData.keySet().iterator();
			while(fyDataIt.hasNext()){
				String decoSys = fyDataIt.next();
				List<String> targetSys = fyData.get(decoSys);
				for(String tSys : targetSys){
					Object[] row = new Object[4];
					row[0] = decoSys;
					row[1] = year;
					row[2] = year + 1;
					row[3] = tSys;
					ganttData.add(row);
				}
			}
		}
		
		return ganttData;
	}
	
	public String[] getGanttHeaders(){
		return new String[] {"Decommissioned System", "Start Transition Year", "End Transition Year", "Target System"};
	}
	
	public String getGanttTitle(){
		return "Transition Timeline";
	}
	
	public String getGanttPlaySheet(){
		return "OUSDGantt";
	}
	
	public List<Object[]> getCostSavingsData(){
		List<Object[]> costSavingsData = new ArrayList<Object[]>();
		
		for(int fyIdx = 0; fyIdx <  this.timeData.size(); fyIdx ++ ){
			
			Map<String, List<String>> fyData = this.timeData.get(fyIdx);
			Integer year = this.fyIndexArray.get(fyIdx);
			
			Iterator<String> fyDataIt = fyData.keySet().iterator();
			Double savings = 0.0;
			Double cost = 0.0;
			while(fyDataIt.hasNext()){
				String decoSys = fyDataIt.next();
				Double sysSavings = this.systemBudgetMap.get(decoSys);
				savings = savings + sysSavings;
				
				Double investmentCost = this.systemInvestmentMap.get(year).get(decoSys);
				cost = cost + investmentCost;
			}
			Object[] row = new Object[3];
			row[0] = year;
			row[1] = savings;
			row[2] = cost;
			costSavingsData.add(row);
		}
		
		return costSavingsData;
	}
	
	public String[] getCostSavingsHeaders(){
		return new String[] {"Transition Year", "Savings", "Investment Cost"};
	}
	
	public String getCostSavingsTitle(){
		return "Cost Savings";
	}
	
	public String getCostSavingsPlaySheet(){
		return "OUSDCombo";
	}
	
	public Map<String, Object> getDashboardData(){
		List<Map<String,Object>> charts = new ArrayList<Map<String,Object>>();
		
		// first to make the gantt chart data
		Map<String, Object> ganttMap = new HashMap<String, Object>();
		ganttMap.put("data", getGanttData());
		ganttMap.put("headers", getGanttHeaders());
		ganttMap.put("title", getGanttTitle());
		ganttMap.put("playsheet", getGanttPlaySheet());
		charts.add(ganttMap);
		
		// then to make the cost savings chart data
		Map<String, Object> costSavingsData = new HashMap<String, Object>();
		costSavingsData.put("data", getCostSavingsData());
		costSavingsData.put("headers", getCostSavingsHeaders());
		costSavingsData.put("title", getCostSavingsTitle());
		costSavingsData.put("playsheet", getCostSavingsPlaySheet());
		charts.add(costSavingsData);
		
		// then to layout the joins.
		List<String[]> joins = new ArrayList<String[]>();
		joins.add(new String[] {"Tranisition Year" , "Transition Year"});

		Map<String, Object> vizMap = new HashMap<String, Object>();
		vizMap.put("charts", charts);
		vizMap.put("joins", joins);
		
		return vizMap;
	}
	
}
