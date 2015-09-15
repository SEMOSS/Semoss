package prerna.ui.components.specific.ousd;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class OUSDTimeline {

	// NAMING
	protected final String decomCount = "System Decommission Count";
	protected final String savingThisYear = "New Savings this year";
	protected final String buildCount = "New Interface Count";
	protected final String investmentCost = "Interface Development Cost";
	protected final String sustainCost = "Interface Sustainment Cost";
	protected final String risk = "Enterprise Risk";
	
	protected final String cumSavings = "Cumulative Savings";
	protected final String prevSavings = "Previous Decommissioning Savings";
	protected final String cumCost = "Cumulative Cost";
	protected final String roi = "ROI";
	protected final String opCost = "Operational Cost";
	
	private static final Logger LOGGER = LogManager.getLogger(OUSDTimeline.class.getName());

	private List<Map<String, List<String>>> timeData = new ArrayList<Map<String, List<String>>>(); // a map for each year containing decomSystem -> list of systems replacing it
	private List<Integer> fyIndexArray = new ArrayList<Integer>(); // what fiscal years exist and what location they exist in in timeData
	private Map<String, List<List<String>>> systemDownstreamMap = new HashMap<String, List<List<String>>>(); // SystemName -> a list of pairs [downstreamSystemName, dataPassed]
	private Map<String, Double> systemBudgetMap = new HashMap<String, Double>(); //map of systems to their respective budgets.
	private Map<String, List<String>> dataSystemMap = new HashMap<String, List<String>>(); //map of data objects to supporting systems
	private Map<String, List<String>> granularBLUMap = new HashMap<String, List<String>>(); //map of granular blu to supporting systems
	private Map<String, List<String>> retirementMap = new HashMap<String, List<String>>(); //map of granular blu to supporting systems
	private List<Map<String, Double>> systemInvestmentMap;//map of year->map of system->investment amount (new interface cost)
	private List<Map<String, Double>> interfaceSustainmentMap;//map of year->map of system->investment amount (new interface cost)
	private List<Double> treeMaxList;
	private List<Object[]> outputLists = new ArrayList<Object[]>();
	private Map<String, List<String>> targetMap = new HashMap<String, List<String>>();	

	public void verifyYears(){
		for(int i = 0; i< fyIndexArray.size()-1; i++){
			if(fyIndexArray.get(i) != fyIndexArray.get(i+1)-1){
				fyIndexArray.add(i+1, fyIndexArray.get(i)+1);
				timeData.add(i+1,  new HashMap<String, List<String>>());
				systemInvestmentMap.add(i+1, new HashMap<String, Double>());
				interfaceSustainmentMap.add(i+1, new HashMap<String, Double>());
				treeMaxList.add(i+1, 0.0);
			}
		}
	}

	public List<Object[]> getOutputLists() {
		return outputLists;
	}
	
	public void setOutputLists(List<Object[]> outputLists) {
		this.outputLists = outputLists;
	}
	
	public Map<String, List<String>> getTargetMap() {
		return targetMap;
	}

	public void setTargetMap(Map<String, List<String>> targetList) {
		this.targetMap = targetList;
	}

	public List<Double> getTreeMaxList() {
		return treeMaxList;
	}

	public void setTreeMaxList(List<Double> treeMaxList) {
		this.treeMaxList = treeMaxList;
	}

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

	public void updateTargetSystems(){
		if(!targetMap.isEmpty() && !timeData.isEmpty()){
			for(Map<String, List<String>> yearData: timeData){
				for(String system: yearData.keySet()){
					if(targetMap.keySet().contains(system)){
						List<String> targets = new ArrayList<String>();
						for(String target: targetMap.get(system)){
							targets.add(target);
						}
						yearData.put(system, targets);
					}else{
						yearData.put(system, new ArrayList<String>());
					}
				}
			}
		}
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
		List<String> usedSystems = new ArrayList<String>(); // keeps track for partial decommissioning
		Integer year = 0;
		Double newCost = 0.0;
		for(int fyIdx = this.timeData.size() - 1; fyIdx >= 0  ; fyIdx -- ){

			Map<String, List<String>> fyData = this.timeData.get(fyIdx);
			year = this.fyIndexArray.get(fyIdx);

			Iterator<String> fyDataIt = fyData.keySet().iterator();
			Double savings = 0.0;
			Double cost = 0.0;
			while(fyDataIt.hasNext()){
				String decoSys = fyDataIt.next();
				if(!usedSystems.contains(decoSys)){
					Double sysSavings = this.systemBudgetMap.get(decoSys);
					savings = savings + sysSavings;
	
					if(this.systemInvestmentMap != null && this.systemInvestmentMap.size()>fyIdx){
						Double investmentCost = this.systemInvestmentMap.get(fyIdx).get(decoSys);
						if(investmentCost!=null) {
							cost = cost + investmentCost;
						}
					}
					usedSystems.add(decoSys);
				}
				else {
					LOGGER.info("SKIPPING SYSTEM BECAUSE PARTIAL DECOMMISSIONING :::::" + decoSys);
				}
			}
			Object[] row = new Object[4];
			row[0] = year + 1;
			row[1] = savings;
			row[2] = newCost;
			row[3] = savings - newCost;
			costSavingsData.add(0, row);
			newCost = cost;
		}
		// add first year row
		Object[] row = new Object[4];
		row[0] = year;
		row[1] = 0.0;
		row[2] = newCost;
		row[3] = 0.0 - newCost;
		costSavingsData.add(0, row);

		return costSavingsData;
	}

	public String[] getCostSavingsHeaders(){
		return new String[] {"Transition Year", "New Savings", "Interface Development Cost", "Net Annual Savings"};
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

	public void buildData(List<Integer> yearList, String[] columns, String name){

		List<String> processedSystems = new ArrayList<String>();

		List<Map<String, List<String>>> systemYears = this.getTimeData();
		Map<String, Double> budgets = this.getBudgetMap();
		Object[] rowSystemCount = new Object[columns.length];
		rowSystemCount[0] = name + this.decomCount;
		Object[] row = new Object[columns.length];
		row[0] = name + this.savingThisYear;
		Object[] rowBuildCount = new Object[columns.length];
		rowBuildCount[0] = name + this.buildCount;
		Object[] rowBuildCost = new Object[columns.length];
		rowBuildCost[0] = name + this.investmentCost;
		Object[] rowSustainCost = new Object[columns.length];
		rowSustainCost[0] = name + this.sustainCost;
		Object[] rowRisk = new Object[columns.length];
		rowRisk[0] = name + this.risk;

		NumberFormat formatter = NumberFormat.getCurrencyInstance(Locale.US);
		NumberFormat percentFormat = NumberFormat.getPercentInstance();
		percentFormat.setMinimumFractionDigits(1);

		List<Map<String, Double>> invest = this.getSystemInvestmentMap();

		List<Map<String, Double>> sustainMap = this.getInterfaceSustainmentMap();

		for(int i = columns.length-1; i>0; i--){
			int year = yearList.get(i-1);
			if(this.getFiscalYears().contains(year)){
				int yearIdx = this.getFyIndexFiscalYear(year);	
				double yearTotal = 0.0;

				Map<String, List<String>> yearMap = systemYears.get(yearIdx);
				int procCount = 0;
				for(String system: yearMap.keySet()){
					if(!processedSystems.contains(system)){
						procCount++;
						yearTotal = yearTotal + budgets.get(system);
						processedSystems.add(system);
					}
				}
				rowSystemCount[i]=procCount;
				String formattedTotal = formatter.format(yearTotal);
				if(year == this.getFiscalYears().get(0)){
					row[i] = formatter.format(0.0);
				}
				row[i+1] = formattedTotal;

				if(invest!=null){
					double buildCount = 0;
					Map<String, Double> yearInvest = invest.get(yearIdx);
					double totalInvest = 0.;
					for(Double val : yearInvest.values()){
						double intCount = val/350000;
						buildCount = buildCount + intCount;
						totalInvest = totalInvest + val;
					}
					rowBuildCount[i] = (int) buildCount;
					rowBuildCost[i] = formatter.format(totalInvest);
				}
				if(sustainMap!=null){
					Map<String, Double> yearSustain = sustainMap.get(yearIdx);
					double totalSustain = 0.;
					for(Double val : yearSustain.values()){
						totalSustain = totalSustain + val;
					}
					rowSustainCost[i] = formatter.format(totalSustain);
				}

				if(this.getTreeMaxList() != null){
					rowRisk[i] = percentFormat.format(this.getTreeMaxList().get(yearIdx));					
				}else{
					rowRisk[i] = "N/A";
				}
			}else{				
				if(year>this.getFiscalYears().get(this.getFiscalYears().size()-1)){
					if(sustainMap!=null){
						Map<String, Double> yearSustain = sustainMap.get(sustainMap.size()-1);
						double totalSustain = 0.;
						for(Double val : yearSustain.values()){
							totalSustain = totalSustain + val;
						}
						rowSustainCost[i] = formatter.format(totalSustain);
					}
				}
			}
		}
		
		outputLists.add(rowSystemCount);
		outputLists.add(row);
		outputLists.add(rowBuildCount);
		outputLists.add(rowBuildCost);
		outputLists.add(rowSustainCost);
		outputLists.add(rowRisk);
		
		additionalRowBuilder(yearList, row, rowSustainCost, rowBuildCost, columns, formatter, percentFormat, name);

	}
	
	private void additionalRowBuilder(List<Integer> yearList, Object[] baseRow, Object[] sustainRow, Object[] buildCostRow, String[] names, NumberFormat formatter, NumberFormat percentFormat, String name){

		double cumulativeCost=0.0;
		double cumulativeSavings=0.0;

		List<Double> annualSavings = new ArrayList<Double>();

		Map<String, Double> timelineBudgets = this.systemBudgetMap;
		double totalBudget = 0.0;
		//		for(Map<String, List<String>> year: timeline.getTimeData()){
		//			for(String key: year.keySet()){
		for(String system: timelineBudgets.keySet()){
			totalBudget = totalBudget + timelineBudgets.get(system);
		}

		Object[] cumulativeSavingsRow = new Object[names.length];
		cumulativeSavingsRow[0] = name + this.cumSavings;
		Object[] savingsRow = new Object[names.length];
		savingsRow[0] = name + this.prevSavings;
		Object[] cumulativeTotalCostRow = new Object[names.length];
		cumulativeTotalCostRow[0] = name + this.cumCost;
		Object[] roiRow = new Object[names.length];
		roiRow[0] = name + this.roi;
		Object[] remainingSystemBudgets = new Object[names.length];
		remainingSystemBudgets[0] = this.opCost;

		for(int i=1; i<names.length; i++){

			double year = Double.parseDouble(names[i].toString());
			if(year<yearList.get(0)){
				continue;
			}

			//cumulative savings
			if(baseRow[i] != null){
				String savings = baseRow[i].toString().replace("$", "").replace(",", "");
				annualSavings.add(Double.parseDouble(savings));
				totalBudget = totalBudget - Double.parseDouble(savings);
			}
			remainingSystemBudgets[i] = formatter.format(totalBudget);
			for(Double value: annualSavings){
				cumulativeSavings = cumulativeSavings + value;
			}
			cumulativeSavingsRow[i] = formatter.format(cumulativeSavings);							

			if(cumulativeSavingsRow[i-1] != null && cumulativeSavingsRow[i-1].toString().contains("$")){
				savingsRow[i] = Double.parseDouble(cumulativeSavingsRow[i].toString().replace("$", "").replace(",", ""))
						-Double.parseDouble(cumulativeSavingsRow[i-1].toString().replace("$", "").replace(",", ""));
				if(baseRow[i] != null){
					savingsRow[i] = (Double)savingsRow[i] - Double.parseDouble(baseRow[i].toString().replace("$", "").replace(",", ""));	
				}
				savingsRow[i] = formatter.format(savingsRow[i]);
			}

			//row sustainment cost and cumulative total cost
			if(sustainRow[i] != null){
				String sustainmentCost = sustainRow[i].toString().replace("$", "").replace(",", "");
				cumulativeCost = cumulativeCost + Double.parseDouble(sustainmentCost);
			}
			if(buildCostRow[i] != null){
				String cost = buildCostRow[i].toString().replace("$", "").replace(",", "");
				cumulativeCost = cumulativeCost + Double.parseDouble(cost);
			}
			cumulativeTotalCostRow[i] = formatter.format(cumulativeCost);


			//ROI value for each year
			double netSavings = 0.0;
			double investment = 0.0;
			if(cumulativeTotalCostRow[i] != null){
				String cost = cumulativeTotalCostRow[i].toString().replace("$", "").replace(",", "");
				investment = Double.parseDouble(cost);
			}
			if(cumulativeSavingsRow[i] != null){
				String savings = cumulativeSavingsRow[i].toString().replace("$", "").replace(",", "");
				netSavings = Double.parseDouble(savings) - investment; 
			}
			if(investment != 0){
				double roi = netSavings/investment;
				roiRow[i] = percentFormat.format(roi);
			}else{
				roiRow[i] = percentFormat.format(0);
			}


		}
		outputLists.add(savingsRow);
		outputLists.add(cumulativeSavingsRow);
		outputLists.add(cumulativeTotalCostRow);
		outputLists.add(roiRow);
		outputLists.add(remainingSystemBudgets);
	}
}
