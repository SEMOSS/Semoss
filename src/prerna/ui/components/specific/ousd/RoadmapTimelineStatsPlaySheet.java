package prerna.ui.components.specific.ousd;

import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.OrderedBTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.PlaySheetEnum;

public class RoadmapTimelineStatsPlaySheet extends GridPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(RoadmapTimelineStatsPlaySheet.class.getName());
	OUSDTimeline timeline;
	String roadmapName;
	String owner;
	
	// NAMING
	protected final String decomCount = "System Decommission Count";
	protected final String savingThisYear = "New Savings";
	protected final String buildCount = "New Interface Count";
	protected final String investmentCost = "Interface Development Cost";
	protected final String sustainCost = "Interface Sustainment Cost";
	protected final String risk = "Enterprise Risk";
	
	protected final String cumSavings = "Cumulative Savings";
	protected final String prevSavings = "Previous Decommissioning Savings";
	protected final String cumCost = "Cumulative Cost";
	protected final String roi = "ROI";
	protected final String opCost = "Operational Cost";

	@Override
	public void setQuery(String query){
		String delimiters = "[;]";
		String[] insights = query.split(delimiters);
		roadmapName = insights[0];
		if(insights.length>1){
			owner = insights[1];
		}
	}

	@Override
	public void createData(){

		try{
			if(owner == null){
				owner = "DFAS";
				timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, roadmapName, owner);				
			}else{
				timeline = OUSDPlaysheetHelper.buildTimeline(engine, roadmapName, owner);
			}
		}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
			e.printStackTrace();
		}

		List<Integer> fyList = timeline.getFiscalYears();

		List<Integer> yearList = new ArrayList<Integer>();
		yearList.addAll(fyList);

		Collections.sort(yearList);

		if(yearList.size()!=0){
			while(yearList.get(yearList.size()-1)<2021){
				yearList.add(yearList.get(yearList.size()-1)+1);
			}
		}

		String[] columns = new String[yearList.size() + 1];
		columns[0] = "System";
		int count = 1;
		for (Integer year : yearList){
			columns[count] = year+"";
			count++;
		}

		this.dataFrame = new OrderedBTreeDataFrame(columns);

		createTable(yearList, timeline, fyList);
	}

	@Override
	public Hashtable getData(){
		String playSheetClassName = PlaySheetEnum.getClassFromName("Grid");
		GridPlaySheet playSheet = null;
		try {
			playSheet = (GridPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
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
		playSheet.setTitle(this.title);
		playSheet.setQuestionID(this.questionNum);
		playSheet.setDataFrame(this.dataFrame);
		Hashtable retHash = (Hashtable) playSheet.getData();
		return retHash;
	}

	private void createTable(List<Integer> yearList, OUSDTimeline timeline, List<Integer> fyList){

		//		this.list = new ArrayList<Object[]>();
		String[] names = this.dataFrame.getColumnHeaders();
		List<String> processedSystems = new ArrayList<String>();

		List<Map<String, List<String>>> systemYears = timeline.getTimeData();
		Map<String, Double> budgets = timeline.getBudgetMap();
		Object[] rowSystemCount = new Object[names.length];
		rowSystemCount[0] = this.decomCount;
		Object[] row = new Object[names.length];
		row[0] = this.savingThisYear;
		Object[] rowBuildCount = new Object[names.length];
		rowBuildCount[0] = this.buildCount;
		Object[] rowBuildCost = new Object[names.length];
		rowBuildCost[0] = this.investmentCost;
		Object[] rowSustainCost = new Object[names.length];
		rowSustainCost[0] = this.sustainCost;
		Object[] rowRisk = new Object[names.length];
		rowRisk[0] = this.risk;

		NumberFormat formatter = NumberFormat.getCurrencyInstance(Locale.US);
		NumberFormat percentFormat = NumberFormat.getPercentInstance();
		percentFormat.setMinimumFractionDigits(1);

		List<Map<String, Double>> invest = timeline.getSystemInvestmentMap();

		List<Map<String, Double>> sustainMap = timeline.getInterfaceSustainmentMap();

		for(int i = names.length-1; i>0; i--){
			int year = yearList.get(i-1);
			if(fyList.contains(year)){
				int yearIdx = timeline.getFyIndexFiscalYear(year);	
				double yearTotal = 0.0;

				Map<String, List<String>> yearMap = systemYears.get(yearIdx);
				int count = 0;
				for(String system: yearMap.keySet()){
					if(!processedSystems.contains(system)){
						count++;
						yearTotal = yearTotal + budgets.get(system);
						processedSystems.add(system);
					}
				}
				rowSystemCount[i]=count;
				String formattedTotal = formatter.format(yearTotal);
				if(year == fyList.get(0)){
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

				if(timeline.getTreeMaxList() != null){
					rowRisk[i] = percentFormat.format(timeline.getTreeMaxList().get(yearIdx));					
				}else{
					rowRisk[i] = "N/A";
				}
			}else{				
				if(year>fyList.get(fyList.size()-1)){
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

		this.dataFrame.addRow(rowSystemCount, rowSystemCount);
		this.dataFrame.addRow(row, row);
		this.dataFrame.addRow(rowBuildCount, rowBuildCount);
		this.dataFrame.addRow(rowBuildCost, rowBuildCost);
		this.dataFrame.addRow(rowSustainCost, rowSustainCost);
		this.dataFrame.addRow(rowRisk, rowRisk);


		additionalRowBuilder(timeline, fyList, row, rowSustainCost, rowBuildCost, names, formatter, percentFormat, this.roadmapName);
	}

	private void additionalRowBuilder(OUSDTimeline timeline, List<Integer> yearList, Object[] baseRow, Object[] sustainRow, Object[] buildCostRow, String[] names, NumberFormat formatter, NumberFormat percentFormat, String name){

		double cumulativeCost=0.0;
		double cumulativeSavings=0.0;

		List<Double> annualSavings = new ArrayList<Double>();

		Map<String, Double> timelineBudgets = timeline.getBudgetMap();
		double totalBudget = 0.0;
		//		for(Map<String, List<String>> year: timeline.getTimeData()){
		//			for(String key: year.keySet()){
		for(String system: timelineBudgets.keySet()){
			totalBudget = totalBudget + timelineBudgets.get(system);
		}

		Object[] cumulativeSavingsRow = new Object[names.length];
		cumulativeSavingsRow[0] = this.cumSavings;
		Object[] savingsRow = new Object[names.length];
		savingsRow[0] = this.prevSavings;
		Object[] cumulativeTotalCostRow = new Object[names.length];
		cumulativeTotalCostRow[0] = this.cumCost;
		Object[] roiRow = new Object[names.length];
		roiRow[0] = this.roi;
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
		this.dataFrame.addRow(savingsRow, savingsRow);
		this.dataFrame.addRow(cumulativeSavingsRow, cumulativeSavingsRow);
		this.dataFrame.addRow(cumulativeTotalCostRow, cumulativeTotalCostRow);
		this.dataFrame.addRow(roiRow, roiRow);
		this.dataFrame.addRow(remainingSystemBudgets, remainingSystemBudgets);

		addTeaserRows(names);
	}
	
	private void addTeaserRows(String[] names){

		Object[] migration = new Object[names.length];
		migration[0] = "Data Migration";
		fillWithTBD(migration);
		this.dataFrame.addRow(migration, migration);

		Object[] hw = new Object[names.length];
		hw[0] = "Hardware Plus Ups";
		fillWithTBD(hw);
		this.dataFrame.addRow(hw, hw);

		Object[] sw = new Object[names.length];
		sw[0] = "Software Plus Ups";
		fillWithTBD(sw);
		this.dataFrame.addRow(sw, sw);

		Object[] people = new Object[names.length];
		people[0] = "People Training";
		fillWithTBD(people);
		this.dataFrame.addRow(people, people);

		Object[] infra = new Object[names.length];
		infra[0] = "Infrastructure";
		fillWithTBD(infra);
		this.dataFrame.addRow(infra, infra);
	}
	
	private void fillWithTBD(Object[] row){
		for(int i = 0; i <row.length ; i++){
			if(row[i] == null){
				row[i] = "TBD";
			}
		}
	}
}
