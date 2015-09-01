package prerna.ui.components.specific.ousd;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.ds.OrderedBTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;

public class RoadmapCleanTableComparisonPlaySheet extends GridPlaySheet{

	OUSDTimeline timeline;
	OUSDTimeline comparatorTimeline;
	String roadmapName;
	String comparatorName;

	@Override
	public void setQuery(String query){
		String delimiters = "[;]";
		String[] insights = query.split(delimiters);
		roadmapName = insights[0];
		comparatorName = insights[1];
	}

	@Override
	public void createData(){

		try{
			timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, roadmapName);
		}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
			e.printStackTrace();
		}

		try{
			comparatorTimeline = OUSDPlaysheetHelper.buildTimeline(this.engine, comparatorName);
		}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
			e.printStackTrace();
		}

		List<Integer> fyList = timeline.getFiscalYears();

		List<Integer> comparatorFyList = comparatorTimeline.getFiscalYears();

		List<Integer> yearList = new ArrayList<Integer>();
		yearList.addAll(fyList);

		for(Integer year: comparatorFyList){
			if(!yearList.contains(year)){
				yearList.add(year);
			}
		}
		Collections.sort(yearList);

		String[] columns = new String[yearList.size() + 1];
		columns[0] = "System";
		int count = 1;
		for (Integer year : yearList){
			columns[count] = year+"";
			count++;
		}
		this.dataFrame = new OrderedBTreeDataFrame(columns);

		createTable(yearList, timeline, fyList, comparatorTimeline, comparatorFyList);
	}

	@Override
	public Hashtable getData(){
		Hashtable<String, Object> map = (Hashtable<String, Object>) super.getData();
		//		map.put("data", this.timeline.getGanttData());
		//		map.put("headers", this.timeline.getGanttHeaders());
		map.put("data", this.timeline.getDashboardData());
		map.put("headers", new System[0]);
		return map;
	}

	private void createTable(List<Integer> yearList, OUSDTimeline timeline, List<Integer> fyList, OUSDTimeline comparatorTimeline, List<Integer> comparatorFyList){

		//		this.list = new ArrayList<Object[]>();
		String[] names = this.dataFrame.getColumnHeaders();
		List<String> processedSystems = new ArrayList<String>();
		List<String> comparatorProcessedSystems = new ArrayList<String>();

		List<Map<String, List<String>>> systemYears = timeline.getTimeData();
		Map<String, Double> budgets = timeline.getBudgetMap();
		Object[] row = new Object[names.length];
		row[0] = this.roadmapName + " Annual Savings";
		Object[] rowBuildCost = new Object[names.length];
		rowBuildCost[0] = this.roadmapName + " Build Cost";
		Object[] rowSustainCost = new Object[names.length];
		rowSustainCost[0] = this.roadmapName + " Sustainment Cost";

		List<Map<String, List<String>>> comparatorYears = comparatorTimeline.getTimeData();
		Map<String, Double> comparatorBudgets = comparatorTimeline.getBudgetMap();
		Object[] comparatorRow = new Object[names.length];
		comparatorRow[0] = this.comparatorName + " Annual Savings";
		Object[] compRowBuildCost = new Object[names.length];
		compRowBuildCost[0] = this.comparatorName + " Build Cost";
		Object[] compRowSustainCost = new Object[names.length];
		compRowSustainCost[0] = this.comparatorName + " Sustainment Cost";

		NumberFormat formatter = NumberFormat.getCurrencyInstance(Locale.US);
		NumberFormat percentFormat = NumberFormat.getPercentInstance();
		percentFormat.setMinimumFractionDigits(1);

		List<Map<String, Double>> invest = timeline.getSystemInvestmentMap();
		List<Map<String, Double>> investComparator = comparatorTimeline.getSystemInvestmentMap();

		List<Map<String, Double>> sustain = timeline.getInterfaceSustainmentMap();
		List<Map<String, Double>> sustainComparator = comparatorTimeline.getInterfaceSustainmentMap();

		for(int i = names.length-1; i>0; i--){
			int year = yearList.get(i-1);
			if(fyList.contains(year)){
				int yearIdx = timeline.getFyIndexFiscalYear(year);	
				double yearTotal = 0.0;

				Map<String, List<String>> yearMap = systemYears.get(yearIdx);
				for(String system: yearMap.keySet()){
					if(!processedSystems.contains(system)){
						yearTotal = yearTotal + budgets.get(system);
						processedSystems.add(system);
					}
				}
				String formattedTotal = formatter.format(yearTotal);
				row[i] = formattedTotal;

				if(invest!=null){
					Map<String, Double> yearInvest = invest.get(yearIdx);
					double totalInvest = 0.;
					for(Double val : yearInvest.values()){
						totalInvest = totalInvest + val;
					}
					rowBuildCost[i] = formatter.format(totalInvest);
				}
				if(sustain!=null){
					Map<String, Double> yearSustain = sustain.get(yearIdx);
					double totalSustain = 0.;
					for(Double val : yearSustain.values()){
						totalSustain = totalSustain + val;
					}
					rowSustainCost[i] = formatter.format(totalSustain);
				}
			}else{				
				int tempYear = year-1;
				while(!fyList.contains(tempYear) && tempYear >2000){
					tempYear--;
				}
				if(tempYear != 2000){
					int yearIdx = timeline.getFyIndexFiscalYear(tempYear);

					if(sustain!=null){
						Map<String, Double> yearSustain = sustain.get(yearIdx);
						double totalSustain = 0.;
						for(Double val : yearSustain.values()){
							totalSustain = totalSustain + val;
						}
						rowSustainCost[i] = formatter.format(totalSustain);
					}
				}
			}
			if(comparatorFyList.contains(year)){
				int yearIdx = comparatorTimeline.getFyIndexFiscalYear(year);	
				double yearTotal = 0.0;

				Map<String, List<String>> yearMap = comparatorYears.get(yearIdx);
				for(String system: yearMap.keySet()){
					if(!comparatorProcessedSystems.contains(system)){
						yearTotal = yearTotal + comparatorBudgets.get(system);
						comparatorProcessedSystems.add(system);
					}
				}
				String formattedTotal = formatter.format(yearTotal);
				comparatorRow[i] = formattedTotal;

				if(investComparator!=null){
					Map<String, Double> yearInvest = investComparator.get(yearIdx);
					double totalInvest = 0.;
					for(Double val : yearInvest.values()){
						totalInvest = totalInvest + val;
					}
					compRowBuildCost[i] = formatter.format(totalInvest);
				}
				if(sustainComparator!=null){
					Map<String, Double> yearSustain = sustainComparator.get(yearIdx);
					double totalSustain = 0.;
					for(Double val : yearSustain.values()){
						totalSustain = totalSustain + val;
					}
					compRowSustainCost[i] = formatter.format(totalSustain);
				}
			}else{				
				int tempYear = year-1;
				while(!comparatorFyList.contains(tempYear) && tempYear >2000){
					tempYear--;
				}
				if(tempYear != 2000){
					int yearIdx = comparatorTimeline.getFyIndexFiscalYear(tempYear);

					if(sustainComparator!=null){
						Map<String, Double> yearSustain = sustainComparator.get(yearIdx);
						double totalSustain = 0.;
						for(Double val : yearSustain.values()){
							totalSustain = totalSustain + val;
						}
						compRowSustainCost[i] = formatter.format(totalSustain);
					}
				}
			}			
		}
		this.dataFrame.addRow(row, row);
		this.dataFrame.addRow(rowBuildCost, rowBuildCost);
		this.dataFrame.addRow(rowSustainCost, rowSustainCost);

		this.dataFrame.addRow(comparatorRow, comparatorRow);
		this.dataFrame.addRow(compRowBuildCost, compRowBuildCost);
		this.dataFrame.addRow(compRowSustainCost, compRowSustainCost);

		additionalRowBuilder(comparatorRow, compRowSustainCost, compRowBuildCost, names, formatter, percentFormat, this.comparatorName);
	}

	private void additionalRowBuilder(Object[] baseRow, Object[] sustainRow, Object[] buildCostRow, String[] names, NumberFormat formatter, NumberFormat percentFormat, String name){

		double cumulativeCost=0.0;
		double cumulativeSavings=0.0;

		List<Double> annualSavings = new ArrayList<Double>();

		Object[] cumulativeSavingsRow = new Object[names.length];
		cumulativeSavingsRow[0] = name + " Cumulative Savings";
		Object[] cumulativeTotalCostRow = new Object[names.length];
		cumulativeTotalCostRow[0] = name + " Cumulative Cost";
		Object[] roiRow = new Object[names.length];
		roiRow[0] = name + " ROI";
		
		for(int i=2; i<names.length; i++){

			//cumulative savings
			if(baseRow[i] != null){
				String savings = baseRow[i].toString().replace("$", "").replace(",", "");
				annualSavings.add(Double.parseDouble(savings));				
			}
			for(Double value: annualSavings){
				cumulativeSavings = cumulativeSavings + value;
			}
			cumulativeSavingsRow[i] = formatter.format(cumulativeSavings);


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
			double roi = netSavings/investment;
			roiRow[i] = percentFormat.format(roi);
		}
		
		this.dataFrame.addRow(cumulativeSavingsRow, cumulativeSavingsRow);
		this.dataFrame.addRow(cumulativeTotalCostRow, cumulativeTotalCostRow);
		this.dataFrame.addRow(roiRow, roiRow);
	}
}
