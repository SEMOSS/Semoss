package prerna.ui.components.specific.ousd;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.ds.OrderedBTreeDataFrame;
import prerna.ui.components.playsheets.ColumnChartPlaySheet;

public class RoadmapCleanTableComparisonBarChartPlaySheet extends ColumnChartPlaySheet{

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
		this.dataFrame = new OrderedBTreeDataFrame(new String[] { "Year", this.comparatorName, this.roadmapName});


		createTable(yearList, timeline, fyList, comparatorTimeline, comparatorFyList, columns);
		processQueryData();
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

	private void createTable(List<Integer> yearList, OUSDTimeline timeline, List<Integer> fyList, OUSDTimeline comparatorTimeline, List<Integer> comparatorFyList, String[] columns){

		ArrayList<Object[]> templist = new ArrayList<Object[]>();
		
		String[] names = this.dataFrame.getColumnHeaders();
		List<String> processedSystems = new ArrayList<String>();
		List<String> comparatorProcessedSystems = new ArrayList<String>();

		List<Map<String, List<String>>> systemYears = timeline.getTimeData();
		Map<String, Double> budgets = timeline.getBudgetMap();
		Object[] row = new Object[yearList.size()+1];
		row[0] = this.roadmapName;

		List<Map<String, List<String>>> comparatorYears = comparatorTimeline.getTimeData();
		Map<String, Double> comparatorBudgets = comparatorTimeline.getBudgetMap();
		Object[] comparatorRow = new Object[yearList.size()+1];
		comparatorRow[0] = this.comparatorName;

		
		NumberFormat formatter = NumberFormat.getCurrencyInstance(Locale.US);
		
		for(int i = yearList.size(); i>0; i--){
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
				//String formattedTotal = formatter.format(yearTotal);
				row[i] = yearTotal;
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
				//String formattedTotal = formatter.format(yearTotal);
				comparatorRow[i] = yearTotal;
			}
		}
		templist.add(row);
		templist.add(comparatorRow);

		for (int i = 1; i < columns.length; i++){
			Object[] newRow = new Object[3];
			newRow[0] = yearList.get(i - 1);
			Double val1 = (Double) (templist.get(0)[i] == null? (Double) 0.0 : templist.get(0)[i]);
			Double val2 = (Double) (templist.get(1)[i] == null? (Double) 0.0 : templist.get(1)[i]);
			newRow[1] = val2;
			newRow[2] = val1;
			this.dataFrame.addRow(newRow, newRow);
		}

	}
}
