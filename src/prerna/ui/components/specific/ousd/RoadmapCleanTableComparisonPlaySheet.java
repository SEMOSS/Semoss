package prerna.ui.components.specific.ousd;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.ds.BTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;

public class RoadmapCleanTableComparisonPlaySheet extends GridPlaySheet{

	OUSDTimeline timeline;
	OUSDTimeline comparatorTimeline;
	String roadmapName;
	String comparatorName;
	
	boolean timelineShift = false;
	boolean comparatorShift = false;

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

		List<Integer> fyList = timeline.getFyIdxArray();
		if(fyList.get(0)==1){
			fyList = timeline.getFiscalYears();
			timelineShift = true;
		}
		List<Integer> comparatorFyList = comparatorTimeline.getFyIdxArray();
		if(comparatorFyList.get(0)==1){
			comparatorFyList = comparatorTimeline.getFiscalYears();
			comparatorShift = true;
		}
		
		List<Integer> yearList = new ArrayList<Integer>();
		yearList.addAll(fyList);
		
		for(Integer year: comparatorFyList){
			if(!yearList.contains(year)){
				yearList.add(year);
			}
		}
		Collections.sort(yearList);
		
		String[] columns = new String[fyList.size() + 1];
		columns[0] = "System";
		int count = 1;
		for (Integer year : yearList){
			columns[count] = year+"";
			count++;
		}
		this.dataFrame = new BTreeDataFrame(columns);

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
		row[0] = this.roadmapName;

		List<Map<String, List<String>>> comparatorYears = comparatorTimeline.getTimeData();
		Map<String, Double> comparatorBudgets = comparatorTimeline.getBudgetMap();
		Object[] comparatorRow = new Object[names.length];
		comparatorRow[0] = this.comparatorName;

		
		NumberFormat formatter = NumberFormat.getCurrencyInstance(Locale.US);
		
		for(int i = names.length-1; i>0; i--){
			int year = yearList.get(i-1);
			if(fyList.contains(year)){
				int yearIdx;
				if(timelineShift){
					yearIdx = timeline.getFyIndexFiscalYear(year);	
				}else{
					yearIdx = timeline.getFyIndex(year);
				}
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
			}
			if(comparatorFyList.contains(year)){
				int yearIdx;
				if(comparatorShift){
					yearIdx = comparatorTimeline.getFyIndexFiscalYear(year);	
				}else{
					yearIdx = comparatorTimeline.getFyIndex(year);					
				}
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
			}
		}
		this.dataFrame.addRow(row, row);
		this.dataFrame.addRow(comparatorRow, comparatorRow);
	}
}
