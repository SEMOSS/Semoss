package prerna.ui.components.specific.ousd;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.ds.BTreeDataFrame;
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

		List<Map<String, List<String>>> systemYears = timeline.getTimeData();
		Map<String, Double> budgets = timeline.getBudgetMap();
		List<Integer> fyList = timeline.getFyIdxArray();

		List<Map<String, List<String>>> comparatorYears = comparatorTimeline.getTimeData();
		Map<String, Double> comparatorBudgets = comparatorTimeline.getBudgetMap();
		List<Integer> comparatorFyList = comparatorTimeline.getFyIdxArray();

		this.dataFrame = new BTreeDataFrame(new String[] { "Year", this.comparatorName, this.roadmapName});
//		this.names[0] = "Year";
//		this.names[1] = this.comparatorName;
//		this.names[2] = this.roadmapName;

		String[] columns = new String[fyList.size() + 1];
		columns[0] = "System";
		int count = 1;
		for (Integer fy : fyList){
			columns[count] = fy+"";
			count++;
		}

		createTable(timeline, systemYears, budgets, comparatorYears, comparatorBudgets, fyList, columns);
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

	private void createTable(OUSDTimeline timeline, List<Map<String, List<String>>> systemYears, Map<String, Double> budgets, List<Map<String, List<String>>> comparatorYears, Map<String, Double> comparatorBudgets, List<Integer> fyNames, String[] oldNames){

		ArrayList<Object[]> templist = new ArrayList<Object[]>();
		
		List<String> processedSystems = new ArrayList<String>();
		Object[] totalRow = new Object[oldNames.length];
		totalRow[0] = this.roadmapName;

		Object[] comparatorTotalRow = new Object[oldNames.length];
		comparatorTotalRow[0] = this.comparatorName;

		NumberFormat formatter = NumberFormat.getCurrencyInstance(Locale.US);

		for(int i = systemYears.size()+2; i>0; i--){
			if(i==9||i==1){
				continue;
			}
			Map<String, List<String>> yearMap = systemYears.get(i-2);

			for(String system: yearMap.keySet()){
				if(totalRow[i-1] == null){
					if(!processedSystems.contains(system)){
						totalRow[i-1] = budgets.get(system);
					}
				}
				else {
					if(!processedSystems.contains(system)){
						totalRow[i-1] = ((Double) totalRow[i-1]) + budgets.get(system);
					}
				}
				processedSystems.add(system);
			}
			totalRow[i-1] = totalRow[i-1];
		}
		templist.add(totalRow);

		for(int i = 0; i<comparatorYears.size(); i++){
			Map<String, List<String>> yearMap = comparatorYears.get(i);

			for(String system: yearMap.keySet()){
				if(comparatorTotalRow[i+2] == null){
					comparatorTotalRow[i+2] = comparatorBudgets.get(system);
				}
				else {
					comparatorTotalRow[i+2] = ((Double) comparatorTotalRow[i+2]) + comparatorBudgets.get(system);
				}
				processedSystems.add(system);
			}
			if(comparatorTotalRow[i+2] != null){
				comparatorTotalRow[i+2] = comparatorTotalRow[i+2];
			}
		}
		templist.add(comparatorTotalRow);

//		this.list = new ArrayList<Object[]>();
		for (int i = 1; i < oldNames.length; i++){
			Object[] row = new Object[3];
			row[0] = fyNames.get(i - 1);
			Double val1 = (Double) (templist.get(0)[i] == null? 0.0 : templist.get(0)[i]);
			Double val2 = (Double) (templist.get(1)[i] == null? 0.0 : templist.get(1)[i]);
			row[1] = val2;
			row[2] = val1;
			this.dataFrame.addRow(row, row);
		}

	}
}
