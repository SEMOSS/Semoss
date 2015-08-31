package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.ds.OrderedBTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;

public class RoadmapCleanTablePlaySheet extends GridPlaySheet{

	OUSDTimeline timeline;
	String roadmapName;

	@Override
	public void setQuery(String query){
		String delimiters = "[,]";
		String[] insights = query.split(delimiters);
		roadmapName = insights[0];
	}

	@Override
	public void createData(){

		try{
			timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, roadmapName);
		}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
			e.printStackTrace();
		}

		List<Map<String, List<String>>> systemYears = timeline.getTimeData();
		Map<String, Double> budgets = timeline.getBudgetMap();
		Map<String, List<List<String>>> sdsMap = timeline.getSystemDownstreamMap();
		List<Integer> fyList = timeline.getFyIdxArray();

		String[] columns = new String[fyList.size() + 2];
		columns[0] = "System";
		int count = 1;
		for (Integer fy : fyList){
			columns[count] = fy+"";
			count++;
		}
		columns[count] = "Total";

		this.dataFrame = new OrderedBTreeDataFrame(columns);

		createTable(timeline, systemYears, budgets, sdsMap);
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

	private void createTable(OUSDTimeline timeline, List<Map<String, List<String>>> systemYears, Map<String, Double> budgets, Map<String, List<List<String>>> sdsMap){

		List<Object[]> list = new ArrayList<Object[]>();
		String[] names = this.dataFrame.getColumnHeaders();
		List<String> processedSystems = new ArrayList<String>();
		Object[] totalRow = new Object[names.length];
		totalRow[0] = "TOTAL";

		for(int i =0; i<systemYears.size(); i++){
			Map<String, List<String>> yearMap = systemYears.get(i);

			for(String system: yearMap.keySet()){
				if(!processedSystems.contains(system)){
					Object[] row = new Object[names.length];
					row[0] = system;
					row[i+1] = budgets.get(system);
					row[names.length - 1] = budgets.get(system);
					list.add(row);
					processedSystems.add(system);
				}
				else {
					// TODO: figure out how to handle partial decommissioning in terms of split
					int sysIdx = processedSystems.indexOf(system);
					Object[] row = list.get(sysIdx);
					row[i+1] = budgets.get(system);
				}
				if(totalRow[i+1] == null){
					totalRow[i+1] = budgets.get(system);
				}
				else {
					totalRow[i+1] = ((Double) totalRow[i+1]) + budgets.get(system);
				}
			}
		}

		list.add(totalRow);
		for(Object[] row : list){
			this.dataFrame.addRow(row, row);
		}
	}
}
