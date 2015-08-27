package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.ds.BTreeDataFrame;
import prerna.ui.components.playsheets.GridPlaySheet;

public class RoadmapPlaySheet extends GridPlaySheet{

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
		String[] columns = new String[5];
		columns[0] = "Year";
		columns[1] = "System";
		columns[2] = "System Budget";
		columns[3] = "Affected System";
		columns[4] = "Lost Data";

		this.dataFrame = new BTreeDataFrame(columns);

		try{
			timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, roadmapName);
		}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
			e.printStackTrace();
		}

		List<Map<String, List<String>>> systemYears = timeline.getTimeData();
		Map<String, Double> budgets = timeline.getBudgetMap();
		Map<String, List<List<String>>> sdsMap = timeline.getSystemDownstreamMap();
		

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

//		ArrayList<Object[]> list = new ArrayList<Object[]>();

		List<String> decommissionedSystems = new ArrayList<String>();
		for(int i =0; i<systemYears.size(); i++){
			List<String> processedSystems = new ArrayList<String>();
			Map<String, List<String>> yearMap = systemYears.get(i);

			for(String system: yearMap.keySet()){
				if(sdsMap.keySet().contains(system)){
					for(List<String> downstream: sdsMap.get(system)){
						Object[] row = new Object[5];
						row[0] = timeline.getYearFromIdx(i);
						row[1] = system;
						row[2] = budgets.get(system);
						row[3] = downstream.get(0);
						row[4] = downstream.get(1);
						if(downstream.get(0).equals("DIFMS")){
							System.out.println();
						}
						if(!yearMap.keySet().contains(downstream.get(0)) && !decommissionedSystems.contains(downstream.get(0))){
							this.dataFrame.addRow(row, row);
							if(!processedSystems.contains(system)){
								processedSystems.add(system);
							}
						}
					}
				}else{
					Object[] row = new Object[5];
					row[0] = timeline.getYearFromIdx(i);
					row[1] = system;
					row[2] = budgets.get(system);
					row[3] = "-";
					row[4] = "-";
					this.dataFrame.addRow(row, row);
					if(!processedSystems.contains(system)){
						processedSystems.add(system);
					}
				}
			}

			for(String system: yearMap.keySet()){
				Object[] row = new Object[5];
				row[0] = timeline.getYearFromIdx(i);
				row[1] = system;
				row[2] = budgets.get(system);
				row[3] = "-";
				row[4] = "-";
				if(!processedSystems.contains(system)){
					this.dataFrame.addRow(row, row);
				}
			}

			for(String system: processedSystems){
				if(!decommissionedSystems.contains(system)){				
					decommissionedSystems.add(system);
				}
			}
		}
	}
}
