//package prerna.ui.components.specific.ousd;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.ds.h2.H2Frame;
//import prerna.ui.components.playsheets.GridPlaySheet;
//
//public class OUSDConstraintCountPlaySheet extends GridPlaySheet{
//
//	private static final Logger LOGGER = LogManager.getLogger(OUSDConstraintCountPlaySheet.class.getName());
//	
//	OUSDTimeline timeline = new OUSDTimeline();
//	
//	String roadmapName;
//	
//	Map<String, List<String>> dataSystemMap;
//	Map<String, List<String>> granularBLUMap;
//	List<String> decomSystems = new ArrayList<String>();
//
//	@Override
//	public void setQuery(String query){
//		String delimiters = "[,]";
//		String[] parts = query.split(delimiters);
//		roadmapName = parts[0];
//	}
//
//	@Override
//	public void createData(){
//
//		try{
//			timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, roadmapName);
//		}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//
//		granularBLUMap = timeline.getGranularBLUMap();
//		dataSystemMap = timeline.getDataSystemMap();
//		
//		
//		
//		String[] columns = new String[8];
//		columns[0] = "Year";
//		columns[1] = "Granular BLU";
//		columns[2] = "Data Object";
//		columns[3] = "Total Supporting System Count";
//		columns[4] = "Remaining Supporting System Count";
//		columns[5] = "Remaining Supporting Systems";
//		columns[6] = "Decommissioned Supporting Count";
//		columns[7] = "Decommissioned Supporting Systems";
//
//		this.dataFrame = new H2Frame(columns);
//		
//		createTable();
//	}
//
//	public void createTable(){
//
////		this.list = new ArrayList<Object[]>();
//
//		int yearIdx = 0;
//		int year = 0;
//
//		while(yearIdx<timeline.getFyIdxArray().size()){
//
//			year = timeline.getYearFromIdx(yearIdx);
//
//			Map<String, List<String>> systemsThisYear = timeline.getTimeData().get(yearIdx);
//			List<String> decomThisYear = new ArrayList<String>();
//			for(String system: systemsThisYear.keySet()){
//				decomThisYear.add(system);
//				decomSystems.add(system);
//			}
//			
//			for(String granularBLU: granularBLUMap.keySet()){
//				List<String> supSys = granularBLUMap.get(granularBLU);
//				Object[] row = new Object[8];
//				row[0] = year;
//				row[1] = granularBLU;
//				row[2] = "-";
//				row[3] = supSys.size();
//				row[4] = remainingCount(supSys);
//				row[5] = remainingSystems(supSys);
//				row[6] = stoppedCount(supSys, decomThisYear);
//				row[7] = stoppedSystems(supSys, decomThisYear);
//				this.dataFrame.addRow(row);
//			}
//
//			for(String dataObject: dataSystemMap.keySet()){
//				List<String> supSys = dataSystemMap.get(dataObject);
//				Object[] row = new Object[8];
//				row[0] = year;
//				row[1] = "-";
//				row[2] = dataObject;
//				row[3] = supSys.size();
//				row[4] = remainingCount(supSys);
//				row[5] = remainingSystems(supSys);
//				row[6] = stoppedCount(supSys, decomThisYear);
//				row[7] = stoppedSystems(supSys, decomThisYear);
//				this.dataFrame.addRow(row);
//
//			}
//
//			yearIdx++;
//		}
//
//	}
//
//	private String stoppedSystems(List<String> supportingSystems, List<String> decomThisYear){
//		List<String> stoppedSystems = new ArrayList<String>();
//
//		for(String system: supportingSystems){
//			if(decomThisYear.contains(system)){
//				stoppedSystems.add(system);
//			}
//		}
//
//		if(stoppedSystems.isEmpty()){
//			return "-";
//		}else{
//			return stoppedSystems.toString();
//		}		
//	}
//
//	private Integer stoppedCount(List<String> supportingSystems, List<String> decomThisYear){
//		int count = 0;
//
//		for(String system: supportingSystems){
//			if(decomThisYear.contains(system)){
//				count++;
//			}
//		}
//		return count;
//	}
//
//	private String remainingSystems(List<String> supportingSystems){
//		List<String> remainingSystems = new ArrayList<String>();
//
//		for(String system: supportingSystems){
//			if(!decomSystems.contains(system)){
//				remainingSystems.add(system);
//			}
//		}
//		if(remainingSystems.isEmpty()){
//			return "-";
//		}else{
//			return remainingSystems.toString();
//		}			
//	}
//
//	private Integer remainingCount(List<String> supportingSystems){
//		int count = supportingSystems.size();
//
//		for(String system: supportingSystems){
//			if(decomSystems.contains(system)){
//				count--;
//			}
//		}
//		return count;
//	}
//
//	
//	
//}