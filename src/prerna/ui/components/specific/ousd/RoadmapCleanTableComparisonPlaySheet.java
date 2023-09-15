package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.annotations.BREAKOUT;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.ui.components.playsheets.GridPlaySheet;

@BREAKOUT
public class RoadmapCleanTableComparisonPlaySheet extends GridPlaySheet{

	String systemOwner;
	List<String> timelineNames = new ArrayList<String>();
	List<OUSDTimeline> timelines = new ArrayList<OUSDTimeline>();
	boolean multiple = false;

	// NAMING
	protected final String decomCount = " System Decommission Count";
	protected final String savingThisYear = " New Savings this year";
	protected final String buildCount = " New Interface Count";
	protected final String investmentCost = " Interface Development Cost";
	protected final String sustainCost = " Interface Sustainment Cost";
	protected final String risk = " Enterprise Risk";

	protected final String cumSavings = " Cumulative Savings";
	protected final String prevSavings = " Previous Decommissioning Savings";
	protected final String cumCost = " Cumulative Cost";
	protected final String roi = " ROI";
	protected final String opCost = " Operational Cost";

	@Override
	public void setQuery(String query){
		String delimiters = "[;]";
		String[] insights = query.split(delimiters);
		for(String timelineName: insights){
			timelineNames.add(timelineName);
		}
	}

	public void buildTable(List<String> names, String sysOwner){
		if(names.size()>1){
			System.out.println("BUILDING TWO ROWS");
			multiple = true;
		}else{
			System.out.println("BUILDING ONE ROW");
		}
		systemOwner = sysOwner;
		createTable();
	}

	public void createTable(){

		Map<String, OUSDTimeline> timelineMap = new HashMap<String, OUSDTimeline>();

		if(multiple){
			for(String timelineName: timelineNames){
				try{
					OUSDTimeline timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, timelineName);
					timelineMap.put(timelineName, timeline);
					timelines.add(timeline);
				}catch(ClassNotFoundException | InstantiationException | IllegalAccessException e){
					e.printStackTrace();
				}
			}
		}else{
			for(String timelineName: timelineNames){
				try{
					if(systemOwner == null){
						systemOwner = "DFAS";
					}
					OUSDTimeline timeline = OUSDPlaysheetHelper.buildTimeline(this.engine, timelineName, systemOwner);
					timelineMap.put(timelineName, timeline);
					timelines.add(timeline);
				}catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
					e.printStackTrace();
				}
			}
		}

		List<Integer> yearList = new ArrayList<Integer>();

		for(String timelineName : timelineMap.keySet()){
			List<Integer> fyList = timelineMap.get(timelineName).getFiscalYears();
			for(Integer year: fyList){
				if(!yearList.contains(year)){
					yearList.add(year);
				}
			}
		}
		Collections.sort(yearList);

		while(yearList.get(yearList.size()-1)<2021){
			yearList.add(yearList.get(yearList.size()-1)+1);
		}

		String[] columns = new String[yearList.size() + 1];
		columns[0] = "System";
		int count = 1;
		for (Integer year : yearList){
			columns[count] = "F"+year+"";
			count++;
		}

		this.dataFrame = new H2Frame(columns);

		for(String timelineName: timelineMap.keySet()){
			createTable(yearList, timelineMap.get(timelineName), timelineName);										
		}


	}

	@Override
	public Hashtable getDataMakerOutput(String... selectors){
		Hashtable<String, Object> map = (Hashtable<String, Object>) super.getDataMakerOutput();
		//		map.put("data", this.timeline.getGanttData());
		//		map.put("headers", this.timeline.getGanttHeaders());
//		List<Object> datas = new ArrayList<Object>();
//		for(OUSDTimeline timeline: timelines){
//			datas.add(timeline.getDashboardData());
//		}
		map.put("data", timelines.get(0).getDashboardData());
		map.put("headers", new System[0]);
		return map;
	}

	private void createTable(List<Integer> yearList, OUSDTimeline timeline, String timelineName){

		String[] names = this.dataFrame.getColumnHeaders();

		timeline.buildData(yearList, names, timelineName);
		List<Object[]> rows = timeline.getOutputLists();
		addRowsToDataFrame(rows);

		if(!multiple){
			addTeaserRows(names);
		}
	}


	private void addTeaserRows(String[] names){

		Object[] migration = new Object[names.length];
		migration[0] = "Data Migration";
		fillWithTBD(migration);
		this.dataFrame.addRow(migration, names);

		Object[] hw = new Object[names.length];
		hw[0] = "Hardware Plus Ups";
		fillWithTBD(hw);
		this.dataFrame.addRow(hw, names);

		Object[] sw = new Object[names.length];
		sw[0] = "Software Plus Ups";
		fillWithTBD(sw);
		this.dataFrame.addRow(sw, names);

		Object[] people = new Object[names.length];
		people[0] = "People Training";
		fillWithTBD(people);
		this.dataFrame.addRow(people, names);

		Object[] infra = new Object[names.length];
		infra[0] = "Infrastructure";
		fillWithTBD(infra);
		this.dataFrame.addRow(infra, names);
	}

	private void addRowsToDataFrame(List<Object[]> rows){
		for(int i = 0; i<rows.size(); i++){
			this.dataFrame.addRow(rows.get(i), this.dataFrame.getColumnHeaders());
		}
	}

	private void fillWithTBD(Object[] row){
		for(int i = 0; i <row.length ; i++){
			if(row[i] == null){
				row[i] = "TBD";
			}
		}
	}

}
