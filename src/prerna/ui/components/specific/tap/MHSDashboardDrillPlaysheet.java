package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.stanford.nlp.util.ArraySet;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLRunner;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class MHSDashboardDrillPlaysheet extends TablePlaySheet implements IDataMaker {

	private static final Logger logger = LogManager.getLogger(TablePlaySheet.class.getName());
	private DataMakerComponent dmComponent;
	
	private final String SDLC = "SDLCPhase";
	private final String MHSGroup = "MHSGroup";
	private final String DHA = "DHAGroup";
	private final String ACTIVITY_SYSTEM = "ActivitySystem";
	private final String ACTIVITY = "Activity";
	private final String SYSTEM = "System";
	private final String HEAT_VALUE = "HeatValue";
	private final String TotalHeatValue = "TotalHeatValue";
	private final String PLANNED_START = "PlannedStart";
	private final String PLANNED_END = "PlannedEnd";
	private final String ACTUAL_START = "ActualStart";
	private final String ACTUAL_END = "ActualEnd";
	private final String DATE = "UploadDate";
	
	//private final String masterQuery = "";
	
	//private final static String masterQuery = "data.import ( api: MHSDashboard . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:ActivitySystem, c:System, c:Activity, c:SystemActivityUploadDate__UploadDate, c:SystemActivityUploadDate__PlannedStart, c:SystemActivityUploadDate__PlannedEnd, c:SystemActivityUploadDate__ActualStart, c:SystemActivityUploadDate__ActualEnd], ([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: ActivitySystem], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: Activity], [c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__UploadDate],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__PlannedStart],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__PlannedEnd],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__ActualStart],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__ActualEnd] ))) ; ";


	//	static String masterPKQL = "data.import "
//			+ "( api:  . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:Activity, c:System, c:ActivitySystem__PlannedStart, c:ActivitySystem__PlannedEnd, c:ActivitySystem__ActualStart, c:ActivitySystem__ActualEnd, c:ActivitySystem__UploadDate, c:ActivitySystem__HeatValue], "
//			+ "([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: Activity], "
//			+ "[c: ActivitySystem , inner.join , c: Activity], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: ActivitySystem__PlannedStart], "
//			+ "[c: ActivitySystem , inner.join , c: ActivitySystem__PlannedEnd], [c: ActivitySystem , inner.join , c: ActivitySystem__ActualStart], [c: ActivitySystem , inner.join , c: ActivitySystem__ActualEnd], "
//			+ "[c: ActivitySystem , inner.join , c: ActivitySystem__UploadDate], [c: ActivitySystem , inner.join , c: ActivitySystem__HeatValue]) ) ) ; ";
	
	private final String totalHeatPKQL = "col.add ( c: TotalHeatValue , m: Sum ( [ c: HeatValue ] [c: SDLCPhase, c: MHSGroup, c:DHAGroup] ) ) ; ";
	
	static String instanceOfPlaysheet = "prerna.ui.components.specific.tap.MHSDashboardDrillPlaysheet";
	
	/**
	 * Method to create a datamaker
	 */
	@Override
	public void createData() {
		if (this.dmComponent == null) {
			//this.query will get the query added to the specific insight
			this.dmComponent = new DataMakerComponent(this.engine, this.query);
		}

		if(this.dataFrame == null) {
			this.dataFrame = new H2Frame();
		}
		this.dataFrame.processDataMakerComponent(this.dmComponent);
	}


	@Override
	public void setUserId(String userId) {
		if(this.dataFrame == null) {
			this.dataFrame = new H2Frame();
		}
		this.dataFrame.setUserId(userId);
	}
	
	/*
	 * 	sdlc to group to dha is the default view
	 * @see prerna.ui.components.playsheets.TablePlaySheet#getDataMakerOutput(java.lang.String[])
	 */
	@Override
	public Map getDataMakerOutput(String... selectors) {
		selectors = new String[]{SDLC, MHSGroup, DHA, SYSTEM, DATE, PLANNED_START, PLANNED_END, ACTUAL_START, ACTUAL_END};
		Map<String, Object> returnHashMap = super.getDataMakerOutput(selectors);
	//	returnHashMap.putAll(getUIOptions());
		return returnHashMap;
	}
	
	// just calls default getDataMakerOutput
	public Map getData(Hashtable<String, Object> obj) {
		String [] selectors = new String[]{SDLC, MHSGroup, DHA, SYSTEM, DATE};
		Map<String, Object> returnHashMap = super.getDataMakerOutput(selectors);
	//	returnHashMap.putAll(getUIOptions());
		return returnHashMap;
	}
	
	public Map aggregateDHAGroup () {
		Map<String, List<Object>> returnMap = new HashMap<String, List<Object>>();
		Map<String, Object> valueMap = new HashMap <String, Object> ();
		Map<String, Object> iteratorMap = new HashMap <String, Object> ();

		List<String> selectorList = new ArrayList<String> ();
		selectorList.add(SDLC);
		selectorList.add(MHSGroup);
		selectorList.add(DHA);
		selectorList.add(HEAT_VALUE);
		selectorList.add(DATE);

		iteratorMap.put(TinkerFrame.DE_DUP, true);
		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);

		boolean getRawData = false;
		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);

		while(iterator.hasNext()){
			Object[] iteratirArr = iterator.next();
			String sdlc = (String) iteratirArr[0];
			String group = (String) iteratirArr[1];
			String dha = (String) iteratirArr[2];
			double heat = (Double) iteratirArr[3];
			String key = sdlc + "_" + group + "_" + dha;

			Map<String, Object> innerMap = new HashMap <String, Object> ();
			innerMap.put(SDLC, sdlc);
			innerMap.put(MHSGroup, group);
			innerMap.put(DHA, dha);
			
			if(valueMap.containsKey(key)) {
				Map<String, Object> innerMapReturn = (Map<String, Object>) valueMap.get(key);
				double totalHeatVal = (double) innerMapReturn.get(HEAT_VALUE);
				totalHeatVal += heat;
				innerMap.put(HEAT_VALUE, totalHeatVal);
				valueMap.put(key, innerMap);

			} else if (!valueMap.containsKey(key)) {
				innerMap.put(HEAT_VALUE, heat);
				valueMap.put(key, innerMap);
			}

		}
		List<Object> returnList =new ArrayList<Object>();
		for(Entry<String, Object> entry : valueMap.entrySet()) {
			 HashMap value = (HashMap) entry.getValue();
			 List<Object> innerList =new ArrayList<Object>();
			 for(Object innerVal : value.entrySet()){
				 Object returnVal = ((Entry<String, Object>) innerVal).getValue();
				 innerList.add(returnVal);
			 }
			 returnList.add(innerList);
		}
		returnMap.put("data", returnList);

		return returnMap;
	}

		
	/**
	 * Method to filter on systems
	 * @param filterActivityTable - HashTable: KEY should be the column that you want to filter on (ie: System), VALUE should be the list of insight you want to filter  
	 * @return 
	 */
	//	public void filter(Hashtable<String, Object> filterTable) {
	//		for (String columnKey : filterTable.keySet()) {
	//			List<Object> insightList = (List<Object>) filterTable.get(columnKey);
	//			dataFrame.filter(columnKey, insightList);
	//		}
	//	}
	public Map filter(Hashtable<String, Object> filterTable) {
		for (String columnKey : filterTable.keySet()) {
			List<Object> insightList = (List<Object>) filterTable.get(columnKey);
			dataFrame.filter(columnKey, insightList);
		}
		return getData(filterTable);
	}

	//TODO unfilter - for system, group, DHA group
	public Map unfilter (Hashtable<String, Object> unfilterDrillDownTable) {
		dataFrame.unfilter();
		return getData(unfilterDrillDownTable);
	}


	/**
	 * Method to return the correct order of the SDLC phases
	 * @return Map, where the KEY is "SDLC", VALUE is a list of the phases
	 */
	public Map<String, List<Object>> getUIOptions () {
		Map<String, List<Object>> returnMap = new HashMap<String, List<Object>> ();
		Map<String, String> uiOptionMap = new HashMap<String, String> ();
		List<Object> sdlcList = new ArrayList <Object> (Arrays.asList("Strategy", "Requirement", "Design", "Development", "Test", "Security", "Deployment", "Training"));
//		uiOptionMap.put("Styling", "MHSDashboard");
		sdlcList.add(uiOptionMap);
		returnMap.put("uiOptions", sdlcList);
		return returnMap;
	}

	/**
	 * Method to get a list of Systems based on the DHA that is selected
	 * @param dhaHashmap - HashTable: KEY should contain "DHAGroup", VALUE should contain the user selected DHA instance
	 * @return Set of systems
	 */
	public Set<String> getDHASystemRelation (Hashtable<String, Object> dhaHashmap) {
		Set<String> returnList = new ArraySet<String> ();                   

		Map<String, Object> iteratorMap = new HashMap <String, Object> ();

		List<String> selectorList = new ArrayList<String> ();
		selectorList.add(DHA);
		selectorList.add(SYSTEM);

		iteratorMap.put(TinkerFrame.DE_DUP, true);
		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);

		boolean getRawData = false;
		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);

		while(iterator.hasNext()){
			Object[] iteratirArr = iterator.next();
			String dhaKey = (String) iteratirArr[0];
			String systemVal = (String) iteratirArr[1];

			for(Object value : dhaHashmap.values() ){
				if(dhaKey.equals((String)value)){
					returnList.add(systemVal);
				}
			}
		}
		return returnList;
	}

	/**
	 * Method to get a list of System based on the Group that is selected
	 * @param groupHashmap - HashTable: KEY should contain "Group", VALUE should contain the user selected Group instance
	 * @return Set of systems
	 */
	public Set<String> getGroupsSystemRelation (Hashtable<String, Object> groupHashmap) {
		Set<String> returnList = new ArraySet<String> ();                   

		Map<String, Object> iteratorMap = new HashMap <String, Object> ();

		List<String> selectorList = new ArrayList<String> ();
		selectorList.add(MHSGroup);
		selectorList.add(SYSTEM);

		iteratorMap.put(TinkerFrame.DE_DUP, true);
		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);

		boolean getRawData = false;
		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);

		while(iterator.hasNext()){
			Object[] iteratirArr = iterator.next();
			String groupsKey = (String) iteratirArr[0];
			String systemVal = (String) iteratirArr[1];

			for(Object value : groupHashmap.values() ){
				if(groupsKey.equals((String)value)){
					returnList.add(systemVal);
				}
			}
		}
		return returnList;
	}

	/**
	 * Method to get the dha-system relationship for when a user clicks on a dha
	 * @param dhaMap - KEY - 'SDLC', 'GROUP', 'DHA' ; VALUE - the string value of the sdlc/group/dha that was sent
	 * @return 
	 * @return Map of all systems with their activities plan and actual start/end date
	 */
	public Map<String, Object> createGanttInsight (Hashtable<String, Object> dhaMap) {
		Insight ganttInsight = new Insight(this.engine, "H2Frame", "Gantt");
		
		String sdlcString =  (String) dhaMap.get("SDLC");
		String groupString =  (String) dhaMap.get("GROUP");
		String dhaString =  (String) dhaMap.get("DHA");
		
		//String pkqlQuery = "data.import ( api: MHSDashboard . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:ActivitySystem, c:System, c:Activity, c:SystemActivityUploadDate__PlannedStart, c:SystemActivityUploadDate__PlannedEnd, c:SystemActivityUploadDate__ActualStart, c:SystemActivityUploadDate__ActualEnd], (c: SDLCPhase = ['%SDLC%'], c: MHSGroup = ['%MHS%'], c:DHAGroup = ['%DHAGroup%']), ([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: ActivitySystem], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: Activity],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__PlannedStart],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__PlannedEnd],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__ActualStart],[c: SystemActivityUploadDate , inner.join , c: SystemActivityUploadDate__ActualEnd] ))) ; ";
		//pkqlQuery=pkqlQuery.replace("%SDLC%", sdlcString).replace("%MHS%", groupString).replace("%DHAGroup%", dhaString);
		
		
//		
//		PKQLRunner run = new PKQLRunner();
//		run.runPKQL(pkqlQuery, newFrame);
		
		
		String ganttQuery = "SELECT DISTINCT ?System ?Activity ?PlannedStart ?PlannedEnd ?ActualStart ?ActualEnd WHERE {{?SDLCPhase a <http://semoss.org/ontologies/Concept/SDLCPhase>}{?MHSGroup a <http://semoss.org/ontologies/Concept/MHSGroup>}{?DHAGroup a <http://semoss.org/ontologies/Concept/DHAGroup>}{?ActivitySystem a <http://semoss.org/ontologies/Concept/ActivitySystem>}{?System a <http://semoss.org/ontologies/Concept/System>}{?Activity a <http://semoss.org/ontologies/Concept/Activity>}{?SystemActivityUploadDate a <http://semoss.org/ontologies/Concept/SystemActivityUploadDate>}{?SDLCPhase <http://semoss.org/ontologies/Relation/Has> ?MHSGroup}{?MHSGroup <http://semoss.org/ontologies/Relation/Has> ?DHAGroup}{?DHAGroup <http://semoss.org/ontologies/Relation/Has> ?ActivitySystem}{?ActivitySystem <http://semoss.org/ontologies/Relation/Has> ?System}{?ActivitySystem <http://semoss.org/ontologies/Relation/Has> ?Activity}{?ActivitySystem <http://semoss.org/ontologies/Relation/Has> ?SystemActivityUploadDate}{?SystemActivityUploadDate <http://semoss.org/ontologies/Relation/Contains/PlannedStart> ?PlannedStart}{?SystemActivityUploadDate <http://semoss.org/ontologies/Relation/Contains/PlannedEnd> ?PlannedEnd}{?SystemActivityUploadDate <http://semoss.org/ontologies/Relation/Contains/ActualStart> ?ActualStart}{?SystemActivityUploadDate <http://semoss.org/ontologies/Relation/Contains/ActualEnd> ?ActualEnd} BIND (<http://semoss.org/ontologies/Concept/SDLCPhase/%SDLC%> as ?SDLCPhase) BIND (<http://semoss.org/ontologies/Concept/MHSGroup/%MHS%> as ?MHSGroup) BIND (<http://semoss.org/ontologies/Concept/DHAGroup/%DHAGroup%> as ?DHAGroup)}";
		ganttQuery = ganttQuery.replace("%SDLC%", sdlcString).replace("%MHS%", groupString).replace("%DHAGroup%", dhaString);
		
		if (this.dmComponent == null) {
			//this.query will get the query added to the specific insight
			this.dmComponent = new DataMakerComponent(this.engine, ganttQuery);
		}
		
		H2Frame newFrame = new H2Frame();
		
		this.dataFrame.processDataMakerComponent(this.dmComponent);
		
		ganttInsight.setInsightID(InsightStore.getInstance().put(ganttInsight));
		ganttInsight.setInsightName("MHS PlaySheet Gantt for: " + sdlcString + " " + groupString + " " + dhaString );
		ganttInsight.setDataMaker(newFrame);
		InsightStore.getInstance().put(ganttInsight);
		return ganttInsight.getWebData();
	}
	
	/*
	 * Formats the data
	 */
	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("dd-MM-yyyy");
	}

//	/**
//	 * Method to filter on systems
//	 * @param filterActivityTable - HashTable: KEY should be the column that you want to filter on, VALUE should contain the user selected Group instance
//	 */
//	public void filterSystem (Hashtable<String, Object> filterSystemTable) {
//		for(String key: filterSystemTable.keySet()){
//			List<Object> activityValue = (List<Object>) filterSystemTable.get(key);
//			dataFrame.filter(key, activityValue);
//		}
//	}
//
//	// TODO unfilter - activities
//	public void unfilterSystem (Hashtable<String, Object> unfilterSystemTable) {
//		for(String activtyKey: unfilterSystemTable.keySet()) {
//			dataFrame.unfilter(activtyKey);
//		}
//	}
}
