package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.stanford.nlp.util.ArraySet;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class MHSDashboardDrillPlaysheet extends TablePlaySheet implements IDataMaker {

	private static final Logger logger = LogManager.getLogger(TablePlaySheet.class.getName());
	private DataMakerComponent dmComponent;
	
	private final String SDLC = "SDLCPhase";
	private final String ActivityGroup = "ActivityGroup";
	private final String DHA = "DHAGroup";
	private final String SDLC_ACTIVITYGROUP_DHA = "SDLCPhase_ActivityGroup_DHAGroup";
	private final String SYSTEM_ACTIVITY = "SystemActivity";
	private final String SYSTEM = "System";
	private final String SYSTEM_OWNER = "SystemOwner";
	private final String ACTIVITY = "Activity";
	private final String HEAT_VALUE = "HeatValue";
	private final String TotalHeatValue = "TotalHeatValue";
	private final String PLANNED_START = "PlannedStart";
	private final String PLANNED_END = "PlannedEnd";
	private final String ACTUAL_START = "ActualStart";
	private final String ACTUAL_END = "ActualEnd";
	private final String UPLOAD_DATE = "UploadDate";
	private final String MIN_ACTIVITY_VALUE = "MinActivityVal";
	
	private String userId;
	
	//private final String masterQuery = "SELECT DISTINCT ?SDLCPhase ?ActivityGroup ?DHAGroup ?SDLCPhase_ActivityGroup_DHAGroup ?SystemActivity ?System ?Activity ?UploadDate ?PlannedStart ?PlannedEnd ?ActualStart ?ActualEnd WHERE {{?SDLCPhase a <http://semoss.org/ontologies/Concept/SDLCPhase>}{?ActivityGroup a <http://semoss.org/ontologies/Concept/ActivityGroup>}{?DHAGroup a <http://semoss.org/ontologies/Concept/DHAGroup>}{?SDLCPhase_ActivityGroup_DHAGroup a <http://semoss.org/ontologies/Concept/SDLCPhase_ActivityGroup_DHAGroup>}{?SystemActivity a <http://semoss.org/ontologies/Concept/SystemActivity>}{?System a <http://semoss.org/ontologies/Concept/System>}{?Activity a <http://semoss.org/ontologies/Concept/Activity>}{?SDLCPhase <http://semoss.org/ontologies/Relation/Has> ?ActivityGroup}{?ActivityGroup <http://semoss.org/ontologies/Relation/Has> ?DHAGroup}{?SDLCPhase <http://semoss.org/ontologies/Relation/Has> ?SDLCPhase_ActivityGroup_DHAGroup}{?ActivityGroup <http://semoss.org/ontologies/Relation/Has> ?SDLCPhase_ActivityGroup_DHAGroup}{?DHAGroup <http://semoss.org/ontologies/Relation/Has> ?SDLCPhase_ActivityGroup_DHAGroup}{?SDLCPhase_ActivityGroup_DHAGroup <http://semoss.org/ontologies/Relation/Has> ?SystemActivity}{?SystemActivity <http://semoss.org/ontologies/Relation/Has> ?System}{?SystemActivity <http://semoss.org/ontologies/Relation/Has> ?Activity}{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/UploadDate> ?UploadDate}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/PlannedStart> ?PlannedStart}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/PlannedEnd> ?PlannedEnd}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/ActualStart> ?ActualStart}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/ActualEnd> ?ActualEnd}}";
	
	//private final static String masterQuery = "data.import ( api: MHSDashboard . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:ActivitySystem, c:System, c:Activity, c:ActivitySystemUploadDate__UploadDate, c:ActivitySystemUploadDate__PlannedStart, c:ActivitySystemUploadDate__PlannedEnd, c:ActivitySystemUploadDate__ActualStart, c:ActivitySystemUploadDate__ActualEnd], ([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: ActivitySystem], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: Activity], [c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__UploadDate],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__PlannedStart],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__PlannedEnd],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__ActualStart],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__ActualEnd] ))) ; ";


	//	static String masterPKQL = "data.import "
//			+ "( api:  . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:Activity, c:System, c:ActivitySystem__PlannedStart, c:ActivitySystem__PlannedEnd, c:ActivitySystem__ActualStart, c:ActivitySystem__ActualEnd, c:ActivitySystem__UploadDate, c:ActivitySystem__HeatValue], "
//			+ "([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: Activity], "
//			+ "[c: ActivitySystem , inner.join , c: Activity], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: ActivitySystem__PlannedStart], "
//			+ "[c: ActivitySystem , inner.join , c: ActivitySystem__PlannedEnd], [c: ActivitySystem , inner.join , c: ActivitySystem__ActualStart], [c: ActivitySystem , inner.join , c: ActivitySystem__ActualEnd], "
//			+ "[c: ActivitySystem , inner.join , c: ActivitySystem__UploadDate], [c: ActivitySystem , inner.join , c: ActivitySystem__HeatValue]) ) ) ; ";
	
//	private final String totalHeatPKQL = "col.add ( c: TotalHeatValue , m: Sum ( [ c: HeatValue ] [c: SDLCPhase, c: MHSGroup, c:DHAGroup] ) ) ; ";
	
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
		this.userId = userId;
		this.dataFrame.setUserId(userId);
	}
	
	/**
	 * Method to pass the FE the ordered list of SDLC phases and all values necessary for the main table
	 * @return returns data, headers, dataTableAlign, systems, and upload date
	 */
	@Override
	public Map getDataMakerOutput(String... selectors) {
		Map<String, Object> returnHashMap = aggregateDHAGroup();
		List<Object> sdlcList = new ArrayList <Object> (Arrays.asList("Strategy", "Requirement", "Design", "Development", "Test", "Security", "Deployment", "Training"));
		Map<String, String> dataTableAlign = new HashMap <String, String> ();
		returnHashMap.put("SDLCList", sdlcList);
		returnHashMap.put("styling", "MHS");
		returnHashMap.put("dataTableAlign", getDataTableAlign());
		returnHashMap.putAll(getSystem());
		returnHashMap.putAll(getSystemOwner());
		returnHashMap.putAll(getUploadDate());
		return returnHashMap;
	}
	
	public Map getDataTableAlign (){
		Map<String, String> dataTableAlign = new HashMap <String, String> ();
		dataTableAlign.put("levelOne", SDLC);
		dataTableAlign.put("levelTwo", ActivityGroup);
		dataTableAlign.put("levelThree", DHA);
		dataTableAlign.put("heatValue", HEAT_VALUE);
		dataTableAlign.put("minValue", MIN_ACTIVITY_VALUE);
		return dataTableAlign;
	}
	
	/**
	 * Method to pass FE values for system which will be used for the filters
	 * @return list of systems within a hashmap
	 */
	public Map getSystem() {
		String [] selectors = new String[]{SYSTEM};
		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
		Object systems = mainHash.get("data");
		Map<String, Object> returnHash = new HashMap<String, Object>();
		returnHash.put(SYSTEM, systems);
		return returnHash;
	}
	
	public Map getSystemOwner() {
		String [] selectors = new String[]{SYSTEM_OWNER};
		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
		Object systemsOwner = mainHash.get("data");
		Map<String, Object> returnHash = new HashMap<String, Object>();
		returnHash.put(SYSTEM_OWNER, systemsOwner);
		return returnHash;
	}
	
	/**
	 * Method to pass FE values for upload dates which will be used for the filters
	 * @return list of uploaded dates within a hashmap
	 */
	public Map getUploadDate() {
		String [] selectors = new String[]{UPLOAD_DATE};
		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
		Object upload = mainHash.get("data");
		Map<String, Object> returnHash = new HashMap<String, Object>();
		returnHash.put(UPLOAD_DATE, upload);
		return returnHash;
	}
	
	/**
	 * Method to iterated through specified selectors and calculate the heat values per each DHA group
	 * @return Hashmap contain SDLC, Group, DHA, heat value, minimum heat value
	 */
	public Map aggregateDHAGroup () {
		Map<String, Object> returnMap = new HashMap <String, Object> ();
		Map<String, Object> iteratorMap = new HashMap <String, Object> ();

		//set selector list
		List<String> selectorList = new ArrayList<String> ();
		selectorList.add(SDLC);
		selectorList.add(ActivityGroup);
		selectorList.add(DHA);
		selectorList.add(SDLC_ACTIVITYGROUP_DHA);
		selectorList.add(SYSTEM);
		selectorList.add(PLANNED_START);
		selectorList.add(PLANNED_END);
		selectorList.add(ACTUAL_START);
		selectorList.add(ACTUAL_END);
		selectorList.add(SYSTEM_OWNER);
		selectorList.add(UPLOAD_DATE);
		
		//delete duplicates
		iteratorMap.put(TinkerFrame.DE_DUP, true);
		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);

		boolean getRawData = false;
		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);

		//iterate to get data
		while(iterator.hasNext()){
			Object[] iteratirArr = iterator.next();
			String sdlc = (String) iteratirArr[0];
			String group = (String) iteratirArr[1];
			String dha = (String) iteratirArr[2];
			String key = (String) iteratirArr[3];
			//String system = (String) iteratirArr[4];
			String plannedStartDate = (String) iteratirArr[5];
			String plannedEndDate = (String) iteratirArr[6];
			String actualStartDate = (String) iteratirArr[7];
			String actualEndDate = (String) iteratirArr[8];

			Map<String, Object> innerMap = new HashMap <String, Object> ();
			
			innerMap.put(SDLC, sdlc);
			innerMap.put(ActivityGroup, group);
			innerMap.put(DHA, dha);
			
			//calculate heat value and get min value per each heat value
			Map<String, Number> valueMap = calculateValues(plannedStartDate, plannedEndDate, actualStartDate, actualEndDate);
			int heatValue = (int) valueMap.get("HeatVal");
			long minVal = (long)valueMap.get("MinVal");
			
			//if key already exists in map
			if(returnMap.containsKey(key)) {
				Map<String, Object> innerMapReturn = (Map<String, Object>) returnMap.get(key);
				int totalHeatVal = (int) innerMapReturn.get(HEAT_VALUE);
				int numActivtyReturn = (int) innerMapReturn.get("ACTIVITY_NUM");
				
				//calculate total average heat value 
				totalHeatVal = totalHeatVal * numActivtyReturn;
				numActivtyReturn +=1; 
				totalHeatVal = (totalHeatVal + heatValue)/numActivtyReturn;
				innerMap.put(HEAT_VALUE, totalHeatVal);
				innerMap.put("ACTIVITY_NUM", numActivtyReturn);
				
				List<Long> minValueReturnList = (List<Long>) innerMapReturn.get(MIN_ACTIVITY_VALUE);
				minValueReturnList.add(minVal);
				innerMap.put(MIN_ACTIVITY_VALUE, minValueReturnList);
				returnMap.put(key, innerMap);
			} 
			//else key doesnt exist in map
			else if (!returnMap.containsKey(key)) {
				innerMap.put(HEAT_VALUE, heatValue);
				innerMap.put ("ACTIVITY_NUM", 1);
				List<Long> minValueList = new ArrayList<Long>();
				minValueList.add(minVal);
				innerMap.put (MIN_ACTIVITY_VALUE, minValueList);
				returnMap.put(key, innerMap);
			}
		}
		return manipulateData(returnMap);
	}
	
	/**
	 * Method to calculate heat value based on the start and end planned and actual dates
	 * @param plannedStartDate String planned start date
	 * @param plannedEndDate String planned end date
	 * @param actualStartDate String actual start date
	 * @param actualEndDate String actual end date
	 * @return Hashmap<String, Long> where "HeatVal" is the heat value for each dha and "MinVal" is the lowest heat value for each DHA  
	 */
	public Map<String, Number> calculateValues (String plannedStartDate, String plannedEndDate, String actualStartDate, String actualEndDate) {
		Date plannedStart = null;
		Date plannedEnd = null;
		Date actualStart = null;
		Date actualEnd = null;
	
		//format string date to type Date
		try {
			if(!plannedStartDate.equals("null")){
				plannedStart = (Date) getDateFormat().parse(plannedStartDate);
			}
			if(!plannedEndDate.equals("null")){
				plannedEnd = (Date) getDateFormat().parse(plannedEndDate);
			}
			if(!actualStartDate.equals("null")){
				actualStart = (Date) getDateFormat().parse(actualStartDate);
			}
			if(!actualEndDate.equals("null")){
				actualEnd = (Date) getDateFormat().parse(actualEndDate);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		Map<String, Number> returnMap = new HashMap<String, Number> ();
		Date todaysDate = Calendar.getInstance().getTime();
		int heatValue = 0;
		long minVal = 0;
		
		// if there are both 'planned' and 'actual' dates : (planned end date - actual end date)
		if(plannedStart !=null && plannedEnd !=null && actualStart !=null && actualEnd !=null){
			heatValue = (int) ((plannedEnd.getTime() - actualEnd.getTime())/(24 * 60 * 60 * 1000));
		}
		// if there are no 'actual' dates : (planned start - today's date) if planned start date is greater than today's date
		else if (plannedStart !=null && plannedEnd !=null && actualStart == null && actualEnd == null) {
			if(plannedStart.getTime() < todaysDate.getTime()){
				heatValue = (int) ((plannedStart.getTime() - todaysDate.getTime())/(24 * 60 * 60 * 1000));
			} else {
				heatValue = 0;
			}
		}  
		// if there is no 'actualend' date : (planned start date - actual start date)
		else if(plannedStart !=null && plannedEnd !=null && actualStart != null && actualEnd == null ){
			heatValue = (int) ((plannedStart.getTime() - actualStart.getTime())/(24 * 60 * 60 * 1000));
			//only get a min value in this case..otherwise minValue will remain 0
			minVal = heatValue;
		} 
		//if there is row with absolutely no dates then it will not be calculated into the heat value
		else{
			heatValue = 0;
		}
		
		returnMap.put("HeatVal", heatValue);
		returnMap.put("MinVal", minVal);
		
		return returnMap;
	}
	
	public static DateFormat getDateFormat() {
		//formating date by month-day-year
		return new SimpleDateFormat("MM-dd-yyyy");
	}

	
	/**
	 * Method to format date the way FE expects it...ie without the string keys that objMap currently contains
	 * @param objMap 
	 * @return Hashmap
	 */
	public Map manipulateData (Map<String, Object> objMap) {
		Map<String, List<Object>> returnMap = new HashMap<String, List<Object>>();
		List<Object> returnList =new ArrayList<Object>();
		
		for(Entry<String, Object> entry : objMap.entrySet()) {
			HashMap value = (HashMap) entry.getValue();
			List<Object> innerList =new ArrayList<Object>();
			
			for(Object innerVal : value.entrySet()){
				String returnKey = ((Entry<String, Object>) innerVal).getKey();
				
				//return all keys but activity_num
				if(!returnKey.equals("ACTIVITY_NUM")){
					
					//get the lowest value in the list of heat values
					if(returnKey.equals(MIN_ACTIVITY_VALUE)){
						List<Long> heatList = (List<Long>) ((Entry<String, Object>) innerVal).getValue();
						Long minHeat = Collections.min(heatList);
						innerList.add(minHeat);
					} else {
						Object returnVal = ((Entry<String, Object>) innerVal).getValue();
						innerList.add(returnVal);
					}
				}
			}
			returnList.add(innerList);
		}
		returnMap.put("data", returnList);
		returnMap.put("headers", Arrays.asList(SDLC, MIN_ACTIVITY_VALUE, ActivityGroup, DHA, HEAT_VALUE));

		return returnMap;
	}
	
		
	/**
	 * Method to filter for the tools bar
	 * @param filterActivityTable - HashTable: KEY should be the column that you want to filter on (ie: System), VALUE should be the list of insight you want to filter  
	 * @return Hashmap of filter values
	 */
	public Map filter(Hashtable<String, Object> filterTable) {
		for (String columnKey : filterTable.keySet()) {
			List<Object> insightList = (List<Object>) filterTable.get(columnKey);
			dataFrame.filter(columnKey, insightList);
		}
		String [] selector = new String []{};
		return getDataMakerOutput(selector);
	}

	/**
	 * Method to unfilter on the tools bar
	 * @param unfilterDrillDownTable null
	 * @return Hashmap of unfiltered values
	 */
	public Map unfilter (Hashtable<String, Object> unfilterDrillDownTable) {
		dataFrame.unfilter();
		String [] selector = new String []{};
		return getDataMakerOutput(selector);
	}

	
	////////     These methods may be used for later iterations of this dashbaord    ////////

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
		selectorList.add(ActivityGroup);
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

//	/**
//	 * PQKL METHOD!!!...do not delete!
//	 * @param dhaMap - KEY - 'SDLC', 'GROUP', 'DHA' ; VALUE - the string value of the sdlc/group/dha that was sent
//	 * @return 
//	 * @return Map of all systems with their activities plan and actual start/end date
//	 */
//	public Map<String, Object> createGanttInsight (Hashtable<String, Object> dhaMap) {
//		Insight ganttInsight = new Insight(this.engine, "H2Frame", "Gantt");
//		
//		String sdlcString =  (String) dhaMap.get("SDLC");
//		String groupString =  (String) dhaMap.get("GROUP");
//		String dhaString =  (String) dhaMap.get("DHA");
//		
//		//String pkqlQuery = "data.import ( api: MHSDashboard . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:ActivitySystem, c:System, c:Activity, c:ActivitySystemUploadDate__PlannedStart, c:ActivitySystemUploadDate__PlannedEnd, c:ActivitySystemUploadDate__ActualStart, c:ActivitySystemUploadDate__ActualEnd], (c: SDLCPhase = ['%SDLC%'], c: MHSGroup = ['%MHS%'], c:DHAGroup = ['%DHAGroup%']), ([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: ActivitySystem], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: Activity],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__PlannedStart],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__PlannedEnd],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__ActualStart],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__ActualEnd] ))) ; ";
//		//pkqlQuery=pkqlQuery.replace("%SDLC%", sdlcString).replace("%MHS%", groupString).replace("%DHAGroup%", dhaString);
//		
//		
////		
////		PKQLRunner run = new PKQLRunner();
////		run.runPKQL(pkqlQuery, newFrame);
//		
//		
//		String ganttQuery = "SELECT DISTINCT ?ActivitySystem ?PlannedStart ?PlannedEnd ?ActualStart ?ActualEnd WHERE {{?SDLCPhase a <http://semoss.org/ontologies/Concept/SDLCPhase>}{?MHSGroup a <http://semoss.org/ontologies/Concept/MHSGroup>}{?DHAGroup a <http://semoss.org/ontologies/Concept/DHAGroup>}{?ActivitySystem a <http://semoss.org/ontologies/Concept/ActivitySystem>}{?System a <http://semoss.org/ontologies/Concept/System>}{?Activity a <http://semoss.org/ontologies/Concept/Activity>}{?ActivitySystemUploadDate a <http://semoss.org/ontologies/Concept/ActivitySystemUploadDate>}{?SDLCPhase <http://semoss.org/ontologies/Relation/Has> ?MHSGroup}{?MHSGroup <http://semoss.org/ontologies/Relation/Has> ?DHAGroup}{?DHAGroup <http://semoss.org/ontologies/Relation/Has> ?ActivitySystem}{?ActivitySystem <http://semoss.org/ontologies/Relation/Has> ?System}{?ActivitySystem <http://semoss.org/ontologies/Relation/Has> ?Activity}{?ActivitySystem <http://semoss.org/ontologies/Relation/Has> ?ActivitySystemUploadDate}{?ActivitySystemUploadDate <http://semoss.org/ontologies/Relation/Contains/PlannedStart> ?PlannedStart}{?ActivitySystemUploadDate <http://semoss.org/ontologies/Relation/Contains/PlannedEnd> ?PlannedEnd}{?ActivitySystemUploadDate <http://semoss.org/ontologies/Relation/Contains/ActualStart> ?ActualStart}{?ActivitySystemUploadDate <http://semoss.org/ontologies/Relation/Contains/ActualEnd> ?ActualEnd} BIND (<http://semoss.org/ontologies/Concept/SDLCPhase/%SDLC%> as ?SDLCPhase) BIND (<http://semoss.org/ontologies/Concept/MHSGroup/%ActivityGroup%> as ?MHSGroup) BIND (<http://semoss.org/ontologies/Concept/DHAGroup/%DHAGroup%> as ?DHAGroup)}";
//		ganttQuery = ganttQuery.replace("%SDLC%", sdlcString).replace("%ActivityGroup%", groupString).replace("%DHAGroup%", dhaString);
//		
//		//this.query will get the query added to the specific insight
//		DataMakerComponent dmc = new DataMakerComponent(this.engine, ganttQuery);
//		
//		H2Frame newFrame = new H2Frame();
////		newFrame.setUserId(this.userId);
////		newFrame.processDataMakerComponent(dmc);
//		ganttInsight.setUserID(this.userId);
//		ganttInsight.setInsightID(InsightStore.getInstance().put(ganttInsight));
//		ganttInsight.setInsightName("MHS PlaySheet Gantt for: " + sdlcString + " " + groupString + " " + dhaString );
//		ganttInsight.setDataMaker(newFrame);
//		
//		newFrame.processDataMakerComponent(dmc);
//		
//		InsightStore.getInstance().put(ganttInsight);
//		return ganttInsight.getWebData();
//	}
	
	/*
	 * Formats the data
	 */

}