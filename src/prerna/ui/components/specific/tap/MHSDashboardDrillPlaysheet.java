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
	private final String SYSTEM_ACTIVITY = "SystemActivity";
	private final String ACTIVITY = "Activity";
	private final String SYSTEM = "System";
	private final String HEAT_VALUE = "HeatValue";
	private final String TotalHeatValue = "TotalHeatValue";
	private final String PLANNED_START = "PlannedStart";
	private final String PLANNED_END = "PlannedEnd";
	private final String ACTUAL_START = "ActualStart";
	private final String ACTUAL_END = "ActualEnd";
	private final String UPLOAD_DATE = "UploadDate";
	private final String MIN_ACTIVITY_VALUE = "MinActivityVal";
	
	private String userId;
	
	//private final String masterQuery = "";
	
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
	
	/*
	 * 	sdlc to group to dha is the default view
	 * @see prerna.ui.components.playsheets.TablePlaySheet#getDataMakerOutput(java.lang.String[])
	 */
	@Override
	public Map getDataMakerOutput(String... selectors) {
		Map<String, Object> returnHashMap = aggregateDHAGroup();
		List<Object> sdlcList = new ArrayList <Object> (Arrays.asList("Strategy", "Requirement", "Design", "Development", "Test", "Security", "Deployment", "Training"));
		returnHashMap.put("SDLCList", sdlcList);
		return returnHashMap;
	}
	
	// just calls default getDataMakerOutput
	public Map getData(Hashtable<String, Object> dhaHashmap) {
		String [] selectors = new String[]{SYSTEM, UPLOAD_DATE};
		return super.getDataMakerOutput(selectors);
	}
	
	public Map aggregateDHAGroup () {
		Map<String, List<Object>> returnMap = new HashMap<String, List<Object>>();
		Map<String, Object> valueMap = new HashMap <String, Object> ();
		Map<String, Object> iteratorMap = new HashMap <String, Object> ();

		List<String> selectorList = new ArrayList<String> ();
		selectorList.add(SDLC);
		selectorList.add(ActivityGroup);
		selectorList.add(DHA);
		selectorList.add(PLANNED_START);
		selectorList.add(PLANNED_END);
		selectorList.add(ACTUAL_START);
		selectorList.add(ACTUAL_END);
		selectorList.add(UPLOAD_DATE);
		selectorList.add(SYSTEM);
		selectorList.add(ACTIVITY);

		iteratorMap.put(TinkerFrame.DE_DUP, true);
		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);

		boolean getRawData = false;
		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);

		while(iterator.hasNext()){
			Object[] iteratirArr = iterator.next();
			String sdlc = (String) iteratirArr[0];
			String group = (String) iteratirArr[1];
			String dha = (String) iteratirArr[2];
			String plannedStartDate = (String) iteratirArr[3];
			String plannedEndDate = (String) iteratirArr[4];
			String actualStartDate = (String) iteratirArr[5];
			String actualEndDate = (String) iteratirArr[6];
            String uploadDate = (String) iteratirArr[7];
            String system = (String) iteratirArr[8];
            String activity = (String) iteratirArr[9];

			String key = sdlc + "_" + group + "_" + dha;

			Date plannedStart = null;
			Date plannedEnd = null;
			Date actualStart = null;
			Date actualEnd = null;
			//Date upload = null;
		
			try {
				if(plannedStartDate != null){
					plannedStart = (Date) getDateFormat().parse(plannedStartDate);
				}
				if(plannedEndDate != null){
					plannedEnd = (Date) getDateFormat().parse(plannedEndDate);
				}
				if(actualStartDate != null){
					actualStart = (Date) getDateFormat().parse(actualStartDate);
				}
				if(actualEndDate != null){
					actualEnd = (Date) getDateFormat().parse(actualEndDate);
				}
//				upload = (Date) getDateFormat().parse(uploadDate);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			Date todaysDate = Calendar.getInstance().getTime();
			long heatValue = 0;
			
			//actualEnd is after todaysDate
			if(actualEnd==null){
				actualEnd = todaysDate;
			}
			
			if(actualEnd.compareTo(todaysDate)> 0){
				if(plannedStart !=null && actualStart != null){
					heatValue = (plannedStart.getTime()-actualStart.getTime())/(24 * 60 * 60 * 1000);
				}else {
					heatValue = 0;
				}
        	}else if(actualEnd.compareTo(todaysDate) < 0){
        		if(plannedEnd !=null && actualEnd != null){
        			heatValue = (plannedEnd.getTime()-actualEnd.getTime())/(24 * 60 * 60 * 1000); 
        		}else {
					heatValue = 0;
				}
        	} 
        	//TODO what do we do in the instance of actual date equaling today's date?
        	else {
        		heatValue = 0;
        	}

			Map<String, Object> innerMap = new HashMap <String, Object> ();
			innerMap.put(SDLC, sdlc);
			innerMap.put(ActivityGroup, group);
			innerMap.put(DHA, dha);
			//innerMap.put(UPLOAD_DATE, upload);
			List<Date> uploadList = new ArrayList<Date> ();
			if(valueMap.containsKey(key)) {
				Map<String, Object> innerMapReturn = (Map<String, Object>) valueMap.get(key);
				long totalHeatVal = (long) innerMapReturn.get(HEAT_VALUE);
				int numActivtyReturn = (int) innerMapReturn.get("ACTIVITY_NUM");
				List<Long> heatValReturn = (List<Long>) innerMapReturn.get(MIN_ACTIVITY_VALUE);
				heatValReturn.add(heatValue);
//				if(heatValReturn > heatValue){
//					innerMap.put (MAX_ACTIVITY_VALUE, heatValReturn);
//				} else {
//					innerMap.put (MAX_ACTIVITY_VALUE, heatValue);
//				}
				
				totalHeatVal = totalHeatVal * numActivtyReturn;
				numActivtyReturn +=1; 
				totalHeatVal = (totalHeatVal + heatValue)/numActivtyReturn;
				innerMap.put(HEAT_VALUE, totalHeatVal);
				innerMap.put("ACTIVITY_NUM", numActivtyReturn);
				innerMap.put (MIN_ACTIVITY_VALUE, heatValReturn);
				valueMap.put(key, innerMap);

			} else if (!valueMap.containsKey(key)) {
				innerMap.put(HEAT_VALUE, heatValue);
				innerMap.put ("ACTIVITY_NUM", 1);
				List<Long> heatList = new ArrayList<Long>();
				heatList.add(heatValue);
 				innerMap.put (MIN_ACTIVITY_VALUE, heatList);
				
				valueMap.put(key, innerMap);
			}
		}

		List<Object> returnList =new ArrayList<Object>();
		for(Entry<String, Object> entry : valueMap.entrySet()) {
			 HashMap value = (HashMap) entry.getValue();
			 List<Object> innerList =new ArrayList<Object>();
			 for(Object innerVal : value.entrySet()){
				 String returnKey = ((Entry<String, Object>) innerVal).getKey();
				 if(!returnKey.equals("ACTIVITY_NUM")){
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
	 * Method to filter on systems
	 * @param filterActivityTable - HashTable: KEY should be the column that you want to filter on (ie: System), VALUE should be the list of insight you want to filter  
	 * @return 
	 */
	public Map filter(Hashtable<String, Object> filterTable) {
		for (String columnKey : filterTable.keySet()) {
			List<Object> insightList = (List<Object>) filterTable.get(columnKey);
			dataFrame.filter(columnKey, insightList);
		}
		String [] selector = new String []{};
		return getDataMakerOutput(selector);
	}

	//TODO unfilter - for system, group, DHA group
	public Map unfilter (Hashtable<String, Object> unfilterDrillDownTable) {
		dataFrame.unfilter();
		String [] selector = new String []{};
		return getDataMakerOutput(selector);
	}


	/**
	 * Method to return the correct order of the SDLC phases
	 * @return Map, where the KEY is "SDLC", VALUE is a list of the phases
	 */
	public Map<String, List<Object>> getUIOptions () {
		Map<String, List<Object>> returnMap = new HashMap<String, List<Object>> ();
		Map<String, String> uiOptionMap = new HashMap<String, String> ();
		List<Object> sdlcList = new ArrayList <Object> (Arrays.asList("Strategy", "Requirement", "Design", "Development", "Test", "Security", "Deployment", "Training"));
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
//	 * Method to get the dha-system relationship for when a user clicks on a dha
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
	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("dd-MM-yyyy");
	}


}
