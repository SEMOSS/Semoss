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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jgrapht.util.VertexPair;

import edu.stanford.nlp.util.ArraySet;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLRunner;
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
	private final String SYSTEM_ACTIVITY = "SYSTEMACTIVITY";
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
	private final String IS_ACTIVE = "IsActive";
	private final String ACTIVITY_NUM = "ACTIVITY_NUM";
	private final String CURRENT_STATUS = "currentStatus";
	private final String HEAT_VAL = "HeatVal";
	private final String DEPENDENCY = "Dependency";
	private final String DURATION = "DURATION";
	
	
	//private final String masterQuery = "SELECT DISTINCT ?SDLCPhase ?ActivityGroup ?DHAGroup ?SDLCPhase_ActivityGroup_DHAGroup ?SystemActivity ?System ?Activity ?UploadDate ?PlannedStart ?PlannedEnd ?ActualStart ?ActualEnd WHERE {{?SDLCPhase a <http://semoss.org/ontologies/Concept/SDLCPhase>}{?ActivityGroup a <http://semoss.org/ontologies/Concept/ActivityGroup>}{?DHAGroup a <http://semoss.org/ontologies/Concept/DHAGroup>}{?SDLCPhase_ActivityGroup_DHAGroup a <http://semoss.org/ontologies/Concept/SDLCPhase_ActivityGroup_DHAGroup>}{?SystemActivity a <http://semoss.org/ontologies/Concept/SystemActivity>}{?System a <http://semoss.org/ontologies/Concept/System>}{?Activity a <http://semoss.org/ontologies/Concept/Activity>}{?SDLCPhase <http://semoss.org/ontologies/Relation/Has> ?ActivityGroup}{?ActivityGroup <http://semoss.org/ontologies/Relation/Has> ?DHAGroup}{?SDLCPhase <http://semoss.org/ontologies/Relation/Has> ?SDLCPhase_ActivityGroup_DHAGroup}{?ActivityGroup <http://semoss.org/ontologies/Relation/Has> ?SDLCPhase_ActivityGroup_DHAGroup}{?DHAGroup <http://semoss.org/ontologies/Relation/Has> ?SDLCPhase_ActivityGroup_DHAGroup}{?SDLCPhase_ActivityGroup_DHAGroup <http://semoss.org/ontologies/Relation/Has> ?SystemActivity}{?SystemActivity <http://semoss.org/ontologies/Relation/Has> ?System}{?SystemActivity <http://semoss.org/ontologies/Relation/Has> ?Activity}{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/UploadDate> ?UploadDate}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/PlannedStart> ?PlannedStart}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/PlannedEnd> ?PlannedEnd}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/ActualStart> ?ActualStart}OPTIONAL{?SystemActivity <http://semoss.org/ontologies/Relation/Contains/ActualEnd> ?ActualEnd}}";
	
	//private final static String masterQuery = "data.import ( api: MHSDashboard . query ( [ c: SDLCPhase , c: MHSGroup, c:DHAGroup, c:ActivitySystem, c:System, c:Activity, c:ActivitySystemUploadDate__UploadDate, c:ActivitySystemUploadDate__PlannedStart, c:ActivitySystemUploadDate__PlannedEnd, c:ActivitySystemUploadDate__ActualStart, c:ActivitySystemUploadDate__ActualEnd], ([ c: SDLCPhase , inner.join , c: MHSGroup] , [c: MHSGroup , inner.join , c: DHAGroup], [c: DHAGroup , inner.join , c: ActivitySystem], [c: ActivitySystem , inner.join , c: System], [c: ActivitySystem , inner.join , c: Activity], [c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__UploadDate],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__PlannedStart],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__PlannedEnd],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__ActualStart],[c: ActivitySystemUploadDate , inner.join , c: ActivitySystemUploadDate__ActualEnd] ))) ; ";

//	private final static String masterQuery = "SELECT  DISTINCT SDLCPhase.SDLCPhase AS SDLCPhase , ActivityGroup.ActivityGroup AS ActivityGroup, DHAGroup.DHAGroup AS DHAGroup , SystemActivityUpload.SystemActivityUpload AS SystemActivityUpload , SystemActivityUpload.UploadDate AS UploadDate , SystemActivityUpload.ActualEnd AS ActualEnd , SystemActivityUpload.ActualStart AS ActualStart , SystemActivityUpload.PlannedStart AS PlannedStart , SystemActivityUpload.PlannedEnd AS PlannedEnd , System.System AS System , DependencySystemActivity.DependencySystemActivity AS DependencySystemActivity , SDLCPhase_ActivityGroup_DHAGroup.SDLCPhase_ActivityGroup_DHAGroup AS SDLCPhase_ActivityGroup_DHAGroup , SystemActivity.SystemActivity AS SystemActivity , SystemActivity.Duration AS Duration , Activity.Activity AS Activity, SystemOwner.SystemOwner AS SystemOwner FROM SystemOwner , DependencySystemActivity , Activity inner  join  System ON SystemOwner.System_FK = System.System inner  join  SystemActivityUpload ON Activity.SystemActivityUpload_FK = SystemActivityUpload.SystemActivityUpload inner  join  SystemActivity ON SystemActivityUpload.SystemActivity_FK = SystemActivity.SystemActivity AND DependencySystemActivity.SystemActivity_FK = SystemActivity.SystemActivity inner  join  SDLCPhase_ActivityGroup_DHAGroup ON SystemActivity.SDLCPhase_ActivityGroup_DHAGroup_FK = SDLCPhase_ActivityGroup_DHAGroup.SDLCPhase_ActivityGroup_DHAGroup inner   join  DHAGroup ON SDLCPhase_ActivityGroup_DHAGroup.DHAGroup_FK = DHAGroup.DHAGroup inner   join  ActivityGroup ON SDLCPhase_ActivityGroup_DHAGroup.ActivityGroup_FK = ActivityGroup.ActivityGroup inner  join  SDLCPhase ON SDLCPhase_ActivityGroup_DHAGroup.SDLCPhase_FK = SDLCPhase.SDLCPhase";

	static String masterPKQL = "data.import ( api: TAP_Readiness_Database . query ( [ c: SDLCPhase , c: SDLCPhase_ActivityGroup_DHAGroup , c: DHAGroup , c: ActivityGroup , c: SystemActivity , c: SystemActivityUpload__Duration , c: SystemActivityUpload , c: SystemActivityUpload__ActualEnd , c: SystemActivityUpload__ActualStart , c: SystemActivityUpload__PlannedStart , c: SystemActivityUpload__PlannedEnd , c: DependencySystemActivity , c: System , c: SystemOwner , c: Activity ] , ( [ c: SDLCPhase , left.outer.join , c: SDLCPhase_ActivityGroup_DHAGroup ] , [ c: DHAGroup , left.outer.join , c: SDLCPhase_ActivityGroup_DHAGroup ] , [ c: ActivityGroup , left.outer.join , c: SDLCPhase_ActivityGroup_DHAGroup ] , [ c: SDLCPhase_ActivityGroup_DHAGroup , left.outer.join , c: SystemActivity ] , [ c: SystemActivity , left.outer.join , c: SystemActivityUpload ] , [ c: SystemActivity , left.outer.join , c: DependencySystemActivity ] , [ c: SystemActivityUpload , left.outer.join , c: System ] , [ c: System , left.outer.join , c: SystemOwner ] , [ c: SystemActivityUpload , left.outer.join , c: Activity ] ) ) ) ; ";
	
	static String instanceOfPlaysheet = "prerna.ui.components.specific.tap.MHSDashboardDrillPlaysheet";
	
//	/**
//	 * Method to create a datamaker
//	 */
//	@Override
//	public void createData() {
//		if (this.dmComponent == null) {
//			//this.query will get the query added to the specific insight
//			this.dmComponent = new DataMakerComponent(this.engine, masterQuery);
//		}
//
//		if(this.dataFrame == null) {
//			this.dataFrame = new H2Frame();
//		}
//		this.dataFrame.processDataMakerComponent(this.dmComponent);
//	}

	
	@Override
	public void setUserId(String userId) {
		if(this.dataFrame == null) {
			this.dataFrame = new H2Frame();
		}
		this.dataFrame.setUserId(userId);
	}
	
	/**
	 * Method to create a datamaker
	 */
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		this.dmComponent = component;
		PKQLRunner run = new PKQLRunner();
		if(this.dataFrame == null) {
			this.dataFrame = new H2Frame();
		}
		run.runPKQL(masterPKQL, this.dataFrame);
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
//		returnHashMap.putAll(getUploadDate());
		return returnHashMap;
	}
	
	public Map getDataTableAlign (){
		Map<String, String> dataTableAlign = new HashMap <String, String> ();
		dataTableAlign.put("levelOne", SDLC);
		dataTableAlign.put("levelTwo", ActivityGroup);
		dataTableAlign.put("levelThree", DHA);
		dataTableAlign.put("heatValue", HEAT_VALUE);
		dataTableAlign.put("status", IS_ACTIVE);
		return dataTableAlign;
	}
	
	/**
	 * Method to pass FE values for system which will be used for the filters
	 * @return list of systems within a hashmap
	 */
	public Map getSystem() {
		Map<String, Object> returnHash = new HashMap<String, Object>();
		
		String [] selectors = new String[]{SYSTEM};
		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
		Object systems = mainHash.get("data");
		returnHash.put(SYSTEM, systems);
		return returnHash;
	}
	
	public Map getSystemOwner() {
		Map<String, Object> returnHash = new HashMap<String, Object>();
		
		String [] selectors = new String[]{SYSTEM_OWNER};
		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
		Object systemsOwner = mainHash.get("data");
		returnHash.put(SYSTEM_OWNER, systemsOwner);
		return returnHash;
	}
	
	/**
	 * Method to pass FE values for upload dates which will be used for the filters
	 * @return list of uploaded dates within a hashmap
	 */
	public Map getUploadDate() {
		Map<String, Object> returnHash = new HashMap<String, Object>();
//		
//		String [] selectors = new String[]{UPLOAD_DATE};
//		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
//		Vector<Object[]> uploadList = (Vector<Object[]>) mainHash.get("data");
		List<Date> uploadDateList = new ArrayList<Date> ();
////		List<Object> uploadDateList = new ArrayList<Object> ();
//		for(Object [] upload : uploadList) {
//			for (int i = 0; i < upload.length; ++i)	{
//				if(!upload[i].equals("null")){
//					Date uploadData;
//					try {
//						uploadData = (Date) getDateFormat().parse((String)upload[i]);
//						uploadDateList.add(uploadData);
//					} catch (ParseException e) {
//						e.printStackTrace();
//					}
//				}
//			
//			}
//		}
//	cc
//		Collections.sort(uploadDateList, Collections.reverseOrder());
//		
		String date= "7/20/2016";
		Date uploadData = null;
		try {
			uploadData = (Date) getDateFormat().parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		uploadDateList.add(uploadData);
		returnHash.put(UPLOAD_DATE, uploadDateList);
		
		
		return returnHash;
	}		
			
//			if(!plannedStartDate.equals("null")){
//			plannedStart = (Date) getDateFormat().parse(upload);
//		
//			Date[] stringArray = Arrays.copyOf(upload, upload.length, Date[].class);
////			Date uploadDate = null;
//			for (Date s: stringArray) {           
////				try {
////					uploadDate = (Date) getDateFormat().parse(s);
////				} catch (ParseException e) {
////					e.printStackTrace();
////				}
//				uploadDateList.add(s);
//		    }
//		}
	
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
		selectorList.add(PLANNED_START);
		selectorList.add(PLANNED_END);
		selectorList.add(ACTUAL_START);
		selectorList.add(ACTUAL_END);
		
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
			String plannedStartDate = (String) iteratirArr[4];
			String plannedEndDate = (String) iteratirArr[5];
			String actualStartDate = (String) iteratirArr[6];
			String actualEndDate = (String) iteratirArr[7];

			Map<String, Object> innerMap = new HashMap <String, Object> ();
			
			innerMap.put(SDLC, sdlc);
			innerMap.put(ActivityGroup, group);
			innerMap.put(DHA, dha);
			innerMap.put(HEAT_VALUE, createNewTinkerFrame(key));
			
			//calculate heat value and get min value per each heat value
			Map<String, Object> valueMap = calculateValues(plannedStartDate, plannedEndDate, actualStartDate, actualEndDate);
			boolean activityStatus = (boolean)valueMap.get(CURRENT_STATUS);
			
			
			//if key already exists in map
			if(returnMap.containsKey(key)) {
				Map<String, Object> innerMapReturn = (Map<String, Object>) returnMap.get(key);
				int numActivtyReturn = (int) innerMapReturn.get(ACTIVITY_NUM);
				boolean isActive = (boolean) innerMapReturn.get(IS_ACTIVE);
				
				//check if block has active activities
				if(isActive){
					innerMap.put(IS_ACTIVE, isActive);
				} else{
					innerMap.put(IS_ACTIVE, activityStatus);
				}
				
				innerMap.put(ACTIVITY_NUM, numActivtyReturn);
				returnMap.put(key, innerMap);
			} 
			
			//else key doesnt exist in map
			else if (!returnMap.containsKey(key)) {
				innerMap.put (ACTIVITY_NUM, 1);
				innerMap.put (IS_ACTIVE, activityStatus);
				returnMap.put(key, innerMap);
			}
		}
		return manipulateData(returnMap);
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
				if(!returnKey.equals(ACTIVITY_NUM)){
						Object returnVal = ((Entry<String, Object>) innerVal).getValue();
						innerList.add(returnVal);
				}
			}
			returnList.add(innerList);
		}
		returnMap.put("data", returnList);
		returnMap.put("headers", Arrays.asList(SDLC, IS_ACTIVE, ActivityGroup, DHA, HEAT_VALUE));

		return returnMap;
	}
	
//	public Map<String, Set<String>> getHeatValue(){
//		Map<String, Set<String>> returnMap = new HashMap <String, Set<String>> ();
//		Map<String, Object> iteratorMap = new HashMap <String, Object> ();
//
//		//set selector list
//		List<String> selectorList = new ArrayList<String> ();
//		selectorList.add(SDLC_ACTIVITYGROUP_DHA);
//		selectorList.add(ACTIVITY);
//		
//		
//		//delete duplicates
//		iteratorMap.put(TinkerFrame.DE_DUP, true);
//		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);
//
//		boolean getRawData = false;
//		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);
//
//		Set<String> activitySet = new ArraySet<String>();
//		//iterate to get data
//		while(iterator.hasNext()){
//		//get a list of all the activities for each SDLC_ACTIVITYGROUP_DHA
//			Object[] iteratirArr = iterator.next();
//			String primKey = (String) iteratirArr[0];
//			String activity = (String) iteratirArr[1];
//			if(returnMap.containsKey(primKey)) {
//				Set<String> innerMapReturn = returnMap.get(primKey);
//				innerMapReturn.add(activity);
//				returnMap.put(primKey, activitySet);
//			}
//			//else key doesnt exist in map
//			else if (!returnMap.containsKey(primKey)) {
//				activitySet.add(activity);
//				returnMap.put(primKey, activitySet);
//			}
//			
//		}
//		return returnMap;
//		
//	}

	
	public Double createNewTinkerFrame(String primKey) {

		// create frame
		TinkerFrame tFrame = new TinkerFrame();

		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		Set<String> relationNode1 = new ArraySet<String> ();
		relationNode1.add(SYSTEM_ACTIVITY);
		relationNode1.add(DURATION);
		relationNode1.add(PLANNED_START);
		relationNode1.add(PLANNED_END);
		relationNode1.add(ACTUAL_START);
		relationNode1.add(ACTUAL_END);
		edgeHash.put(SYSTEM_ACTIVITY, relationNode1);
		

		Map<String, String> dataTypeMap1 = new HashMap<String, String>();
		dataTypeMap1.put(SYSTEM_ACTIVITY, "STRING");
		dataTypeMap1.put(DURATION, "NUMBER");
		dataTypeMap1.put(PLANNED_START, "STRING");
		dataTypeMap1.put(PLANNED_END, "STRING");
		dataTypeMap1.put(ACTUAL_START, "STRING");
		dataTypeMap1.put(ACTUAL_END, "STRING");
		
		// merge edge hash and data types
		tFrame.mergeEdgeHash(edgeHash, dataTypeMap1);

		//this query will get all the distinct activities that belong to the specified primKey (SDLCPhase_ActivityGroup_DHAGroup)
		String query1 = "SELECT DISTINCT SYSTEMACTIVITY.SYSTEMACTIVITY as SystemActivity, SYSTEMACTIVITYUPLOAD.DURATION, DEPENDENCYSYSTEMACTIVITY.DEPENDENCYSYSTEMACTIVITY as SystemActivity, SYSTEMACTIVITYUPLOAD.PLANNEDSTART as PlannedStart,SYSTEMACTIVITYUPLOAD.PLANNEDEND as PlannedEnd, SYSTEMACTIVITYUPLOAD.ACTUALSTART as ActualStart, SYSTEMACTIVITYUPLOAD.ACTUALEND as ActualEnd "
				+ "FROM SYSTEMACTIVITY Inner Join DEPENDENCYSYSTEMACTIVITY ON SYSTEMACTIVITY.SYSTEMACTIVITY = DEPENDENCYSYSTEMACTIVITY.SYSTEMACTIVITY_FK Left Outer Join SYSTEMACTIVITYUPLOAD ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEMACTIVITYUPLOAD.SYSTEMACTIVITY_FK "
				+ "WHERE SYSTEMACTIVITY.SYSTEMACTIVITY IN (Select DISTINCT SYSTEMACTIVITY  From SYSTEMACTIVITY Where SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK ='"+ primKey +"')";
//				+ "WHERE SYSTEMACTIVITY.SYSTEMACTIVITY IN (Select DISTINCT SYSTEMACTIVITY  From SYSTEMACTIVITY Where SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK ='Design_System_Design_SDD')";

		
		IRawSelectWrapper iterator1 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query1);
		
		Map<Integer, Set<Integer>> cardinality1 = new HashMap<Integer, Set<Integer>>();
		Set<Integer> cardSet1 = new HashSet<Integer>();
		cardSet1.add(1);
		cardSet1.add(2);
		cardSet1.add(3);
		cardSet1.add(4);
		cardSet1.add(5);
		cardSet1.add(6);
		cardinality1.put(0, cardSet1);

		String[] headers1 = new String[]{SYSTEM_ACTIVITY, DURATION, SYSTEM_ACTIVITY, PLANNED_START, PLANNED_END, ACTUAL_START, ACTUAL_END};

		Map<String, String> logicalToTypeMap1 = new HashMap<String, String>();
		logicalToTypeMap1.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
		logicalToTypeMap1.put(DURATION, DURATION);
		logicalToTypeMap1.put(PLANNED_START, PLANNED_START);
		logicalToTypeMap1.put(PLANNED_END, PLANNED_END);
		logicalToTypeMap1.put(ACTUAL_START, ACTUAL_START);
		logicalToTypeMap1.put(ACTUAL_END, ACTUAL_END);

		List<String> dependencyList = new ArrayList<String> ();
		// iterate through and add to the tinker frame
		while(iterator1.hasNext()) {
			IHeadersDataRow nextRow = iterator1.next();
			tFrame.addRelationship(headers1, nextRow.getValues(), nextRow.getRawValues(), cardinality1, logicalToTypeMap1);
			
			String dependency = (String) nextRow.getValues()[2];
			dependencyList.add(dependency);
		}
		
		
		
		////////////SECOND QUERY
		String dependencyConcat = String.join("', '", dependencyList);
		
		String query2 = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITYUPLOAD.DURATION, SYSTEMACTIVITYUPLOAD.PLANNEDSTART as PlannedStart,SYSTEMACTIVITYUPLOAD.PLANNEDEND as PlannedEnd, SYSTEMACTIVITYUPLOAD.ACTUALSTART as ActualStart, SYSTEMACTIVITYUPLOAD.ACTUALEND as ActualEnd FROM SYSTEMACTIVITY "
				+ "Left Outer Join SYSTEMACTIVITYUPLOAD ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEMACTIVITYUPLOAD.SYSTEMACTIVITY_FK WHERE SYSTEMACTIVITY IN ('"+ dependencyConcat + "')";
		
		IRawSelectWrapper iterator2 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query2);
		
		String[] headers2 = new String[]{SYSTEM_ACTIVITY, DURATION, PLANNED_START, PLANNED_END, ACTUAL_START, ACTUAL_END};
		
		Map<Integer, Set<Integer>> cardinality2 = new HashMap<Integer, Set<Integer>>();
		Set<Integer> cardSet2 = new HashSet<Integer>();
		cardSet2.add(1);
		cardSet2.add(2);
		cardSet2.add(3);
		cardSet2.add(4);
		cardSet2.add(5);
		cardinality2.put(0, cardSet2);
		
		while(iterator2.hasNext()) {
			IHeadersDataRow nextRow = iterator2.next();
			tFrame.addRelationship(headers2, nextRow.getValues(), nextRow.getRawValues(), cardinality2, logicalToTypeMap1);
		}
		
		return calculateHeatValue(tFrame);
	}	
	
	
	private double calculateHeatValue (TinkerFrame tFrame){
		Map<String, Double> ActivityDurationtHash = new HashMap<String, Double>();
		GraphTraversal ret = tFrame.runGremlin("g.traversal().V().has('TYPE','" + SYSTEM_ACTIVITY + "').outE().inV().has('TYPE','" + SYSTEM_ACTIVITY + "').path()");
		
		ArraySet<List<Vertex>> dependencySet = new ArraySet<List<Vertex>>();
		while(ret.hasNext()) {
			org.apache.tinkerpop.gremlin.process.traversal.Path p = (Path) ret.next();
			for(int i = 0 ; i < p.size(); i++) {
				Object a = p.get(i);
				if(a instanceof Vertex) {
					if(!((Vertex) a).value("NAME").equals("_")) dependencySet.addAll(getPathsRecursion(null, (Vertex)a, new ArraySet<>()));
				}
			}
		}
		return calculateDelay(dependencySet);
	}
	
	private ArraySet<List<Vertex>> getPathsRecursion (Vertex lastVertex, Vertex nextVertex, ArraySet<List<Vertex>> pathSet) {
		List<Vertex> path = new ArrayList<Vertex>();
		
		//get the list where the last vertex in the list is 'lastVertex' from the ArraySet<ArrayList<Vertex>> and the add nextVertex to path
		Iterator itr = pathSet.iterator();
		while(itr.hasNext()){
			List<Vertex> vertexPath = (ArrayList<Vertex>) itr.next();
			if(lastVertex !=null){
				//if the item is the last item
				if(vertexPath.get(vertexPath.size() - 1).equals(lastVertex)){
					path = vertexPath;
					path.add(nextVertex);
				}
				//check if lastVertex is present anywhere in the list...if so, get the sublist of everything before and including lastVertex, then add nextVertex
				else if(vertexPath.contains(lastVertex)){
					for(Vertex vert : vertexPath) {
						path.add(vert);
						if (vert.equals(lastVertex)){
							break;
						}
					}
					path.add(nextVertex);
				}
			}
		}
		
		//if lastVertex is null or path is still null then create a new path...ie this is the first node of the path
		if(lastVertex == null){
			path.add(nextVertex);
		}
		
		//if path is still empty check 
		if(path.isEmpty()){
			path.add(lastVertex);
			path.add(nextVertex);
		}
		
		ArraySet<List<Vertex>> pathSets = new ArraySet<List<Vertex>>();
		if(!nextVertex.value("NAME").equals("_")){
			String edge = SYSTEM_ACTIVITY + "+++" + SYSTEM_ACTIVITY;
			Iterator<Edge> activityEdgeIt = nextVertex.edges(Direction.OUT, "TYPE", edge);
			
			pathSets.add(path);
			
			//check if nextVertex has a child/children 
			while(activityEdgeIt.hasNext()){
				pathSets.addAll(getPathsRecursion(nextVertex, activityEdgeIt.next().inVertex(), pathSets));
			}
		} else {
			pathSets.add(path);
		}
		return pathSets;
	}
	
	private ArraySet<ArrayList<Vertex>> getPathsRecursion (Vertex nextVertex, ArrayList<Vertex> previousPath) {
		ArrayList<Vertex> path = new ArrayList<Vertex>();
		path.addAll(previousPath);
		path.add(nextVertex);
		
		ArraySet<ArrayList<Vertex>> paths = new ArraySet<ArrayList<Vertex>>();
		System.out.println("Activity name:::: " + nextVertex.value("NAME"));
		if(!nextVertex.value("NAME").equals("_")){
			String edge = SYSTEM_ACTIVITY + "+++" + SYSTEM_ACTIVITY;
			Iterator<Edge> activityEdgeIt = nextVertex.edges(Direction.OUT, "TYPE", edge);
			
			//check if nextVertex has a child/children 
			while(activityEdgeIt.hasNext()){
				paths.addAll(getPathsRecursion(activityEdgeIt.next().inVertex(), path));
			}
			if(!activityEdgeIt.hasNext()){
				paths.add(path);
			}
			
		} else {
			paths.add(path);
		}
		return paths;
	}
	
	private double calculateDelay (Set<List<Vertex>> vertexPathSet){			

		List<Double> pathSumList = new ArrayList<Double> ();
		if(!(vertexPathSet.isEmpty())){
			for(List<Vertex> vertexPathList : vertexPathSet){

				Date todaysDate = Calendar.getInstance().getTime();

				Double sum = 0.0;
				for(Vertex vert: vertexPathList){

					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
					Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("VALUE"));

					Iterator<Edge> plannedStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_START);
					String vertexPlannedStart = plannedStartEdgeIt.next().inVertex().value("NAME");

					Iterator<Edge> plannedEndEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_END);
					String vertexPlannedEnd = plannedEndEdgeIt.next().inVertex().value("NAME");

					Iterator<Edge> actualStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_START);
					String vertexActualStart = actualStartEdgeIt.next().inVertex().value("NAME");

					Iterator<Edge> actualEndEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_END);
					String vertexActualEnd = actualEndEdgeIt.next().inVertex().value("NAME");

					Map<String, Date> dateHash = convertStringDates(vertexPlannedStart, vertexPlannedEnd, vertexActualStart, vertexActualEnd);
					Date plannedStartDate = dateHash.get(PLANNED_START);
					Date plannedEndDate = dateHash.get(PLANNED_END);
					Date actualStartDate = dateHash.get(ACTUAL_START);
					Date actualEndDate = dateHash.get(ACTUAL_END);

					Date projectedStart = null;

					//ALGORITHM
					if(actualStartDate == null) {
						if(plannedStartDate == null || plannedStartDate.before(todaysDate)){
							projectedStart = todaysDate;
						}else {
							projectedStart = plannedStartDate;
						}
					}else if(!(actualStartDate == null)){
						projectedStart = actualStartDate;
					}

					Date projectedEnd = addToDate(projectedStart, vertexDuration);

					//calculate total amount of days this activity will be delayed
					double delay= 0.0;
					if(!(plannedEndDate == null)){
						delay = (double) ((projectedEnd.getTime() - plannedEndDate.getTime())/(24 * 60 * 60 * 1000));
					} else {
						delay = (double) ((projectedEnd.getTime() - projectedStart.getTime())/(24 * 60 * 60 * 1000));
					}
					sum += delay;
				}
				pathSumList.add(sum);
			}
			System.out.println("Max Value:::: " +  Collections.max(pathSumList));
			return Collections.max(pathSumList);
		} else {
			return 0.0;
		}
	}
	
	private Date addToDate (Date date, Double duration){
		Date projectedEnd = null;
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, duration.intValue());
		
		return cal.getTime();
	}
	
	
	private double getLongestPathSum (Set<List<Vertex>> vertexPathSet){
		List<Double> pathSumList = new ArrayList<Double> ();
		for(List<Vertex> vertexPathList : vertexPathSet){
			
			Double sum = 0.0;
			for(Vertex vert: vertexPathList){
				String edge = SYSTEM_ACTIVITY + "+++" + DURATION;
				Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", edge);
				Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("VALUE"));
				System.out.println(">>> duration is " + vertexDuration);
				sum += vertexDuration;
			}
			pathSumList.add(sum);
		}
		System.out.println("Max Value:::: " +  Collections.max(pathSumList));
		return Collections.max(pathSumList);
	}
	
	
	private double getPrimKeyActivitySum (Map<String, Double> ActivityDurationtHash) {
		//...FOR NOW I WILL JUST ADD ALL THE ACTIVITES of the same primKey together...but that will probably change 
		//because what if all the initial sub activies start at the same time...then it wouldn't make sense to add them (we should the look at the start date) 
		Double sumAllCriticalPaths = 0.0;
		for(String activityKey : ActivityDurationtHash.keySet()){
			Double activityPathDuration = ActivityDurationtHash.get(activityKey);
			sumAllCriticalPaths += activityPathDuration;
		}
		
		System.out.println("Sum::::::: " + sumAllCriticalPaths);
		return sumAllCriticalPaths;
	}
	
	public Map<String, Date> convertStringDates (String plannedStartDate, String plannedEndDate, String actualStartDate, String actualEndDate) {
		Date plannedStart = null;
		Date plannedEnd = null;
		Date actualStart = null;
		Date actualEnd = null;
		Map<String, Date> dateHash = new HashMap<String, Date>();
		//format string date to type Date
		try {
			if(!plannedStartDate.equals("null") && !plannedStartDate.equals("_")){
				plannedStart = (Date) getDateFormat().parse(plannedStartDate);
			}
			if(!plannedEndDate.equals("null") && !plannedEndDate.equals("_")){
				plannedEnd = (Date) getDateFormat().parse(plannedEndDate);
			}
			if(!actualStartDate.equals("null") && !actualStartDate.equals("_")){
				actualStart = (Date) getDateFormat().parse(actualStartDate);
			}
			if(!actualEndDate.equals("null") && !actualEndDate.equals("_")){
				actualEnd = (Date) getDateFormat().parse(actualEndDate);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		dateHash.put(PLANNED_START, plannedStart);
		dateHash.put(PLANNED_END, plannedEnd);
		dateHash.put(ACTUAL_START, actualStart);
		dateHash.put(ACTUAL_END, actualEnd);
		
		return dateHash; 
	}

	/**
	 * Method to calculate heat value based on the start and end planned and actual dates
	 * @param plannedStartDate String planned start date
	 * @param plannedEndDate String planned end date
	 * @param actualStartDate String actual start date
	 * @param actualEndDate String actual end date
	 * @return Hashmap<String, Long> where "HeatVal" is the heat value for each dha and "MinVal" is the lowest heat value for each DHA  
	 */
	public Map<String, Object> calculateValues (String plannedStartDate, String plannedEndDate, String actualStartDate, String actualEndDate) {
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
		
		Map<String, Object> returnMap = new HashMap<String, Object> ();
		Date todaysDate = Calendar.getInstance().getTime();
		boolean isActive = false;
		int CriticalPathValue = 0;
		
//		// COMPLETED: if there are both 'planned' and 'actual' dates : (planned end date - actual end date)
//		if(plannedStart !=null && plannedEnd !=null && actualStart !=null && actualEnd !=null){
//			heatValue = (int) ((plannedEnd.getTime() - actualEnd.getTime())/(24 * 60 * 60 * 1000));
//		}
//		
//		// HASN'T BEGUN: if there are no 'actual' dates : (planned start - today's date) if planned start date is greater than today's date
//		else if (plannedStart !=null && plannedEnd !=null && actualStart == null && actualEnd == null) {
//			if(plannedStart.getTime() < todaysDate.getTime()){
//				heatValue = (int) ((plannedStart.getTime() - todaysDate.getTime())/(24 * 60 * 60 * 1000));
//			} else {
//				heatValue = 0;
//			}
//		}  
//		
//		// STILL ACTIVE: if there is no 'actualend' date : (planned start date - actual start date)
		if(plannedStart !=null && plannedEnd !=null && actualStart != null && actualEnd == null ){
//			heatValue = (int) ((plannedStart.getTime() - actualStart.getTime())/(24 * 60 * 60 * 1000));
			//only get a min value in this case..otherwise minValue will remain 0
			isActive = true;
		} 
//		//if there is row with absolutely no dates then it will not be calculated into the heat value
//		else{
//			heatValue = 0;
//		}
//		returnMap.put(HEAT_VAL, heatValue);
		returnMap.put(CURRENT_STATUS, isActive);
		
		return returnMap;
	}
	
	public static DateFormat getDateFormat() {
		//formating date by month-day-year
		return new SimpleDateFormat("MM-dd-yyyy");
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