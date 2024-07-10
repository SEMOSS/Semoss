//package prerna.ui.components.specific.tap;
//
//import java.text.DateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Calendar;
//import java.util.Collections;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Hashtable;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Set;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//import org.apache.tinkerpop.gremlin.process.traversal.Path;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
//import org.apache.tinkerpop.gremlin.structure.Direction;
//import org.apache.tinkerpop.gremlin.structure.Edge;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//
//import edu.stanford.nlp.util.ArraySet;
//import prerna.ds.TinkerFrame;
//import prerna.ds.h2.H2Frame;
//import prerna.engine.api.IEngine;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.sablecc.PKQLRunner;
//import prerna.ui.components.playsheets.TablePlaySheet;
//import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
//import prerna.ui.components.playsheets.datamakers.IDataMaker;
//
//public class MHSDashboardDrillPlaysheet extends TablePlaySheet implements IDataMaker {
//
//	private DataMakerComponent dmComponent;
//
//	private final String SDLC = "SDLCPhase";
//	private final String ActivityGroup = "ActivityGroup";
//	private final String DHA = "DHAGroup";
//	private final String SDLC_ACTIVITYGROUP_DHA = "SDLCPhase_ActivityGroup_DHAGroup";
//	private final String SYSTEM_ACTIVITY = "SYSTEMACTIVITY";
//	private final String SYSTEM = "System";
//	private final String SYSTEM_OWNER = "SystemOwner";
//	private final String HEAT_VALUE = "HeatValue";
//	private final String PLANNED_START = "PlannedStart";
//	private final String ACTUAL_START = "ActualStart";
//	private final String ACTUAL_END = "ActualEnd";
//	private final String STATUS = "STATUS";
//	private final String DURATION = "DURATION";
//	private final String DEVIATION = "Deviation";
//	private final String DEVIATION_START = "DeviationStart";
//	private final String DEVIATION_FINISH = "DeviationFinish";
//	private final String EARLY_START = "EarlyStart";
//	private final String EARLY_FINISH = "EarlyFinish";
//	private final String LATE_START = "LateStart";
//	private final String LATE_FINISH = "LateFinish";
//	private final String SLACK = "Slack";
//	private final String DELAY = "Delay";
//	private final String CRITICAL_PATH = "CriticalPath";
//	private final String PLANNED_LF = "PlannedLF";
//
//	private final Date todaysDate = Calendar.getInstance().getTime();
//	
//	static String masterPKQL = "data.import(api: %engineName%. query ( [ c: SDLCPhase , c: SDLCPhase_ActivityGroup_DHAGroup ,  c: SDLCPhase_ActivityGroup_DHAGroup__HeatValue , c: ActivityGroup , c: DHAGroup , c: SystemActivity , c: SystemActivity__ActualEnd , c: SystemActivity__Duration , c: SystemActivity__LateFinish , c: SystemActivity__Delay , c: SystemActivity__CriticalPath , c: SystemActivity__LateStart , c: SystemActivity__EarlyFinish , c: SystemActivity__EarlyStart , c: SystemActivity__ActualStart , c: SystemActivity__Slack , c: SystemActivity__KeyStatus , c: SystemActivity__Deviation , c: SystemActivity__DeviationStart , c: SystemActivity__DeviationFinish , c: SystemActivity__PlannedLF , c: System , c: System__PlannedStart , c: SystemOwner , c: Activity , c: DependencySystemActivity ] , ( [ c: SDLCPhase , left.outer.join , c: SDLCPhase_ActivityGroup_DHAGroup ] , [ c: ActivityGroup , left.outer.join , c: SDLCPhase_ActivityGroup_DHAGroup ] , [ c: DHAGroup , left.outer.join , c: SDLCPhase_ActivityGroup_DHAGroup ] , [ c: SDLCPhase_ActivityGroup_DHAGroup , left.outer.join , c: SystemActivity ] , [ c: SystemActivity , left.outer.join , c: System ] , [ c: System , left.outer.join , c: SystemOwner ] , [ c: SystemActivity , left.outer.join , c: Activity ] , [ c: SystemActivity , left.outer.join , c: DependencySystemActivity ] ) ) ) ;";
//	static String instanceOfPlaysheet = "prerna.ui.components.specific.tap.MHSDashboardDrillPlaysheet";
//
//	// /**
//	// * Method to create a datamaker
//	// */
//	// @Override
//	public void createData() {
//		// if (this.dmComponent == null) {
//		// //this.query will get the query added to the specific insight
//		// this.dmComponent = new DataMakerComponent(this.engine, masterQuery);
//		// }
//		//
//		// if(this.dataFrame == null) {
//		// this.dataFrame = new H2Frame();
//		// }
//		// this.dataFrame.processDataMakerComponent(this.dmComponent);
//	}
//
//	@Override
//	public void setUserId(String userId) {
//		if (this.dataFrame == null) {
//			this.dataFrame = new H2Frame();
//		}
//		this.dataFrame.setUserId(userId);
//	}
//
//	/**
//	 * Method to create a datamaker
//	 */
//	@Override
//	public void processDataMakerComponent(DataMakerComponent component) {
//		this.dmComponent = component;
//		IEngine engine = component.getEngine();
//		String engineName = engine.getEngineName();
//		masterPKQL = masterPKQL.replace("%engineName%", engineName);
//		PKQLRunner run = new PKQLRunner();
//		if (this.dataFrame == null) {
//			this.dataFrame = new H2Frame();
//		}
//		run.runPKQL(masterPKQL, this.dataFrame);
//	}
//
//	/**
//	 * Method to pass the FE the ordered list of SDLC phases and all values necessary for the main table
//	 * @return returns data, headers, dataTableAlign, systems, and upload date
//	 */
//	@Override
//	public Map getDataMakerOutput(String... selectors) {
//		Map<String, Object> returnHashMap = aggregateDHAGroup();
//		returnHashMap.putAll(getData());
//		return returnHashMap;
//	}
//
//	/**
//	 * Method to pass FE all the other necessary date, including: sdlc list, the kind of styling to use (ie. MHS), 
//	 * data table align, and a set of the filterable system and system owners
//	 * @return Map
//	 */
//	private Map getData () {
//		Map<String, Object> returnHashMap = new HashMap<String, Object> ();
//		List<Object> sdlcList = new ArrayList<Object>(Arrays.asList("Strategy", "Requirement", "Design", "Development", "Test", "Security", "Deployment", "Training"));
//		Map<String, String> dataTableAlign = new HashMap<String, String>();
//		returnHashMap.put("SDLCList", sdlcList);
//		returnHashMap.put("styling", "MHS");
//		returnHashMap.put("dataTableAlign", getDataTableAlign());
//		returnHashMap.putAll(getSystem());
//		returnHashMap.putAll(getSystemOwner());
//		return returnHashMap;
//	}
//	
//	/**
//	 * Method to pass FE the table alignment of the data
//	 */
//	public Map getDataTableAlign() {
//		Map<String, String> dataTableAlign = new HashMap<String, String>();
//		dataTableAlign.put("levelOne", SDLC);
//		dataTableAlign.put("levelTwo", ActivityGroup);
//		dataTableAlign.put("levelThree", DHA);
//		dataTableAlign.put(STATUS, STATUS);
//		dataTableAlign.put("heatValue", HEAT_VALUE);
//		dataTableAlign.put("earlyStart", EARLY_START);
//		dataTableAlign.put("lateFinish", LATE_FINISH);
//		return dataTableAlign;
//	}
//
//	/**
//	 * Method to iterated through specified selectors and calculate the heat values per each DHA group
//	 * @return Hashmap contain SDLC, Group, DHA, heat value, minimum heat value
//	 */
//	public Map aggregateDHAGroup() {
//		Map<String, Object> returnMap = new HashMap<String, Object>();
//		Map<String, Object> iteratorMap = new HashMap<String, Object>();
//
//		// set selector list
//		List<String> selectorList = new ArrayList<String>();
//		selectorList.add(SDLC);
//		selectorList.add(ActivityGroup);
//		selectorList.add(DHA);
//		selectorList.add(SDLC_ACTIVITYGROUP_DHA);
//
//		// delete duplicates
//		iteratorMap.put(TinkerFrame.DE_DUP, true);
//		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);
//
//		Iterator<Object[]> iterator = dataFrame.iterator(iteratorMap);
//
//		
//		//Get the data with the actual dates
//		TinkerFrame tFrame = createNewTinkerFrame();
//		HashMap<String, Object> pathMap = getPath (tFrame);
//		ArraySet<List<Vertex>> pathSet = (ArraySet<List<Vertex>>) pathMap.get("Path");
//		Set<Vertex> vertexSet = (Set<Vertex>) pathMap.get("Vertex");
//		
//		//Get the planned completion date
//		getPlannedEarlyDates(pathSet);
//		getPlannedLateDates(pathSet, tFrame);
//		
//		//Get all the actual information
//		getActualEarlyDates(pathSet, tFrame);
//		getActualLateDates(pathSet);
//
//		addToFrame(tFrame);
//		calculateSlack(vertexSet);
//
//		// iterate to get data
//		while (iterator.hasNext()) {
//			Object[] iteratirArr = iterator.next();
//			String sdlc = (String) iteratirArr[0];
//			String group = (String) iteratirArr[1];
//			String dha = (String) iteratirArr[2];
//			String key = (String) iteratirArr[3];
//
//			Map<String, Object> innerMap = new HashMap<String, Object>();
//
//			if (!returnMap.containsKey(key)) {
//				innerMap.put(SDLC, sdlc);
//				innerMap.put(ActivityGroup, group);
//				innerMap.put(DHA, dha);
//				// get the rest of the information for the tile: early start, late start, heat value, status
//				innerMap.putAll(getTileInfo(key));
//				returnMap.put(key, innerMap);
//			}
//		}
//		return manipulateData(returnMap);
//	}
//
//	/**
//	 * Method to return all the values need to display on the white tile
//	 * @param primKey the unique key
//	 * @return map of early start, late start, heat value, status
//	 */
//	private Map<String, Object> getTileInfo(String primKey) {
//		//query to get the slack, early start, late finish, status, and delay based on the prime key
//		String query = "SELECT SLACK as SLACK, EARLYSTART AS EARLYSTART, LATEFINISH AS LATEFINISH, KEYSTATUS, DELAY FROM SYSTEMACTIVITY where SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK ='" + primKey + "'";
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		Object slack = null;
//		String earlyStart = null;
//		String lateFinish = null;
//		String status = null;
//		String delay = null;
//
//		List<Double> slackList = new ArrayList<Double>();
//		List<Date> esList = new ArrayList<Date>();
//		List<Date> lfList = new ArrayList<Date>();
//		List<String> statusList = new ArrayList<String>();
//		List<String> delayList = new ArrayList<String>();
//
//		//iterates and gets specified values in rows
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			if (!(nextRow.getRawValues() == null)) {
//				slack = nextRow.getRawValues()[0];
//				if (slack != null) {
//					slackList.add(Double.parseDouble((String) slack));
//				}
//				earlyStart = (String) nextRow.getRawValues()[1];
//				lateFinish = (String) nextRow.getRawValues()[2];
//				status = (String) nextRow.getRawValues()[3];
//				delay = (String) nextRow.getRawValues()[4];
//				statusList.add(status);
//				delayList.add(delay);
//				try {
//					esList.add((Date) getDateFormat().parse(earlyStart));
//					lfList.add((Date) getDateFormat().parse(lateFinish));
//				} catch (ParseException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//
//		double heatValue = 0.0;
//		Map<String, Object> returnMap = new HashMap<String, Object>();
//		if (!slackList.isEmpty()) {
//			heatValue = Collections.min(slackList);
//		} 
//		
//		returnMap.put(EARLY_START, getDateFormat(Collections.min(esList)));
//		returnMap.put(LATE_FINISH, getDateFormat(Collections.max(lfList)));
//
//		String groupStatus = null;
//
//		if (statusList.contains("active")) {
//			groupStatus = "active";
//		} else if (statusList.contains("projected")) {
//			groupStatus = "projected";
//		} else {
//			groupStatus = "completed";
//			if (!delayList.isEmpty()) {
//				heatValue = Double.parseDouble(Collections.max(delayList));
//			}
//		}
//		
//		//query to update H2
//		String updateDBquery = "UPDATE SDLCPHASE_ACTIVITYGROUP_DHAGROUP SET HEATVALUE =" + heatValue + " WHERE SDLCPHASE_ACTIVITYGROUP_DHAGROUP = '"+ primKey +"'";
//		dmComponent.getEngine().insertData(updateDBquery);
//		
//		returnMap.put(HEAT_VALUE, heatValue);
//		returnMap.put(STATUS, groupStatus);
//
//		return returnMap;
//	}
//
//	/**
//	 * Method to format date the way FE expects it...ie without the string keys that objMap currently contains
//	 * @param objMap 
//	 * @return Hashmap
//	 */
//	public Map manipulateData(Map<String, Object> objMap) {
//		Map<String, List<Object>> returnMap = new HashMap<String, List<Object>>();
//		List<Object> returnList = new ArrayList<Object>();
//
//		for (Entry<String, Object> entry : objMap.entrySet()) {
//			HashMap value = (HashMap) entry.getValue();
//			List<Object> innerList = new ArrayList<Object>();
//
//			for (Object innerVal : value.entrySet()) {
//				Object returnVal = ((Entry<String, Object>) innerVal).getValue();
//				innerList.add(returnVal);
//			}
//			returnList.add(innerList);
//		}
//		returnMap.put("data", returnList);
//		returnMap.put("headers", Arrays.asList(STATUS, SDLC, ActivityGroup, DHA, EARLY_START, LATE_FINISH, HEAT_VALUE));
//
//		return returnMap;
//	}
//
//	/**
//	 * Method to create a tinkerframe
//	 * @return
//	 */
//	public TinkerFrame createNewTinkerFrame() {
//
//		// create frame
//		TinkerFrame tFrame = new TinkerFrame();
//
//		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
//		Set<String> relationNode = new ArraySet<String>();
//		relationNode.add(SYSTEM_ACTIVITY);
//		relationNode.add(DURATION);
//		relationNode.add(PLANNED_START);
//		relationNode.add(ACTUAL_START);
//		relationNode.add(ACTUAL_END);
//		edgeHash.put(SYSTEM_ACTIVITY, relationNode);
//
//		Map<String, String> dataTypeMap = new HashMap<String, String>();
//		dataTypeMap.put(SYSTEM_ACTIVITY, "STRING");
//		dataTypeMap.put(DURATION, "NUMBER");
//		dataTypeMap.put(ACTUAL_START, "STRING");
//		dataTypeMap.put(ACTUAL_END, "STRING");
//		dataTypeMap.put(PLANNED_START, "STRING");
//
//		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
//		logicalToTypeMap.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
//		logicalToTypeMap.put(DURATION, DURATION);
//		logicalToTypeMap.put(ACTUAL_START, ACTUAL_START);
//		logicalToTypeMap.put(ACTUAL_END, ACTUAL_END);
//		logicalToTypeMap.put(PLANNED_START, PLANNED_START);
//
//		// merge edge hash and data types
//		tFrame.mergeEdgeHash(edgeHash, dataTypeMap);
//
//		// this query will get all the distinct activities that belong to the specified primKey (SDLCPhase_ActivityGroup_DHAGroup)
//		String query1 = "SELECT DISTINCT SYSTEMACTIVITY.SYSTEMACTIVITY as SystemActivity, SystemActivity.DURATION, DEPENDENCYSYSTEMACTIVITY.DEPENDENCYSYSTEMACTIVITY as SystemActivity, SystemActivity.ACTUALSTART as ActualStart, SystemActivity.ACTUALEND as ActualEnd, SYSTEM.PLANNEDSTART "
//				+ "FROM SYSTEMACTIVITY "
//				+ "Left Join DEPENDENCYSYSTEMACTIVITY ON SYSTEMACTIVITY.SYSTEMACTIVITY = DEPENDENCYSYSTEMACTIVITY.SYSTEMACTIVITY_FK "
//				+ "Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM.SYSTEMACTIVITY_FK ";
//
//		IRawSelectWrapper iterator1 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query1);
//
//		Map<Integer, Set<Integer>> cardinality1 = new HashMap<Integer, Set<Integer>>();
//		Set<Integer> cardSet1 = new HashSet<Integer>();
//		cardSet1.add(1);
//		cardSet1.add(2);
//		cardSet1.add(3);
//		cardSet1.add(4);
//		cardSet1.add(5);
//		cardinality1.put(0, cardSet1);
//
//		String[] headers1 = new String[] { SYSTEM_ACTIVITY, DURATION, SYSTEM_ACTIVITY, ACTUAL_START, ACTUAL_END, PLANNED_START };
//
//		List<String> dependencyList = new ArrayList<String>();
//		// iterate through and add to the tinker frame
//		while (iterator1.hasNext()) {
//			IHeadersDataRow nextRow = iterator1.next();
//			tFrame.addRelationship(headers1, nextRow.getValues(), cardinality1, logicalToTypeMap);
//
//			// get a dependency list of all the non-null values for the second query
//			if (!(nextRow.getValues()[2] == null)) {
//				String dependency = (String) nextRow.getValues()[2];
//				dependencyList.add(dependency);
//			}
//		}
//
//		//////////// SECOND QUERY /////////////////////
//		if (!dependencyList.isEmpty()) {
//			String dependencyConcat = String.join("', '", dependencyList);
//
//			// this query will get all the properties of the dependency activities
//			String query2 = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITY.DURATION, SYSTEMACTIVITY.ACTUALSTART as ActualStart, SYSTEMACTIVITY.ACTUALEND as ActualEnd, SYSTEM.PLANNEDSTART "
//					+ "FROM SYSTEMACTIVITY Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM .SYSTEMACTIVITY_FK WHERE SYSTEMACTIVITY IN ('"
//					+ dependencyConcat + "')";
//
//			IRawSelectWrapper iterator2 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query2);
//
//			String[] headers2 = new String[] { SYSTEM_ACTIVITY, DURATION, ACTUAL_START, ACTUAL_END, PLANNED_START };
//
//			Map<Integer, Set<Integer>> cardinality2 = new HashMap<Integer, Set<Integer>>();
//			Set<Integer> cardSet2 = new HashSet<Integer>();
//			cardSet2.add(1);
//			cardSet2.add(2);
//			cardSet2.add(3);
//			cardSet2.add(4);
//			cardinality2.put(0, cardSet2);
//
//			while (iterator2.hasNext()) {
//				IHeadersDataRow nextRow = iterator2.next();
//				tFrame.addRelationship(headers2, nextRow.getValues(), cardinality2, logicalToTypeMap);
//			}
//		}
//
//		return tFrame;
//	}
//
//	/**
//	 * Method to add the Devation property to frame
//	 * @param tFrame
//	 */
//	private void addDevationToFrame(TinkerFrame tFrame) {
//
//		// add to frame
//		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
//		Set<String> relationNode = new ArraySet<String>();
//		relationNode.add(SYSTEM_ACTIVITY);
//		relationNode.add(DEVIATION);
//		edgeHash.put(SYSTEM_ACTIVITY, relationNode);
//
//		Map<String, String> dataTypeMap = new HashMap<String, String>();
//		dataTypeMap.put(SYSTEM_ACTIVITY, "STRING");
//		dataTypeMap.put(DEVIATION, "STRING");
//
//		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
//		logicalToTypeMap.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
//		logicalToTypeMap.put(DEVIATION, DEVIATION);
//
//		String query = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITY.DEVIATION FROM SYSTEMACTIVITY Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM .SYSTEMACTIVITY_FK ";
//
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		String[] headers = new String[] { SYSTEM_ACTIVITY, DEVIATION};
//
//		Map<Integer, Set<Integer>> cardinality = new HashMap<Integer, Set<Integer>>();
//		Set<Integer> cardSet = new HashSet<Integer>();
//		cardSet.add(1);
//		cardinality.put(0, cardSet);
//
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			tFrame.addRelationship(headers, nextRow.getValues(), cardinality, logicalToTypeMap);
//		}
//	}
//	
//	/**
//	 * Method to add planned late finish to tinkerframe
//	 * @param tFrame
//	 */
//	private void addPlannedLFToFrame(TinkerFrame tFrame) {
//
//		// add to frame
//		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
//		Set<String> relationNode = new ArraySet<String>();
//		relationNode.add(SYSTEM_ACTIVITY);
//		relationNode.add(PLANNED_LF);
//		edgeHash.put(SYSTEM_ACTIVITY, relationNode);
//
//		Map<String, String> dataTypeMap = new HashMap<String, String>();
//		dataTypeMap.put(SYSTEM_ACTIVITY, "STRING");
//		dataTypeMap.put(PLANNED_LF, "STRING");
//
//		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
//		logicalToTypeMap.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
//		logicalToTypeMap.put(PLANNED_LF, PLANNED_LF);
//
//		String query = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITY.PLANNEDLF FROM SYSTEMACTIVITY Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM .SYSTEMACTIVITY_FK ";
//
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		String[] headers = new String[] { SYSTEM_ACTIVITY, PLANNED_LF};
//
//		Map<Integer, Set<Integer>> cardinality = new HashMap<Integer, Set<Integer>>();
//		Set<Integer> cardSet = new HashSet<Integer>();
//		cardSet.add(1);
//		cardinality.put(0, cardSet);
//
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			tFrame.addRelationship(headers, nextRow.getValues(), cardinality, logicalToTypeMap);
//		}
//	}
//	
//	/**
//	 * Method to add the newly calculated values to the frame
//	 * @param tFrame
//	 */
//	private void addToFrame(TinkerFrame tFrame) {
//
//		// add to frame
//		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
//		Set<String> relationNode = new ArraySet<String>();
//		relationNode.add(SYSTEM_ACTIVITY);
//		relationNode.add(EARLY_START);
//		relationNode.add(EARLY_FINISH);
//		relationNode.add(LATE_START);
//		relationNode.add(LATE_FINISH);
//		edgeHash.put(SYSTEM_ACTIVITY, relationNode);
//
//		Map<String, String> dataTypeMap = new HashMap<String, String>();
//		dataTypeMap.put(SYSTEM_ACTIVITY, "STRING");
//		dataTypeMap.put(EARLY_START, "NUMBER");
//		dataTypeMap.put(EARLY_FINISH, "STRING");
//		dataTypeMap.put(LATE_START, "NUMBER");
//		dataTypeMap.put(LATE_FINISH, "STRING");
//
//		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
//		logicalToTypeMap.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
//		logicalToTypeMap.put(EARLY_START, EARLY_START);
//		logicalToTypeMap.put(EARLY_FINISH, EARLY_FINISH);
//		logicalToTypeMap.put(LATE_START, LATE_START);
//		logicalToTypeMap.put(LATE_FINISH, LATE_FINISH);
//
//		String query = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITY.EARLYSTART, SYSTEMACTIVITY.EARLYFINISH, SYSTEMACTIVITY.LATESTART, SYSTEMACTIVITY.LATEFINISH "
//				+ "FROM SYSTEMACTIVITY Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM .SYSTEMACTIVITY_FK ";
//
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		String[] headers = new String[] { SYSTEM_ACTIVITY, EARLY_START, EARLY_FINISH, LATE_START, LATE_FINISH };
//
//		Map<Integer, Set<Integer>> cardinality = new HashMap<Integer, Set<Integer>>();
//		Set<Integer> cardSet = new HashSet<Integer>();
//		cardSet.add(1);
//		cardSet.add(2);
//		cardSet.add(3);
//		cardSet.add(4);
//		cardinality.put(0, cardSet);
//
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			tFrame.addRelationship(headers, nextRow.getValues(), cardinality, logicalToTypeMap);
//		}
//	}
//
//	/**
//	 * Method to get a set list of all the unique nodes that are the first of the paths
//	 * @return
//	 */
//	private List<String> getFirstNodeList() {
//		String query = "SELECT DISTINCT SYSTEMACTIVITY from SYSTEMACTIVITY WHERE SYSTEMACTIVITY.SYSTEMACTIVITY NOT IN (SELECT DEPENDENCYSYSTEMACTIVITY FROM DEPENDENCYSYSTEMACTIVITY)";
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		List<String> initialNodeList = new ArrayList<String>();
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			if (!(nextRow.getRawValues() == null)) {
//				initialNodeList.add((String) nextRow.getRawValues()[0]);
//			}
//		}
//		return initialNodeList;
//	}
//
//	/**
//	 * Get a set list of all the paths contained in the tinkerframe
//	 * @param tFrame
//	 * @return
//	 */
//	private HashMap<String, Object> getPath (TinkerFrame tFrame) {
//
//		GraphTraversal ret = tFrame.runGremlin("g.traversal().V().has('TYPE','" + SYSTEM_ACTIVITY + "').outE().inV().has('TYPE','" + SYSTEM_ACTIVITY + "').path()");
//		ArraySet<List<Vertex>> pathSet = new ArraySet<List<Vertex>>();
//		Set<Vertex> vertexSet = new ArraySet<Vertex>();
//		List<String> initialNodeList = getFirstNodeList();
//
//		while (ret.hasNext()) {
//			org.apache.tinkerpop.gremlin.process.traversal.Path p = (Path) ret.next();
//			for (int i = 0; i < p.size(); i++) {
//				Object a = p.get(i);
//				if (a instanceof Vertex) {
//					String vertName = ((Vertex) a).value("NAME");
//					if (!(vertName.equals("_"))) {
//						vertexSet.add((Vertex) a);
//						if (initialNodeList.contains(vertName)) {
//							pathSet.addAll(getPathsRecursion ((Vertex) a, new ArrayList<Vertex>()));
//						}
//					}
//				}
//			}
//		}
//	
//		// HERE Print statements of parent child relationship
//		for (Vertex parent : vertexSet) {
//			Iterator<Edge> edgeiterator = parent.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + SYSTEM_ACTIVITY);
//			while (edgeiterator.hasNext()) {
//				String parentstring = ((Vertex) parent).value("NAME");
//				String children = edgeiterator.next().inVertex().value("NAME");
//				System.out.println("Path:::::::" + parentstring + " : " + children);
//			}
//		}
//		
//		HashMap<String, Object> returnMap = new HashMap <String, Object> ();
//		returnMap.put("Path", pathSet);
//		returnMap.put("Vertex", vertexSet);
//
//		return returnMap;
//	}
//
//	/**
//	 * Method to determine the path of the all the vertices 
//	 * @param currentVertex vertex
//	 * @param path list of all the previous paths 
//	 * @return A set of all the possible paths
//	 */
//	private ArraySet<List<Vertex>> getPathsRecursion (Vertex currentVertex, ArrayList<Vertex> path) {
//		path.add(currentVertex);
//		Iterator<Edge> edgeiterator = currentVertex.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + SYSTEM_ACTIVITY);
//		ArraySet<List<Vertex>> paths = new ArraySet<List<Vertex>>();
//		while (edgeiterator.hasNext()) {
//
//			Vertex childNode = edgeiterator.next().inVertex();
//
//			if (childNode.value("NAME").equals("_")) {
//				paths.add(path);
//			} else {
//				ArrayList<Vertex> newPath = new ArrayList<Vertex>();
//				newPath.addAll(path);
//				paths.addAll(getPathsRecursion (childNode, newPath));
//			}
//		}
//		return paths;
//	}
//
//	///////////////////////////////////////////          CALCULATE PLANNED & ACUTAL DATES            //////////////////////////////////////////////
//	
//	/**
//	 * Planned methods will be initially run to determine the earliest date of completion for the project without taking the actual dates into account
//	 * @param vertexPathSet Set of all the path lists
//	 * @return
//	 */
//	private void getPlannedEarlyDates (Set<List<Vertex>> vertexPathSet) {
//		List<Double> pathSumList = new ArrayList<Double>();
//
//		for (List<Vertex> vertexPathList : vertexPathSet) {
//
//			Double sum = 0.0;
//			int i = 0;
//			Date ES = null;
//			Date EF = null;
//
//			for (Vertex vert : vertexPathList) {
//
//				if (!vert.value("NAME").equals("_")) {
//					String systemActivity = vert.value("NAME");
//					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
//					String durationString =durationEdgeIt.next().inVertex().value("NAME");
//					Double vertexDuration = Double.parseDouble(durationString);
//
//					Iterator<Edge> plannedStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_START);
//					String vertexPlannedStart = plannedStartEdgeIt.next().inVertex().value("NAME");
//
//					if (i == 0) {
//						try {
//							ES = (Date) getDateFormat().parse(vertexPlannedStart);
//						} catch (ParseException e) {
//							classLogger.error(Constants.STACKTRACE, e);
//						}
//					} else {
//						ES = EF;
//					}
//
//					EF = addToDate(ES, vertexDuration);
//
//					// update db
//					String updateDBquery = "UPDATE SYSTEMACTIVITY SET " + EARLY_START + " = '" + getDateFormat(ES) + "', " + EARLY_FINISH + " = '" + getDateFormat(EF) + "' WHERE SYSTEMACTIVITY = '" + systemActivity + "'";
//					dmComponent.getEngine().insertData(updateDBquery);
//
//					sum += vertexDuration;
//					i++;
//				}
//			}
//			pathSumList.add(sum);
//		}
//	}
//	
//	/**
//	 * Planned methods will be initially run to determine the earliest date of completion for the project without taking the actual dates into account
//	 * @param vertexPathSet Set of all the path lists
//	 * @return
//	 */
//	private void getPlannedLateDates(Set<List<Vertex>> vertexPathSet, TinkerFrame tFrame) {
//		for (List<Vertex> vertexPathList : vertexPathSet) {
//			Collections.reverse(vertexPathList);
//
//			int i = 0;
//			Date LS = null;
//			Date LF = null;
//
//			Date criticalDate = getLastestEFDate();
//			for (Vertex vert : vertexPathList) {
//				if (!vert.value("NAME").equals("_")) {
//
//					String systemActivty = vert.value("NAME");
//					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
//					String durationString = durationEdgeIt.next().inVertex().value("NAME");
//					Double vertexDuration = Double.parseDouble(durationString);
//					
//					if (i == 0) {
//						LF = criticalDate;
//					} else {
//						LF = LS;
//					}
//
//					LS = subractFromDate(LF, vertexDuration);
//					
//					//add the plannedLF to the tinkerframe 
//					String updateDBquery = "UPDATE SYSTEMACTIVITY SET " + PLANNED_LF + " = '" + getDateFormat(LF) + "' WHERE SYSTEMACTIVITY = '"	+ systemActivty + "'";
//					dmComponent.getEngine().insertData(updateDBquery);
//					i++;
//				}
//			}
//
//		}
//		addPlannedLFToFrame(tFrame);
//	}
//
//	/**
//	 * Method to calculate the early start and end dates for all the nodes in each path, while taking into account the actual dates, and then add it to the DB
//	 * @param vertexPathSet Set of all the path lists
//	 * @param tFrame current tinkerFrame
//	 */
//	private void getActualEarlyDates(Set<List<Vertex>> vertexPathSet, TinkerFrame tFrame) {
//		List<Double> pathSumList = new ArrayList<Double>();
//
//		String status = null;
//		for (List<Vertex> vertexPathList : vertexPathSet) {
//			Collections.reverse(vertexPathList);
//			Double sum = 0.0;
//			int i = 0;
//			Date ES = null;
//			Date EF = null;
//
//			for (Vertex vert : vertexPathList) {
//
//				if (!vert.value("NAME").equals("_")) {
//					String systemActivity = vert.value("NAME");
//					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
//					Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("NAME"));
//
//					Iterator<Edge> plannedStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_START);
//					String vertexPlannedStart = plannedStartEdgeIt.next().inVertex().value("NAME");
//
//					Iterator<Edge> actualStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_START);
//					String vertexActualStart = actualStartEdgeIt.next().inVertex().value("NAME");
//
//					Iterator<Edge> actualEndEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_END);
//					String vertexActualEnd = actualEndEdgeIt.next().inVertex().value("NAME");
//
//					String deviation = "slack";
//					String updateDBquery = null;
//					// if there is a actual start value
//					if (!(vertexActualStart == null) && !(vertexActualStart.equals("_"))) {
//						try {
//							ES = (Date) getDateFormat().parse(vertexActualStart);
//						} catch (ParseException e) {
//							classLogger.error(Constants.STACKTRACE, e);
//						}
//
//						// if activity is completed
//						if (!(vertexActualEnd.equals("_")) && !(vertexActualEnd == null)) {
//
//							Double delay = null;
//							Date plannedCompletedDate =null;
//
//							// get the actual end date
//							try {
//								EF = (Date) getDateFormat().parse(vertexActualEnd);
//							} catch (ParseException e) {
//								classLogger.error(Constants.STACKTRACE, e);
//							}
//							status = "completed";
//							deviation = "delay";
//							
//							//if the deviation is completed then get a delay value else...pass in null;
//							
//							Iterator<Edge> plannedLFEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_LF);
//							String vertexPlannedCompletion = plannedLFEdgeIt.next().inVertex().value("NAME");
//							try {
//								plannedCompletedDate = getDateFormat().parse(vertexPlannedCompletion);
//							} catch (ParseException e) {
//								classLogger.error(Constants.STACKTRACE, e);
//							}
//							delay = (double) ((EF.getTime() - plannedCompletedDate.getTime()) / (24 * 60 * 60 * 1000));
//							updateDBquery = "UPDATE SYSTEMACTIVITY SET " + EARLY_START + " = '" + getDateFormat(ES) + "', " + EARLY_FINISH + " = '" + getDateFormat(EF) + "',  KEYSTATUS = '" + status + "', DEVIATION = '" + deviation + "', " + DEVIATION_START + " = '" + getDateFormat(plannedCompletedDate) +  "', " + DEVIATION_FINISH + " = '" + getDateFormat(EF) +  "', " + DELAY + " = '" + delay + "' WHERE SYSTEMACTIVITY = '" + systemActivity + "'";
//
//						}
//						// if activity is still active
//						else {
//							EF = addToDate(ES, vertexDuration);
//							status = "active";
//						}
//					}
//
//					// if there is not actual start value
//					else {
//						if (i == 0) {
//							try {
//								Date plannedStart = (Date) getDateFormat().parse(vertexPlannedStart);
//								if (plannedStart.before(todaysDate)){
//									ES = todaysDate;
//								} else {
//									ES = plannedStart;
//								}
//							} catch (ParseException e) {
//								classLogger.error(Constants.STACKTRACE, e);
//							}
//						} else {
//							Date earliestES = getExistingDateValue(systemActivity, EARLY_START);
//							
//							if (earliestES == null || earliestES.before(EF)) {
//								ES = EF;
//							} else {
//								ES = earliestES;
//							}
//						}
//
//						EF = addToDate(ES, vertexDuration);
//						status = "projected";
//					}
//
//					if(updateDBquery == null) {
//						updateDBquery = "UPDATE SYSTEMACTIVITY SET " + EARLY_START + " = '" + getDateFormat(ES) + "', " + EARLY_FINISH + " = '" + getDateFormat(EF) + "',  KEYSTATUS = '" + status + "', DEVIATION = '" + deviation + "', " + DEVIATION_START + " = '" + getDateFormat(EF) +  "' WHERE SYSTEMACTIVITY = '" + systemActivity + "'";
//					}
//					
//					dmComponent.getEngine().insertData(updateDBquery);
//					
//					sum += vertexDuration;
//					i++;
//				}
//			}
//			pathSumList.add(sum);
//		}
//		addDevationToFrame(tFrame);
//	}
//
//	/**
//	 * Method to calculate the late start and end dates for all the nodes in each path, while taking into account the actual dates, and then add it to the DB
//	 * @param vertexPathSet Set of all the path lists
//	 */
//	private void getActualLateDates(Set<List<Vertex>> vertexPathSet) {
//		for (List<Vertex> vertexPathList : vertexPathSet) {
//			Collections.reverse(vertexPathList);
//
//			int i = 0;
//			Date LS = null;
//			Date LF = null;
//
//			Date criticalDate = getLastestEFDate();
//			for (Vertex vert : vertexPathList) {
//				if (!vert.value("NAME").equals("_")) {
//
//					String systemActivity = vert.value("NAME");
//					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
//					Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("NAME"));
//					
//					Iterator<Edge> deviationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DEVIATION);
//					String vertexDeviation = deviationEdgeIt.next().inVertex().value("NAME");
//					
//					if (i == 0) {
//						LF = criticalDate;
//					} else {
//						Date earliestLS = getExistingDateValue(systemActivity, LATE_START);
//						if (earliestLS == null || earliestLS.before(LS)) {
//							LF = LS;
//						} else {
//							LF = earliestLS;
//						}
//					}
//
//					LS = subractFromDate(LF, vertexDuration);
//					//	// use delay instead of slack
//					
//										
////					delay = (double) ((LF.getTime() - plannedEF.getTime()) / (24 * 60 * 60 * 1000));
//
//					// update db
//					String systemActivty = vert.value("NAME");
//					String updateDBquery= null;
//					if(vertexDeviation.equals("slack")){
//						updateDBquery = "UPDATE SYSTEMACTIVITY SET " + LATE_START + " = '" + getDateFormat(LS) + "', " + LATE_FINISH + " = '" + getDateFormat(LF) + "', " + DEVIATION_FINISH + " = '" + getDateFormat(LF) + "' WHERE SYSTEMACTIVITY = '"	+ systemActivty + "'";
//					}else{
//						updateDBquery = "UPDATE SYSTEMACTIVITY SET " + LATE_START + " = '" + getDateFormat(LS) + "', " + LATE_FINISH + " = '" + getDateFormat(LF) + "' WHERE SYSTEMACTIVITY = '"	+ systemActivty + "'";
//					}
//					dmComponent.getEngine().insertData(updateDBquery);
//					i++;
//				}
//			}
//
//		}
//	}
//	
//	///////////////////////////////////////////          FORMATING DATES            //////////////////////////////////////////////
//
//	/**
//	 * Method to calculate and add the slack value to the DB
//	 * Slack = LATEFINISH - EARLYFINISH
//	 * @param vertexSet a list of all the vertices in the tinkerframe 
//	 */
//	private void calculateSlack(Set<Vertex> vertexSet) {
//
//		// for every single activity in the current tinkerframe get the LS and ES and then subtract them together to get the Slack value
//
//		for (Vertex vert : vertexSet) {
//			String systemActivty = vert.value("NAME");
//			Iterator<Edge> lateStartIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + LATE_START);
//			String LS = (String) lateStartIterator.next().inVertex().value("NAME");
//			Date LSDate = null;
//			try {
//				LSDate = getDateFormat().parse(LS);
//			} catch (ParseException e1) {
//				e1.printStackTrace();
//			}
//
//			Iterator<Edge> earlyStartIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + EARLY_START);
//			String ES = (String) earlyStartIterator.next().inVertex().value("NAME");
//			Date ESDate = null;
//			try {
//				ESDate = getDateFormat().parse(ES);
//			} catch (ParseException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//
//			Iterator<Edge> earlyFinishIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + EARLY_FINISH);
//			String EF = (String) earlyFinishIterator.next().inVertex().value("NAME");
//			Date EFDate = null;
//			try {
//				EFDate = getDateFormat().parse(EF);
//			} catch (ParseException e1) {
//				e1.printStackTrace();
//			}
//
//			Iterator<Edge> lateFinishIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + LATE_FINISH);
//			String LF = (String) lateFinishIterator.next().inVertex().value("NAME");
//			Date LFDate = null;
//			try {
//				LFDate = getDateFormat().parse(LF);
//			} catch (ParseException e1) {
//				e1.printStackTrace();
//			}
//
////			System.out.println("ES VAL:::" + ESDate);
////			System.out.println("EF VAL:::" + EFDate);
////			System.out.println("LS VAL:::" + LSDate);
////			System.out.println("LF VAL:::" + LFDate);
//
//			Double slack = (double) ((LFDate.getTime() - EFDate.getTime()) / (24 * 60 * 60 * 1000));
//			//SLACK SHOULD NEVER BE A NEGATIVE VALUE
//			if (slack >= 0.0) {
//				boolean isCritical = false;
//				if (slack == 0.0) {
//					isCritical = true;
//				}
//				
//				// update db
//				String updateDBquery = "UPDATE SYSTEMACTIVITY SET " + SLACK + " = '" + slack + "', " + CRITICAL_PATH + " = '" + isCritical + "'  WHERE SYSTEMACTIVITY = '" + systemActivty + "'";
//				dmComponent.getEngine().insertData(updateDBquery);
//			}
//		}
//	}
//
//	/**
//	 * Method to determine if the node has already been passed in another path. If it has then get the specified prop to be used in a check
//	 * @param systemActivity node name
//	 * @param prop Property of the node (mostly EarlyStart/LateStart)
//	 * @return date
//	 */
//	private Date getExistingDateValue(String systemActivity, String prop) {
//		String query = "SELECT DISTINCT " + prop + " from SYSTEMACTIVITY WHERE SYSTEMACTIVITY = '" + systemActivity + "'";
//
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		Date date = null;
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			if (!(nextRow.getRawValues() == null)) {
//				String EFString = (String) nextRow.getRawValues()[0];
//				if (!(EFString == null)) {
//					try {
//						date = getDateFormat().parse(EFString);
//					} catch (ParseException e) {
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//		}
//		return date;
//	}
//
//	/**
//	 * Method to get the largest/latest Early finish date which represents the finishing date of the last node on the critical path
//	 * @return last date
//	 */
//	private Date getLastestEFDate() {
//		String query = "SELECT EARLYFINISH FROM SYSTEMACTIVITY";
//
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//
//		List<Date> dateList = new ArrayList<Date>();
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			if (!(nextRow.getRawValues() == null)) {
//				String EFString = (String) nextRow.getRawValues()[0];
//				if (!(EFString == null)) {
//					try {
//						Date ef = getDateFormat().parse(EFString);
//						dateList.add(ef);
//					} catch (ParseException e) {
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//		}
//		Date maxEF = Collections.max(dateList);
//
//		return maxEF;
//	}
//
//	
//	///////////////////////////////////////////          FORMATING DATES            //////////////////////////////////////////////
//	
//	/**
//	 * Method to convert a string to Date format, if we know the string is a date
//	 * @param stringDate date in a string format
//	 * @return date format 
//	 */
//	public Date convertStringDates(String stringDate) {
//		Date date = null;
//		// format string date to type Date
//		try {
//			if (!stringDate.equals("null") && !stringDate.equals("_")) {
//				date = (Date) getDateFormat().parse(stringDate);
//			}
//		} catch (ParseException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//
//		return date;
//	}
//
//	/**
//	 * Method to format date by month-day-year
//	 * @return
//	 */
//	public static DateFormat getDateFormat() {
//		return new SimpleDateFormat("MM-dd-yyyy");
//	}
//
//	/**
//	 * Method to convert date to formated String
//	 * @param date
//	 * @return
//	 */
//	public String getDateFormat(Date date) {
//		DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
//		String strDate = dateFormat.format(date);
//
//		return strDate;
//	}
//	
//	///////////////////////////////////////////          DATE MANIPULATION            //////////////////////////////////////////////
//	
//	
//	/**
//	 * Method to calculate a future date through the addition method
//	 * @param date initial date
//	 * @param duration number of days to add by
//	 * @return new date value 
//	 */
//	private Date addToDate(Date date, Double duration) {
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(date);
//		cal.add(Calendar.DATE, duration.intValue());
//
//		return cal.getTime();
//	}
//
//	/**
//	 * Method to calculate a past date through the subtraction method
//	 * @param date initial date
//	 * @param duration number of days to subtract by
//	 * @return new date value
//	 */
//	private Date subractFromDate(Date date, Double duration) {
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(date);
//		duration *= -1;
//		cal.add(Calendar.DATE, duration.intValue());
//
//		return cal.getTime();
//	}
//	
//
//	
//	///////////////////////////////////////////          FILTERING            //////////////////////////////////////////////
//	
//	/**
//	 * Method to filter for the tools bar
//	 * @param filterActivityTable - HashTable: KEY should be the column that you want to filter on (ie: System), VALUE should be the list of insight you want to filter
//	 * @return Hashmap of filter values
//	 */
//	public Map filter(Hashtable<String, Object> filterTable) {
//		StringBuilder filterString = new StringBuilder();
//		
//		//append each filtering instance to where clause of the query
//		for (String columnKey : filterTable.keySet()) {
//			List<Object> insightList = (List<Object>) filterTable.get(columnKey);
//			
//			if(!insightList.isEmpty() && insightList !=null){
//
//				int i= 0;
//				for(Object insight: insightList){
//
//					if(insightList.size() > 1){
//						//first
//						if(i==0) {
//							filterString.append("(" + columnKey + "= '" + insight + "' OR ");
//						}
//
//						//last
//						else if (i == insightList.size()-1) {
//							filterString.append(columnKey + "= '" + insight + "')");
//						}
//						//middle
//						else{
//							filterString.append(columnKey + "= '" + insight + "' OR ");
//						}
//					} else{
//						//if there is only one item to filter on
//						filterString.append("(" + columnKey + "= '" + insight + "')");
//					}
//
//					i++;
//				}
//				filterString.append(" AND ");
//			}
//		}
//		
//		//delete the last appended 'and'
//		filterString.delete(filterString.length()-4, filterString.length()-1);
//		
//		String query  = "SELECT SDLCPHASE_FK, ACTIVITYGROUP_FK, DHAGROUP_FK, SDLCPHASE_ACTIVITYGROUP_DHAGROUP, EARLYSTART , LATEFINISH, KEYSTATUS,  HEATVALUE  FROM SDLCPHASE_ACTIVITYGROUP_DHAGROUP "
//		+ "inner join SYSTEMACTIVITY on SDLCPHASE_ACTIVITYGROUP_DHAGROUP.SDLCPHASE_ACTIVITYGROUP_DHAGROUP=SYSTEMACTIVITY.SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK "
//		+ "inner join SYSTEM  on SYSTEMACTIVITY.SYSTEMACTIVITY =SYSTEM.SYSTEMACTIVITY_FK "
//		+ "inner join SYSTEMOWNER on SYSTEM.SYSTEM  = SYSTEMOWNER.SYSTEM_FK "
//		+ "where " + filterString;
//		
//		System.out.println(query);
//		Map<String, Object> queryMap = new HashMap<String, Object>();
//		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
//		while (iterator.hasNext()) {
//			IHeadersDataRow nextRow = iterator.next();
//			Map<String, Object> innerMap = new HashMap<String, Object>();
//
//			if (!(nextRow.getRawValues() == null)) {
//				innerMap.put(STATUS, (String) nextRow.getRawValues()[6]);
//				innerMap.put(SDLC, (String) nextRow.getRawValues()[0]);
//				innerMap.put(ActivityGroup, (String) nextRow.getRawValues()[1]);
//				innerMap.put(DHA, (String) nextRow.getRawValues()[2]);
//				innerMap.put(EARLY_START, (String) nextRow.getRawValues()[4]);
//				innerMap.put(LATE_FINISH, (String) nextRow.getRawValues()[5]);
//				innerMap.put(HEAT_VALUE, (String) nextRow.getRawValues()[7]);
//				queryMap.put((String) nextRow.getRawValues()[3], innerMap);
//			}
//		}
//		
//		//returnMap will contain {STATUS, SDLC, ActivityGroup, DHA, EARLY_START, LATE_FINISH, HEAT_VALUE} + general data
//		Map<String, Object> returnMap = manipulateData(queryMap);
//		returnMap.putAll(getData());
//		return returnMap;
//	}
//
//	/**
//	 * Method to unfilter on the tools bar
//	 * @param unfilterDrillDownTable null
//	 * @return Hashmap of unfiltered values 
//	 */
//	public Map unfilter(Hashtable<String, Object> unfilterDrillDownTable) {
//		String[] selector = new String[] {};
//		return getDataMakerOutput(selector);
//	}
//	
//	
//	
//	///////////////////////////////////////////          MISC            //////////////////////////////////////////////
//	
//	/**
//	 * Method to pass FE values for system which will be used for the filters
//	 * @return system options
//	 */
//	public Map getSystem() {
//		Map<String, Object> returnHash = new HashMap<String, Object>();
//
//		String[] selectors = new String[] { SYSTEM };
//		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
//		Object systems = mainHash.get("data");
//		returnHash.put(SYSTEM, systems);
//		return returnHash;
//	}
//
//	/**
//	 * Method to pass FE values for systemOwner to filter on
//	 * @return systemOwner options
//	 */
//	public Map getSystemOwner() {
//		Map<String, Object> returnHash = new HashMap<String, Object>();
//
//		String[] selectors = new String[] { SYSTEM_OWNER };
//		Map<String, Object> mainHash = super.getDataMakerOutput(selectors);
//		Object systemsOwner = mainHash.get("data");
//		returnHash.put(SYSTEM_OWNER, systemsOwner);
//		return returnHash;
//	}
//
//}