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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import edu.stanford.nlp.util.ArraySet;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
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
	private final String ACTUAL_START = "ActualStart";
	private final String ACTUAL_END = "ActualEnd";
	private final String UPLOAD_DATE = "UploadDate";
	private final String IS_ACTIVE = "IsActive";
	private final String ACTIVITY_NUM = "ACTIVITY_NUM";
	private final String STATUS = "STATUS";
	private final String DEPENDENCY = "Dependency";
	private final String DURATION = "DURATION";
	private final String EARLY_START = "EarlyStart";
	private final String EARLY_FINISH = "EarlyFinish";
	private final String LATE_START = "LateStart";
	private final String LATE_FINISH = "LateFinish";
	private final String SLACK = "Slack";
	private final String CRITICALVAL = "criticalVal";
	
	private final Date todaysDate = Calendar.getInstance().getTime();
	static String engineName= "TAP_Readiness_Database_V1";
//	static String engineName = "Dummy_1";
	static String masterPKQL = "data.import(api:"+ engineName +".query([c:SDLCPhase,c:SDLCPhase_ActivityGroup_DHAGroup,c:ActivityGroup,c:DHAGroup,c:SystemActivity,c:SystemActivity__ActualEnd,c:SystemActivity__Duration,c:SystemActivity__ProjectedEnd,c:SystemActivity__LateFinish,c:SystemActivity__Delay,c:SystemActivity__CriticalPath,c:SystemActivity__LateStart,c:SystemActivity__EarlyFinish,c:SystemActivity__EarlyStart,c:SystemActivity__ActualStart,c:SystemActivity__Slack,c:SystemActivity__ProjectedStart,c:SystemActivity__KeyStatus,c:System,c:System__PlannedStart,c:SystemOwner, c:Activity,c:DependencySystemActivity], ([c:SDLCPhase,left.outer.join,c:SDLCPhase_ActivityGroup_DHAGroup],[c:ActivityGroup,left.outer.join,c:SDLCPhase_ActivityGroup_DHAGroup],[c:DHAGroup,left.outer.join,c:SDLCPhase_ActivityGroup_DHAGroup],[c:SDLCPhase_ActivityGroup_DHAGroup,left.outer.join,c:SystemActivity],[c:SystemActivity,left.outer.join,c:System],[c:System,left.outer.join,c:SystemOwner], [c:SystemActivity,left.outer.join,c:Activity], [c:SystemActivity,left.outer.join,c:DependencySystemActivity])));";
	
	static String instanceOfPlaysheet = "prerna.ui.components.specific.tap.MHSDashboardDrillPlaysheet";
	
//	/**
//	 * Method to create a datamaker
//	 */
//	@Override
	public void createData() {
//		if (this.dmComponent == null) {
//			//this.query will get the query added to the specific insight
//			this.dmComponent = new DataMakerComponent(this.engine, masterQuery);
//		}
//
//		if(this.dataFrame == null) {
//			this.dataFrame = new H2Frame();
//		}
//		this.dataFrame.processDataMakerComponent(this.dmComponent);
	}

	
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
		return returnHashMap;
	}
	
	public Map getDataTableAlign (){
		Map<String, String> dataTableAlign = new HashMap <String, String> ();
		dataTableAlign.put("levelOne", SDLC);
		dataTableAlign.put("levelTwo", ActivityGroup);
		dataTableAlign.put("levelThree", DHA);
		dataTableAlign.put(STATUS, STATUS);
		dataTableAlign.put("heatValue", HEAT_VALUE);
		dataTableAlign.put("earlyStart", EARLY_START);
		dataTableAlign.put("lateFinish", LATE_FINISH);
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
		
		//delete duplicates
		iteratorMap.put(TinkerFrame.DE_DUP, true);
		iteratorMap.put(TinkerFrame.SELECTORS, selectorList);

		boolean getRawData = false;
		Iterator<Object[]> iterator = dataFrame.iterator(getRawData, iteratorMap);

		createNewTinkerFrame();
		
		//iterate to get data
		while(iterator.hasNext()){
			Object[] iteratirArr = iterator.next();
			String sdlc = (String) iteratirArr[0];
			String group = (String) iteratirArr[1];
			String dha = (String) iteratirArr[2];
			String key = (String) iteratirArr[3];
			
			Map<String, Object> innerMap = new HashMap <String, Object> ();
			
			if (!returnMap.containsKey(key)) {
				innerMap.put (ACTIVITY_NUM, 1);
				innerMap.put(SDLC, sdlc);
				innerMap.put(ActivityGroup, group);
				innerMap.put(DHA, dha);
				innerMap.putAll(getTileInfo(key));
				innerMap.putAll(getStatus(key));
				returnMap.put(key, innerMap);
			}
		}
		return manipulateData(returnMap);
	}
	
	private Map<String, Object> getTileInfo (String primKey) {
		String query = "SELECT MIN(SLACK) as SLACK, MIN(EARLYSTART) AS EARLYSTART, MAX(LATEFINISH) AS LATEFINISH FROM SYSTEMACTIVITY where SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK ='" + primKey + "'";
		//MIN(NULLIF(SLACK, 0.0))
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
		
		Object slack = null;
		Date earlyStart = null;
		Date lateFinish = null;
		while(iterator.hasNext()) {
			IHeadersDataRow nextRow = iterator.next();
			if(!(nextRow.getRawValues() == null)){		
				slack = nextRow.getRawValues()[0];
				try {
					earlyStart = getDateFormat().parse((String) nextRow.getRawValues()[1]);
					lateFinish =  getDateFormat().parse((String) nextRow.getRawValues()[2]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}

		Map<String, Object> returnMap = new HashMap<String, Object> ();
		returnMap.put(HEAT_VALUE, slack);
		returnMap.put(EARLY_START, earlyStart);
		returnMap.put(LATE_FINISH, lateFinish);
		
		return returnMap;
	}
	
	private Map<String, Object> getStatus (String primKey) {
		String query = "SELECT KEYSTATUS FROM SYSTEMACTIVITY where SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK ='" + primKey + "'";
		
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
		
		String status = null;
		List<String> statusList = new ArrayList<String>();
		while(iterator.hasNext()) {
			IHeadersDataRow nextRow = iterator.next();
			if(!(nextRow.getRawValues() == null)){		
				status = (String)nextRow.getRawValues()[0];
				statusList.add(status);
			}
		}
		String groupStatus = null;
		if(statusList.contains("active")) {
			groupStatus = "active";
		} else if (statusList.contains("projected")) {
			groupStatus = "projected";
		} else {
			groupStatus = "completed";
		}
		
		Map<String, Object> returnMap = new HashMap<String, Object> ();
		returnMap.put(STATUS, groupStatus);
		return returnMap;
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
		returnMap.put("headers", Arrays.asList(STATUS, SDLC, ActivityGroup, DHA, EARLY_START, LATE_FINISH, HEAT_VALUE));

		return returnMap;
	}
	
	public Map<String, Object> createNewTinkerFrame() {

		// create frame
		TinkerFrame tFrame = new TinkerFrame();

		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		Set<String> relationNode = new ArraySet<String> ();
		relationNode.add(SYSTEM_ACTIVITY);
		relationNode.add(DURATION);
		relationNode.add(PLANNED_START);
		relationNode.add(ACTUAL_START);
		relationNode.add(ACTUAL_END);
		edgeHash.put(SYSTEM_ACTIVITY, relationNode);
		

		Map<String, String> dataTypeMap = new HashMap<String, String>();
		dataTypeMap.put(SYSTEM_ACTIVITY, "STRING");
		dataTypeMap.put(DURATION, "NUMBER");
		dataTypeMap.put(ACTUAL_START, "STRING");
		dataTypeMap.put(ACTUAL_END, "STRING");
		dataTypeMap.put(PLANNED_START, "STRING");
		
		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
		logicalToTypeMap.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
		logicalToTypeMap.put(DURATION, DURATION);
		logicalToTypeMap.put(ACTUAL_START, ACTUAL_START);
		logicalToTypeMap.put(ACTUAL_END, ACTUAL_END);
		logicalToTypeMap.put(PLANNED_START, PLANNED_START);
		
		// merge edge hash and data types
		tFrame.mergeEdgeHash(edgeHash, dataTypeMap);

		//this query will get all the distinct activities that belong to the specified primKey (SDLCPhase_ActivityGroup_DHAGroup)
		String query1 = "SELECT DISTINCT SYSTEMACTIVITY.SYSTEMACTIVITY as SystemActivity, SystemActivity.DURATION, DEPENDENCYSYSTEMACTIVITY.DEPENDENCYSYSTEMACTIVITY as SystemActivity, SystemActivity.ACTUALSTART as ActualStart, SystemActivity.ACTUALEND as ActualEnd, SYSTEM.PLANNEDSTART "
				+ "FROM SYSTEMACTIVITY "
				+ "Left Join DEPENDENCYSYSTEMACTIVITY ON SYSTEMACTIVITY.SYSTEMACTIVITY = DEPENDENCYSYSTEMACTIVITY.SYSTEMACTIVITY_FK "
				+ "Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM.SYSTEMACTIVITY_FK ";
		
		IRawSelectWrapper iterator1 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query1);
		
		Map<Integer, Set<Integer>> cardinality1 = new HashMap<Integer, Set<Integer>>();
		Set<Integer> cardSet1 = new HashSet<Integer>();
		cardSet1.add(1);
		cardSet1.add(2);
		cardSet1.add(3);
		cardSet1.add(4);
		cardSet1.add(5);
		cardinality1.put(0, cardSet1);

		String[] headers1 = new String[]{SYSTEM_ACTIVITY, DURATION, SYSTEM_ACTIVITY, ACTUAL_START, ACTUAL_END, PLANNED_START};

		List<String> dependencyList = new ArrayList<String> ();
		// iterate through and add to the tinker frame
		while(iterator1.hasNext()) {
			IHeadersDataRow nextRow = iterator1.next();
			tFrame.addRelationship(headers1, nextRow.getValues(), nextRow.getRawValues(), cardinality1, logicalToTypeMap);
			
			//get a dependency list of all the non-null values for the second query 
			if(!(nextRow.getValues()[2] == null)){		
				String dependency = (String) nextRow.getValues()[2];
				dependencyList.add(dependency);
			}
		}
		 
		//////////// SECOND QUERY /////////////////////
		if(!dependencyList.isEmpty()){
			String dependencyConcat = String.join("', '", dependencyList);
			
			//this query will get all the properties of the dependency activities
			
			String query2 = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITY.DURATION, SYSTEMACTIVITY.ACTUALSTART as ActualStart, SYSTEMACTIVITY.ACTUALEND as ActualEnd, SYSTEM.PLANNEDSTART "
					+ "FROM SYSTEMACTIVITY Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM .SYSTEMACTIVITY_FK WHERE SYSTEMACTIVITY IN ('"+ dependencyConcat + "')";
			
			IRawSelectWrapper iterator2 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query2);
			
			String[] headers2 = new String[]{SYSTEM_ACTIVITY, DURATION, ACTUAL_START, ACTUAL_END, PLANNED_START};
			
			Map<Integer, Set<Integer>> cardinality2 = new HashMap<Integer, Set<Integer>>();
			Set<Integer> cardSet2 = new HashSet<Integer>();
			cardSet2.add(1);
			cardSet2.add(2);
			cardSet2.add(3);
			cardSet2.add(4);
			cardinality2.put(0, cardSet2);
			
			while(iterator2.hasNext()) {
				IHeadersDataRow nextRow = iterator2.next();
				tFrame.addRelationship(headers2, nextRow.getValues(), nextRow.getRawValues(), cardinality2, logicalToTypeMap);
			}
		}

		return calculateHeatValue(tFrame);
	}	
	
	private void addToFrame (TinkerFrame tFrame) {
		
		// add to frame
		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		Set<String> relationNode = new ArraySet<String> ();
		relationNode.add(SYSTEM_ACTIVITY);
		relationNode.add(EARLY_START);
		relationNode.add(EARLY_FINISH);
		relationNode.add(LATE_START);
		relationNode.add(LATE_FINISH);
		edgeHash.put(SYSTEM_ACTIVITY, relationNode);
		

		Map<String, String> dataTypeMap = new HashMap<String, String>();
		dataTypeMap.put(SYSTEM_ACTIVITY, "STRING");
		dataTypeMap.put(EARLY_START, "NUMBER");
		dataTypeMap.put(EARLY_FINISH, "STRING");
		dataTypeMap.put(LATE_START, "NUMBER");
		dataTypeMap.put(LATE_FINISH, "STRING");
		
		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
		logicalToTypeMap.put(SYSTEM_ACTIVITY, SYSTEM_ACTIVITY);
		logicalToTypeMap.put(EARLY_START, EARLY_START);
		logicalToTypeMap.put(EARLY_FINISH, EARLY_FINISH);
		logicalToTypeMap.put(LATE_START, LATE_START);
		logicalToTypeMap.put(LATE_FINISH, LATE_FINISH);
		
		String query = "SELECT SYSTEMACTIVITY, SYSTEMACTIVITY.EARLYSTART, SYSTEMACTIVITY.EARLYFINISH, SYSTEMACTIVITY.LATESTART, SYSTEMACTIVITY.LATEFINISH "
				+ "FROM SYSTEMACTIVITY Left Join SYSTEM ON SYSTEMACTIVITY.SYSTEMACTIVITY = SYSTEM .SYSTEMACTIVITY_FK ";
//				+ "WHERE SYSTEMACTIVITY.SDLCPHASE_ACTIVITYGROUP_DHAGROUP_FK ='"+ primKey +"'";
		
		IRawSelectWrapper iterator2 = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
		
		String[] headers2 = new String[]{SYSTEM_ACTIVITY, EARLY_START, EARLY_FINISH, LATE_START, LATE_FINISH};
		
		Map<Integer, Set<Integer>> cardinality2 = new HashMap<Integer, Set<Integer>>();
		Set<Integer> cardSet2 = new HashSet<Integer>();
		cardSet2.add(1);
		cardSet2.add(2);
		cardSet2.add(3);
		cardSet2.add(4);
		cardinality2.put(0, cardSet2);
		
		while(iterator2.hasNext()) {
			IHeadersDataRow nextRow = iterator2.next();
			tFrame.addRelationship(headers2, nextRow.getValues(), nextRow.getRawValues(), cardinality2, logicalToTypeMap);
		}
	}
	
	private List<String> getFirstNodeList () {
		String query = "SELECT DISTINCT SYSTEMACTIVITY from SYSTEMACTIVITY WHERE SYSTEMACTIVITY.SYSTEMACTIVITY NOT IN (SELECT DEPENDENCYSYSTEMACTIVITY FROM DEPENDENCYSYSTEMACTIVITY)";
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
		
		List<String> initialNodeList = new ArrayList<String> ();
		while(iterator.hasNext()) {
			IHeadersDataRow nextRow = iterator.next();
			if(!(nextRow.getRawValues() == null)){		
				initialNodeList.add((String) nextRow.getRawValues()[0]);
			}
		}
		return initialNodeList;
	}
	
	private Map<String, Object> calculateHeatValue (TinkerFrame tFrame){
		
		tFrame.openBackDoor();
		
		GraphTraversal ret = tFrame.runGremlin("g.traversal().V().has('TYPE','" + SYSTEM_ACTIVITY + "').outE().inV().has('TYPE','" + SYSTEM_ACTIVITY + "').path()");
		ArraySet<List<Vertex>> pathSet = new ArraySet<List<Vertex>>();
		Set<Vertex> vertexSet = new ArraySet <Vertex> ();
		List<String> initialNodeList = getFirstNodeList();
		
		while(ret.hasNext()) {
			org.apache.tinkerpop.gremlin.process.traversal.Path p = (Path) ret.next();
			for(int i = 0 ; i < p.size(); i++) {
				Object a = p.get(i);
				if(a instanceof Vertex) {
					String vertName = ((Vertex) a).value("NAME");
					if(!(vertName.equals("_"))){ 
						vertexSet.add((Vertex)a);
						if(initialNodeList.contains(vertName)) {
							pathSet.addAll(getPathsRecursion2((Vertex)a, new ArrayList<Vertex>()));
						}
					}
				}
			}
		} 
		//HERE
		for(Vertex parent : vertexSet)
		{
			Iterator<Edge> edgeiterator  = parent.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + SYSTEM_ACTIVITY);
			while (edgeiterator.hasNext()){
				String parentstring = ((Vertex) parent).value("NAME");
				String children = edgeiterator.next().inVertex().value("NAME");
				System.out.println(parentstring + " : " + children);
			}
		}
		
		
		Double criticalVal = calculateCriticalValue(pathSet, tFrame);
		calculateLateDates(pathSet, criticalVal, tFrame); 
		
		addToFrame(tFrame);
		Double minSlack = calculateSlack(vertexSet);
		
		Map<String, Object> returnHash = new HashMap<String, Object>();
		returnHash.put(HEAT_VALUE, minSlack);
		
		return returnHash;
	}
	
	private ArraySet<List<Vertex>> getPathsRecursion2 (Vertex currentVertex, ArrayList<Vertex> path) {
		path.add(currentVertex);
		Iterator<Edge> edgeiterator  = currentVertex.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + SYSTEM_ACTIVITY);
		ArraySet<List<Vertex>> paths = new ArraySet<List<Vertex>>();
		while( edgeiterator.hasNext() )
		{
			
			Vertex childNode = edgeiterator.next().inVertex();
		
			if(childNode.value("NAME").equals("_"))
			{
				paths.add(path);
			}
			else
			{
				ArrayList<Vertex> newPath = new ArrayList<Vertex>();
				newPath.addAll(path);
				paths.addAll(getPathsRecursion2(childNode,newPath));
			}
		}
		
		return(paths);
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
	
	private Double calculateCriticalValue (Set<List<Vertex>> vertexPathSet, TinkerFrame tFrame){	
		System.out.println("PATHS:::::::");
		
		List<Double> pathSumList = new ArrayList<Double> ();
		
		String status = null;
		for(List<Vertex> vertexPathList : vertexPathSet){
			
			Double sum = 0.0;
			int i = 0;
			Date ES = null;
			Date EF = null; 
			
			for(Vertex vert: vertexPathList){
				
				if(!vert.value("NAME").equals("_")){
					String systemActivity = vert.value("NAME");
					System.out.print(systemActivity);
					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
					Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("VALUE"));
					
					Iterator<Edge> plannedStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_START);
					String vertexPlannedStart = plannedStartEdgeIt.next().inVertex().value("NAME");
					
					Iterator<Edge> actualStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_START);
					String vertexActualStart = actualStartEdgeIt.next().inVertex().value("NAME");
					
					Iterator<Edge> actualEndEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_END);
					String vertexActualEnd = actualEndEdgeIt.next().inVertex().value("NAME");

					//if there is a actual start value
					if(!(vertexActualStart == null ) && !(vertexActualStart.equals("_"))) {
						try {
							ES = (Date) getDateFormat().parse(vertexActualStart);
							System.out.println("ACTUAL EARLY START:::::: " + ES);
						} catch (ParseException e) {
							e.printStackTrace();
						}

						//if activity is completed
						if (!(vertexActualEnd.equals("_") && !(vertexActualEnd == null ))){
//							Date plannedEF = null;
//							if(i == 0) {
//								try {
//									plannedEF = addToDate((Date) getDateFormat().parse(vertexPlannedStart), vertexDuration);
//								} catch (ParseException e) {
//									e.printStackTrace();
//								}
//							} else {
//								Date previousES = getDBFDateValue(systemActivity, EARLY_START);
//								if(previousES == null || previousES.before(EF)){
//									plannedEF = EF;
//								}else{
//									plannedEF = previousES;
//								}
//							}
							
							
							try {
								EF = (Date) getDateFormat().parse(vertexActualEnd);
							} catch (ParseException e) {
								e.printStackTrace();
							} 
							status = "completed";	
							
							//use delay instead of slack
							
						}
						//if activity is still active
						else {
							EF = addToDate(ES, vertexDuration);
							status = "active";
						}
					}
					
					//if there is not actual start value
					else {
						if(i == 0) {
							try {
								ES = (Date) getDateFormat().parse(vertexPlannedStart);
							} catch (ParseException e) {
								e.printStackTrace();
							}
						} else {
							Date previousES = getDBFDateValue(systemActivity, EARLY_START);
							if(previousES == null || previousES.before(EF)){
								ES = EF;
							}else{
								ES = previousES;
							}
						}
						
						EF = addToDate(ES, vertexDuration);
						status = "projected";
					}
					
					//update db
					String updateDBquery = "UPDATE SYSTEMACTIVITY SET " + EARLY_START + " = '"+ getDateFormat(ES)  + "', " + EARLY_FINISH + " = '"+ getDateFormat(EF) + "',  KEYSTATUS = '"+ status + "' WHERE SYSTEMACTIVITY = '" + systemActivity + "'";
					dmComponent.getEngine().insertData(updateDBquery);
					
					sum += vertexDuration;
					i++;	
				} 
			}
			pathSumList.add(sum);
		}
		return Collections.max(pathSumList);
	}
	
	private Date getDBFDateValue(String systemActivity, String prop){
		String query = "SELECT DISTINCT "+ prop +" from SYSTEMACTIVITY WHERE SYSTEMACTIVITY = '" + systemActivity + "'";
		
		
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
		
		Date date = null;
		while(iterator.hasNext()) {
			IHeadersDataRow nextRow = iterator.next();
			if(!(nextRow.getRawValues() == null)){		
				String EFString = (String) nextRow.getRawValues()[0];
				if(!(EFString == null)){
					try {
						date = getDateFormat().parse(EFString);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return date;
	}
	
	
	private Date getLastNodeEFDate() {
		String query = "SELECT max(EARLYFINISH) FROM SYSTEMACTIVITY ";
		
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), query);
		
		Date maxEF = null;
		while(iterator.hasNext()) {
			IHeadersDataRow nextRow = iterator.next();
			if(!(nextRow.getRawValues() == null)){		
				String EFString = (String) nextRow.getRawValues()[0];
				if(!(EFString == null)){
					try {
						maxEF = getDateFormat().parse(EFString);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return maxEF;
	}
	
	
	private void calculateLateDates (Set< List<Vertex>> vertexPathSet, Double criticalVal, TinkerFrame tFrame) {
		for(List<Vertex> vertexPathList : vertexPathSet){
			Collections.reverse(vertexPathList);
			
			int i = 0;
			Date LS = null;
			Date LF = null; 
			
			for(Vertex vert: vertexPathList){
				if(!vert.value("NAME").equals("_")){
					
					String systemActivity = vert.value("NAME");
					Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
					Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("VALUE"));
					
//					Iterator<Edge> earlyFinishEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + EARLY_FINISH);
//					String vertexEarlyFinish = (String) earlyFinishEdgeIt.next().inVertex().value("NAME");
					
//					Iterator<Edge> plannedStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + PLANNED_START);
//					String vertexPlannedStart = plannedStartEdgeIt.next().inVertex().value("NAME");
//					Date plannedStart = null;
//					try {
//						plannedStart = getDateFormat().parse(vertexPlannedStart);
//					} catch (ParseException e) {
//						e.printStackTrace();
//					}

					if(i == 0) {
						//TODO IF PLANNEDSTART IS AFTER CURRENT DATE THEN USE CURRENT DATE
						
						Vertex lastNodeInPath = vertexPathList.get(0);
						System.out.println("last node in path " +lastNodeInPath.values("NAME"));
						LF = getLastNodeEFDate();
					} else {
						Date previousLF = getDBFDateValue(systemActivity, LATE_FINISH);
						if(previousLF == null || previousLF.after(LS)){
							LF = LS;
						}else{
							LF = previousLF;
						}
					}

					LS = subractFromDate(LF, vertexDuration);
					
					//update db
					String systemActivty = vert.value("NAME");
					String updateDBquery = "UPDATE SYSTEMACTIVITY SET " + LATE_START + " = '"+ getDateFormat(LS) + "', " + LATE_FINISH + " = '"+ getDateFormat(LF) + "' WHERE SYSTEMACTIVITY = '" + systemActivty + "'";
					dmComponent.getEngine().insertData(updateDBquery);
					i++;
				}
			}
			
		}
	} 
	
	
	//calculate and add the slack value to the excel
	private Double calculateSlack (Set<Vertex> vertexSet) {
		
		//for every single activity in the current tinkerframe get the LS and ES and then subtract them together to get the Slack value
		
		List<Double> slackList = new ArrayList<Double> ();
		for(Vertex vert : vertexSet){
			String systemActivty = vert.value("NAME");
			System.out.println("SYSTEM NAME::::::" + systemActivty);
			Iterator<Edge> lateStartIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + LATE_START);
			String LS = (String) lateStartIterator.next().inVertex().value("VALUE");
			Date LSDate = null;
			try {
				LSDate = getDateFormat().parse(LS);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
			
			Iterator<Edge> earlyStartIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + EARLY_START);
			String ES = (String) earlyStartIterator.next().inVertex().value("VALUE");
			Date ESDate = null;
			try {
				ESDate = getDateFormat().parse(ES);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			
			/////
			//TODO DELETE!
			
			Iterator<Edge> earlyFinishIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + EARLY_FINISH);
			String EF = (String) earlyFinishIterator.next().inVertex().value("VALUE");
			Date EFDate = null;
			try {
				EFDate = getDateFormat().parse(EF);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
			
			Iterator<Edge> lateFinishIterator = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + LATE_FINISH);
			String LF = (String) lateFinishIterator.next().inVertex().value("VALUE");
			Date LFDate = null;
			try {
				LFDate = getDateFormat().parse(LF);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
			
			System.out.println("ES VAL:::" + ESDate);
			System.out.println("EF VAL:::" + EFDate);
			System.out.println("LS VAL:::" + LSDate);
			System.out.println("LF VAL:::" + LFDate);
			
			///////////////////////////////
			
			Double slack = (double) ((LSDate.getTime() - ESDate.getTime())/(24 * 60 * 60 * 1000));
			
			System.out.println("SLACK VAL:::" + slack);
			
			if(slack != 0) {
				slackList.add(slack);
			}
			
			//update db
			String updateDBquery = "UPDATE SYSTEMACTIVITY SET " + SLACK + " = '"+ slack + "' WHERE SYSTEMACTIVITY = '" + systemActivty + "'";
			dmComponent.getEngine().insertData(updateDBquery);
		}
		
		Double minSlack = 0.0;
		if(!slackList.isEmpty()){
			minSlack = Collections.min(slackList);
		} 
		
		return minSlack;
		
	}
	
	
	private void calculateEarlyDates (Set<List<Vertex>> vertexPathSet){	
//		Map<String, Object> returnMap = new HashMap<String, Object> ();
//		List<Double> pathSumList = new ArrayList<Double> ();
//		Boolean isActive = false;
//		if(!(vertexPathSet.isEmpty())){
//
//			for(List<Vertex> vertexPathList : vertexPathSet){
//
//				Date todaysDate = Calendar.getInstance().getTime();
//
//				Double sum = 0.0;
//				
//				int i = 0;
//				Double ES = 0.0;
//				Double EF = 0.0;
//				
//				for(Vertex vert: vertexPathList){
//					if(!vert.value("NAME").equals("_")){
//						Iterator<Edge> durationEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + DURATION);
//						Double vertexDuration = Double.parseDouble(durationEdgeIt.next().inVertex().value("VALUE"));
//	
//						Iterator<Edge> actualStartEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_START);
//						String vertexActualStart = actualStartEdgeIt.next().inVertex().value("NAME");
//	
//						Iterator<Edge> actualEndEdgeIt = vert.edges(Direction.OUT, "TYPE", SYSTEM_ACTIVITY + "+++" + ACTUAL_END);
//						String vertexActualEnd = actualEndEdgeIt.next().inVertex().value("NAME");
//						
//						
//						if(i == 0) {
//							ES = 0.0;
//						} else {
//							ES = EF;
//						}
//	
//						EF = ES + vertexDuration;
//						
//						//TODO store this data on the excel
//						
//						
//						Map<String, Date> dateHash = convertStringDates(vertexActualStart, vertexActualEnd);
////						Date plannedStartDate = dateHash.get(PLANNED_START);
////						Date plannedEndDate = dateHash.get(PLANNED_END);
//						Date actualStartDate = dateHash.get(ACTUAL_START);
//						Date actualEndDate = dateHash.get(ACTUAL_END);
//	
//						Date projectedStart = null;
//	
//						//ALGORITHM
////						if(actualStartDate == null) {
////							if(plannedStartDate == null || plannedStartDate.before(todaysDate)){
////								projectedStart = todaysDate;
////							}else {
////								projectedStart = plannedStartDate;
////							}
////						}else 
//						if(!(actualStartDate == null)){
//							projectedStart = actualStartDate;
//						} else{
//							projectedStart = todaysDate;
//						}
//	
//						String systemActivty = vert.value("NAME"); 
//						
////						//if there is a user inputted value already in the db for column projectedStart, then compare it to calculated projectedStart date						
////						String projectedStartQuery = "SELECT PROJECTEDSTART from SYSTEMACTIVITYUPLOAD WHERE SYSTEMACTIVITY_FK = '" + systemActivty + "'";
////						IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(dmComponent.getEngine(), projectedStartQuery);
////						while(iterator.hasNext()) {
////							IHeadersDataRow nextRow = iterator.next();
////							if(!(nextRow.getValues()[0] == null)){
////								Date containedProjectedStart;
////								try {
////									containedProjectedStart = (Date) getDateFormat().parse((String)nextRow.getValues()[0]);
////									if(containedProjectedStart.after(projectedStart)){
////										projectedStart = containedProjectedStart;
////									}
////								} catch (ParseException e) {
////									continue;
////								}
////							}
////						}
//						
//						
//						Date projectedEnd = addToDate(projectedStart, vertexDuration);
//						
//						//calculate total amount of days this activity will be delayed
//						double delay= 0.0;
////						if(!(plannedEndDate == null)){
////							delay = (double) ((projectedEnd.getTime() - plannedEndDate.getTime())/(24 * 60 * 60 * 1000)); 
////						} else {
////							delay = (double) ((projectedEnd.getTime() - projectedStart.getTime())/(24 * 60 * 60 * 1000));
////						}
//						
//						String dateType = null;
//						//Completed
//						if(! (actualEndDate == null)) { 
//							if(actualEndDate.before(todaysDate)){
//								dateType = "Completed";
//							}
//							if(! (actualStartDate == null)) {
//								//Active
//								if(actualStartDate.before(todaysDate) && actualEndDate.after(todaysDate)) {
//									dateType = "Active";
//									isActive = true;
//								}
//							}
//						}
//						//Projected
//						else {
//							dateType = "Projected";
//						}
//						
//						String updateDBquery = "UPDATE SYSTEMACTIVITYUPLOAD SET PROJECTEDSTART = '"+ getDateFormat(projectedStart) + "', DELAY = '"+ delay + "', PROJECTEDEND = '"+ getDateFormat(projectedEnd) + "', DATETYPE = '"+ dateType + "' WHERE SYSTEMACTIVITY_FK = '" + systemActivty + "'";
//						dmComponent.getEngine().insertData(updateDBquery);
//						sum += delay;
//					}
//				pathSumList.add(-sum);
//				}
//			}
//			
//			Double criticalVal = Collections.max(pathSumList);
//			double slack = 0.0;
//			List<Double> slackList = new ArrayList<Double> ();
//			for (Double pathSum : pathSumList) {
//				double currentSlack = criticalVal - pathSum;
//				if(currentSlack != 0.0){
//					slackList.add(currentSlack);
//				}
//			}
//			
//			if(!slackList.isEmpty()){
//				slack = Collections.min(slackList);
//			}
//			
//			returnMap.put(HEAT_VALUE, slack);
//			returnMap.put(IS_ACTIVE, isActive);
//			
//			return returnMap;
//		} else {
//			returnMap.put(HEAT_VALUE, 0.0);
//			returnMap.put(IS_ACTIVE, isActive);
//			return returnMap;
//		}
	}
	
	
	private Date addToDate (Date date, Double duration){
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, duration.intValue());
		
		return cal.getTime();
	}
	
	private Date subractFromDate (Date date, Double duration){
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		duration *= -1;
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
				sum += vertexDuration;
			}
			pathSumList.add(sum);
		}
		return Collections.max(pathSumList);
	}
	
	
	public Date convertStringDates (String stringDate) {
		Date date = null;
		Map<String, Date> dateHash = new HashMap<String, Date>();
		//format string date to type Date
		try {
			if(!stringDate.equals("null") && !stringDate.equals("_")){
				date = (Date) getDateFormat().parse(stringDate);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return date; 
	}

	/**
	 * Method to format date by month-day-year
	 * @return
	 */
	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("MM-dd-yyyy");
	}

	/**
	 * Method to convert date to formated String
	 * @param date
	 * @return
	 */
	public String getDateFormat(Date date) {
		DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
		String strDate = dateFormat.format(date);
		
		return strDate;
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


}