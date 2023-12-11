package prerna.ds;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.util.DIHelper;
import prerna.util.Utility;

@Deprecated
public class QueryStruct {

	// what is in a query
	// 1. selectors - what is it that we want to give back
	// 2. filters - what are the things that we want to filter ooo.. profound
	// Title = ["Ants story", "etc"] etc..
	// Studio = ["WB", "Fox"]
	// 3. How you want to join
	// Title.Title Inner_Join Studio.Title_Fk
	public Map <String, List<String>> selectors = new Hashtable<String, List<String>>();
	
	// there could be multiple comparators for the same thing
	// for instance I could say
	// moviebudget > 200 and < 300
	// so it would go as
	// | Movie Budget |  > | Vector(200) | // or this could be a whole object / vector
	//				  |  < | Vector(300) |
	public Map <String, Map<String, List>> andfilters = new Hashtable<String, Map<String, List>>();

	//Hashtable <String, Hashtable<String, Vector>> orfilters = new Hashtable<String, Hashtable<String, Vector>>();
	// relations are of the form
	// item = <relation vector>
	// concept = type of join toCol
	// Movie	 InnerJoin Studio, Genre
	//			 OuterJoin Nominated
	public Map <String, Map<String, List>> relations = new Hashtable<String, Map<String, List>>();
	
	//holds the selector we want to order by
	//tableName -> ColName
	private Map <String, String> orderBy = new Hashtable<>();
	
	//holds the selector(s) we want to group by
	//tableName -> ColName
	private Map <String, Set<String>> groupBy = new Hashtable<>();
	
	private long limit = -1;
	private long offset = -1;
	
	public final static int NO_COUNT = 0; //use this when we don't want to do a count
	public final static int COUNT_CELLS = 1; //use this when we want to count the number of total rows*selectors in the query
	public final static int COUNT_DISTINCT_SELECTORS = 2; //use this when we want to count the number of distinct values for a selector in a query
	
	private int performCount = NO_COUNT;
	
	private String engineName;
	
	public static String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";
		
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}
	
	public String getEngineName() {
		return this.engineName;
	}

	public void addSelector(String concept, String property)
	{
		if(property == null)
			property = PRIM_KEY_PLACEHOLDER;
		
		addToHash(concept, property, selectors);
		
	}
	
	
	public void addFilter(String fromCol, String comparator, List filterData)
	{
		// the filter data is typically of the format
		// there could be more than one comparator
		
		// find if this property is there
		// ok if the logical name stops being unique this will have some weird results
		Map <String, List> compHash = new Hashtable<String, List>();
		if(andfilters.containsKey(fromCol))
			compHash = andfilters.get(fromCol);
		
		List curData = new Vector();
		// next piece is to see if we have the comparator
		if(compHash.containsKey(comparator))
			curData = compHash.get(comparator);
		
		curData.addAll(filterData);
		
		// put it back
		compHash.put(comparator, curData);	
		
		// put it back
		andfilters.put(fromCol, compHash);
	}
	
	public void addRelation(String fromConcept, String toConcept, String comparator)
	{
		// I need pick the keys from the table based on relationship and then add that to the relation
		// need to figure out type of 
		// find if this property is there
		// ok if the logical name stops being unique this will have some weird results
		
		
		Map <String, List> compHash = new Hashtable<String, List>();
		if(relations.containsKey(fromConcept))
			compHash = relations.get(fromConcept);
		
		List curData = new Vector();
		// next piece is to see if we have the comparator
		if(compHash.containsKey(comparator))
			curData = compHash.get(comparator);
		
		curData.add(toConcept);
		
		// put it back
		compHash.put(comparator, curData);	
		
		// put it back
		relations.put(fromConcept, compHash);
	}
	
	public void setLimit(long limit) {
		this.limit = limit;
	}
	
	public long getLimit() {
		return this.limit;
	}
	
	public void setOffSet(long offset) {
		this.offset = offset;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public void setOrderBy(String concept, String property) {
		if(property == null) {
			property = PRIM_KEY_PLACEHOLDER;
		}
		
		this.orderBy.put(concept, property);
	}
	
	public Map<String, String> getOrderBy() {
		return this.orderBy;
	}
	
	public void addGroupBy(String concept, String property) {
		if(property == null) {
			property = PRIM_KEY_PLACEHOLDER;
		}
		
		if(groupBy.containsKey(concept)) {
			this.groupBy.get(concept).add(property);
		} else {
			Set<String> propertyList = new HashSet<>(3);
			propertyList.add(property);
			this.groupBy.put(concept, propertyList);
		}
		
	}
	
	public Map<String, Set<String>> getGroupBy() {
		return this.groupBy;
	}
	
	public int getPerformCount() {
		return this.performCount;
	}
	
	public void setPerformCount(int performCount) {
		this.performCount = performCount;
	}
	
	private void addToHash(String concept, String property, Map <String, List<String>> hash)
	{
		// group it by table and you are done
		List <String> propList = new Vector<String>();
		
		if(hash.containsKey(concept))
			propList = hash.get(concept);
		
		propList.add(property);
		
		hash.put(concept, propList);
	}

	public void print() {
		// TODO Auto-generated method stub
		System.out.println("SELECTORS " + selectors);
		System.out.println("FILTERS.. " + andfilters);
		System.out.println("RELATIONS.. " + relations);
	}
	
	public Map<String, Map<String, List>> getRelations(){
		return this.relations;
	}
	
	public Map<String, List<String>> getSelectors(){
		return this.selectors;
	}

	/**
	 * This uses the selector list and relations lists to determine how everything is connected
	 *
	 * Will return like this:
	 * Title --> [Title__Budget, Studio]
	 * Studio --> [StudioOwner]
	 * etc.
	 * 
	 * @return
	 */
	public Map<String, Set<String>> getReturnConnectionsHash() {
		// create the return edgeHash map
		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		
		/*
		 * 1) iterate through and add concepts and properties
		 * This step is very simple and doesn't require any special logic
		 * Just need to consider the case when PRIM_KEY_PLACEHOLDER is not present which means
		 * That the query return only returns the property and not the main concept
		 * 
		 * 2) iterate through and add the relationships
		 * This needs to take into consideration intermediary nodes
		 * e.g. i have concepts a -> b -> c -> d but I only want to return a-> d
		 * thus, the edge hash should only contain a -> d
		 */
		
		// 1) iterate through all the selectors
		for(String selectorKey: this.selectors.keySet()) {
			List<String> props = this.selectors.get(selectorKey);
			Set<String> downNodeTypes = edgeHash.get(selectorKey);
			// if the props doesn't contain a prim_key_placeholder... then it is actually just a property and not a concept
			if(!props.contains(PRIM_KEY_PLACEHOLDER)) {
				// just loop through and add all the properties by themselves
				for(String prop : props){
					edgeHash.put(selectorKey + "__" + prop, new HashSet<String>());
				}
			} else {
				// a prim_key_placeholder was found
				// thus, we need to add the concept to all of its properties
				if(downNodeTypes == null){
					downNodeTypes = new HashSet<String>();
				}
				edgeHash.put(selectorKey, downNodeTypes);
				for(String prop : props){
					// make sure we don't add a node to itself (e.g. Title__Title)
					if(prop.equals(PRIM_KEY_PLACEHOLDER)) {
						continue;
					}
					// mergeQSEdgeHash needs this to be the concept__property... plus need to keep it consistent with relations
					downNodeTypes.add(selectorKey + "__" + prop); 
				}
			}
		}

		// 2) need to determine and connect the appropriate connections based on the 
		if(this.relations != null) {
			// get the starting concept
			for(String startNode : this.relations.keySet()) {
				// the relMap contains the joinType pointing to a list of columns to be joined to
				Map<String, List> relMap = this.relations.get(startNode);
				// else, just doing a normal join
				// if the edge hash has the start node as a selector
				// then we need to see if we should connect it
				// otherwise, check if it is a relationship based on a property
				// if that also fails, do nothing
				// this is because the logic for returning a -> d can be done when checking
				// the endNode of the relationship
				if(edgeHash.containsKey(startNode)) {
					processRelationship(startNode, relMap, edgeHash);
				} else {
					if(startNode.contains("__")) {
						String concept = startNode.substring(0, startNode.indexOf("__"));
						if(edgeHash.containsKey(concept)) {
							processRelationship(concept, relMap, edgeHash);
						}
					}
				}
			}
		}

		return edgeHash;
	}
	
	/**
	 * Logic to process the relationship
	 * This takes into consideration intermediary nodes that should not be added to the return hash
	 * e.g. i have concepts a -> b -> c -> d but I only want to return a-> d
	 * @param startNode				The startNode of the relationship
	 * @param relMap				The relationships being observed for the startNode
	 * @param edgeHash				The existing edge hash to determine what the current selectors are
	 */
	private void processRelationship(String startNode, Map<String, List> relMap, Map<String, Set<String>> edgeHash) {
		// grab all the end nodes
		// the edge hash doesn't care about what kind of join it is
		Collection<List> endNodeValues = relMap.values();
		for(List<String> endNodeList : endNodeValues) {
			// iterate through all the end nodes
			for(String endNode : endNodeList) {
				// need to ignore the prim_key_value...
				if(startNode.equals(endNode)) {
					continue;
				}
				
				// if the endNode already exists as a key in the edgeHash,
				// then just connect it and we are done
				if(edgeHash.containsKey(endNode)) {
					edgeHash.get(startNode).add(endNode);
				} else {
					// maybe we are joining on a prop
					// lets first test this out
					if(endNode.contains("__")) {
						String concept = endNode.substring(0, endNode.indexOf("__"));
						if(edgeHash.containsKey(concept)) {
							// we found the parent.. therefore we add it 
							// just add parent to the startNode
							edgeHash.get(startNode).add(concept);
						} else {
							// here we need to loop through and find the shortest path
							// starting from this specific endNode to an endNode which is 
							// a selector to be returned
							// we use a recursive method determineShortestEndNodePath to fill in 
							// the list newEndNodeList and then we add that to the edgeHash
							List<String> newEndNodeList = new Vector<String>();
							determineShortestEndNodePath(endNode, edgeHash, newEndNodeList);
							for(String newEndNode : newEndNodeList) {
								edgeHash.get(startNode).add(newEndNode);
							}
						}
					} else {
						// here we need to loop through and find the shortest path
						// starting from this specific endNode to an endNode which is 
						// a selector to be returned
						// we use a recursive method determineShortestEndNodePath to fill in 
						// the list newEndNodeList and then we add that to the edgeHash
						List<String> newEndNodeList = new Vector<String>();
						determineShortestEndNodePath(endNode, edgeHash, newEndNodeList);
						for(String newEndNode : newEndNodeList) {
							edgeHash.get(startNode).add(newEndNode);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Recursive method to find the shortest path to all the nearest concepts that are being returned as selectors
	 * @param endNode					The endNode that is node a selector which we are trying to find the shortest path to 
	 * @param edgeHash					The edgeHash to find the current selectors
	 * @param newEndNodeList			The list of endNodes that have been found using the logic to find the shortest 
	 * 									path for connected nodes
	 */
	private void determineShortestEndNodePath(String endNode, Map<String, Set<String>> edgeHash, List<String> newEndNodeList) {
		// this endNode is a node which is not a selector
		// need to find the shortest path to nodes which this endNode is connected to which is also a selector

		// first see if there is a connection for the endNode to traverse to
		if(this.relations.containsKey(endNode)) {
			// grab the join map
			Map<String, List> joinMap = this.relations.get(endNode);
			// we do not care at all about the type of join
			// just go through and get the list of nodes which we care about
			Collection<List> connections = joinMap.values();
			for(List<String> endNodeList :  connections) {
				for(String possibleNewEndNode : endNodeList) {
					// if this connection is a selector (i.e. key in the edgeHash), then we need to add it to the newEndNodeList
					if(edgeHash.containsKey(possibleNewEndNode)) {
						newEndNodeList.add(possibleNewEndNode);
					} else {
						// maybe we are joining on a prop
						// lets first test this out
						if(possibleNewEndNode.contains("__")) {
							String concept = possibleNewEndNode.substring(0, possibleNewEndNode.indexOf("__"));
							if(edgeHash.containsKey(concept)) {
								// we found the parent.. therefore we add it 
								// append it to the list
								newEndNodeList.add(concept);
							} else {
								// if possibleNewEndNode is in fact not a end node
								// then we need to recursively go down the path and see if it has a possibleNewEndNode
								determineShortestEndNodePath(possibleNewEndNode, edgeHash, newEndNodeList);
							}
						} else {
							// if possibleNewEndNode is in fact not a end node
							// then we need to recursively go down the path and see if it has a possibleNewEndNode
							determineShortestEndNodePath(possibleNewEndNode, edgeHash, newEndNodeList);
						}
					}
				}
			}
		}
	}

	/* 
	 * Returns whether or not a filter already exists for this column
	 */
	public boolean hasFiltered(String column) {
		if(this.andfilters.containsKey(column)){
			return true;
		}
		else {
			return false;
		}
	}
	
	/**
	 * Returns if no information has been set into the query struct
	 * @return
	 */
	public boolean isEmpty() {
		// if any of the main 3 objects within the QS have info, return false
		// even in the case that selectors are empty, if other info is set, the QS will still 
		// return false for this method
		if(!this.selectors.isEmpty() || !this.relations.isEmpty() || !this.andfilters.isEmpty()) {
			return false;
		}
		
		return true;
	}
	
	public void merge(QueryStruct incomingQS) {
		mergeSelectors(incomingQS.selectors);
		mergeFilters(incomingQS.andfilters);
		mergeRelations(incomingQS.relations);
		if(incomingQS.limit > -1) {
			setLimit(incomingQS.limit);
		}
		
		if(incomingQS.offset > -1) {
			setOffSet(incomingQS.offset);
		}
		
		if(incomingQS.getEngineName() != null) {
			setEngineName(incomingQS.getEngineName());
		}
		
	}
	
	public void mergeSelectors(Map<String, List<String>> incomingSelectors) {
		for(String key : incomingSelectors.keySet()) {
			
			Set<String> selectorSet = new HashSet<>();
			if(this.selectors.containsKey(key)) {
				selectorSet.addAll(this.selectors.get(key));
				selectorSet.addAll(incomingSelectors.get(key));
				
				Vector<String> allSelectors = new Vector<>();
				allSelectors.addAll(selectorSet);
				
				this.selectors.put(key, allSelectors);
//				this.selectors.get(key).addAll(incomingSelectors.get(key));
			} else {
				selectorSet.addAll(incomingSelectors.get(key));
				Vector<String> newVector = new Vector<>();
				newVector.addAll(selectorSet);
				this.selectors.put(key, newVector);
			}
		}
	}
	
	public void mergeFilters(Map<String, Map<String, List>> incomingFilters) {
		for(String key : incomingFilters.keySet()) {
			Map<String, List> incomingHash = incomingFilters.get(key);
			if(this.andfilters.containsKey(key)) {
				Map<String, List> thisHash = this.andfilters.get(key);
				for(String relationKey : incomingHash.keySet()) {
					List v;
					if(thisHash.containsKey(relationKey)) {
						v = thisHash.get(relationKey);
					} else {
						v = new Vector();
					}
					v.addAll(incomingHash.get(relationKey));
					thisHash.put(relationKey, v);
				}
			} else {
				Map<String, List> newHash = new Hashtable<>();
				for(String relationKey : incomingHash.keySet()) {
					Vector v = new Vector();
					v.addAll(incomingHash.get(relationKey));
					newHash.put(relationKey, v);
				}
				this.andfilters.put(key, newHash);
			}
		}
	}
	
	public void mergeRelations(Map<String, Map<String, List>> incomingRelations) {
		for(String key : incomingRelations.keySet()) {
			Map<String, List> incomingHash = incomingRelations.get(key);
			if(this.relations.containsKey(key)) {
				Map<String, List> thisHash = this.relations.get(key);
				for(String relationKey : incomingHash.keySet()) {
					List v;
					if(thisHash.containsKey(relationKey)) {
						v = thisHash.get(relationKey);
					} else {
						v = new Vector();
					}
					v.addAll(incomingHash.get(relationKey));
					thisHash.put(relationKey, v);
				}
			} else {
				Map<String, List> newHash = new Hashtable<>();
				for(String relationKey : incomingHash.keySet()) {
					Vector v = new Vector();
					v.addAll(incomingHash.get(relationKey));
					newHash.put(relationKey, v);
				}
				this.relations.put(key, newHash);
			}
		}
	}
	
	public void mergeGroupBy(Hashtable<String, List<String>> groupBys) {
		
	}
	
	public QueryStruct deepCopy() {
		QueryStruct copy = new QueryStruct();
		Gson gson = new Gson();
		
		String stringified_selectors = gson.toJson(this.selectors);
		copy.selectors = gson.fromJson(stringified_selectors, new TypeToken<Hashtable<String, Vector<String>>>() {}.getType());
		
		String stringified_relations = gson.toJson(this.relations);
		copy.relations = gson.fromJson(stringified_relations, new TypeToken<Hashtable <String, Hashtable<String, Vector>>>() {}.getType());
		
		String stringified_filters = gson.toJson(this.andfilters);
		copy.andfilters = gson.fromJson(stringified_filters, new TypeToken<Hashtable <String, Hashtable<String, Vector>>>() {}.getType());
		
		return copy;
	}
	
//	public static void main(String [] args) throws Exception
//	{
//		// test code for getting proper edge hash when there are intermediary nodes that
//		// i.e. the query requires a specific node that you do not want in your selectors
//		// e.g. i have concepts a -> b -> c -> d but I only want to return a-> d
//		// thus, the edge hash should only contain a -> d 
//		
//		QueryStruct qs = new QueryStruct();
//		qs.addSelector("a", "x");
//		qs.addSelector("b", null);
//		qs.addSelector("b", "y");
//		qs.addSelector("d", null);
//
//		qs.addRelation("a__x", "b__y", "inner.join");
//		qs.addRelation("b__y", "c", "inner.join");
//		qs.addRelation("c", "d", "inner.join");
//
//		System.out.println(qs.getReturnConnectionsHash());
//		
//		// previous test code .. based on path assuming it is done by b.s.s
////		
////		QueryStruct qs = new QueryStruct();
////		qs.addSelector("Title", "Title");
////		qs.addFilter("Title__Title", "=", Arrays.asList(new String[]{"WB", "ABC"}));
////		qs.addRelation("Title__Title", "Actor__Title_FK", "inner.join");
////		
////		Gson gson = new Gson();
////		System.out.println(gson.toJson(qs));
////		
////		loadEngine4Test();
////		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp("Movie_DB"); 
////		SPARQLInterpreter in = new SPARQLInterpreter(engine);
////		
////		in.setQueryStruct(qs);
////		String query = in.composeQuery();
////		System.out.println(query);
//	}

//	private static void loadEngine4Test(){
//		DIHelper.getInstance().loadCoreProp("C:\\Users\\bisutton\\workspace\\SEMOSSDev\\RDF_Map.prop");
//		FileInputStream fileIn = null;
//		try{
//			Properties prop = new Properties();
//			String fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\UpdatedRDBMSMovies.smss";
//			fileIn = new FileInputStream(fileName);
//			prop.load(fileIn);
//			System.err.println("Loading DB " + fileName);
//			Utility.loadEngine(fileName, prop);
//			fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\Movie_DB.smss";
//			fileIn = new FileInputStream(fileName);
//			prop.load(fileIn);
//			System.err.println("Loading DB " + fileName);
//			Utility.loadEngine(fileName, prop);
//		}catch(IOException e){
//			e.printStackTrace();
//		}finally{
//			try{
//				if(fileIn!=null)
//					fileIn.close();
//			}catch(IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}
}
