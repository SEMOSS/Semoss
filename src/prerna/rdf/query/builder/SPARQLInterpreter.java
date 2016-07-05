package prerna.rdf.query.builder;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;
import prerna.util.Constants;
import prerna.util.Utility;

public class SPARQLInterpreter implements IQueryInterpreter {
	
	// core class to convert the query struct into a sql query
	QueryStruct qs = null;
	
	SEMOSSQuery semossQuery = new SEMOSSQuery();
	// Need to think about this query as if it is a sql query
	// Each clause within sparql query is thought of as a table within sql
	// Join types are set by making various clauses optional or not optional
	
	Boolean addedJoins = false;
	IEngine engine = null;

	public SPARQLInterpreter(IEngine engine)
	{
		this.engine = engine;
	}

	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}
	
	public String composeQuery()
	{
		semossQuery.setQueryType(SPARQLConstants.SELECT);
		semossQuery.setDisctinct(true);
		
		addSelectors();
		addFilters();
		addJoins();
		
		String query = null;
		semossQuery.createQuery();
		query = semossQuery.getQuery();
		System.out.println("QUERY....  " + query);
		return query;
	}

	private String addFrom(String table, String colName)
	{
		if(table != null && colName != null && !colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)) // this is a derived data
		{
			addNodeProperty(table, colName);
			return colName;
		}
		else if(colName == null || colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER))
		{
			addNode(table);
			return table;
		}
		return " invalid addFrom call ";
	}
	
	private String getVarName(String physName, boolean property){
		String logName = "";
		physName = Utility.getInstanceName(engine.getTransformedNodeName("http://semoss.org/ontologies/DisplayName/" + physName, false));
		if(!property){
			logName = engine.getTransformedNodeName("http://semoss.org/ontologies/Concept/" + physName, true);
		}
		else {
			logName = engine.getTransformedNodeName("http://semoss.org/ontologies/Relation/Contains/" + physName, true);
		}
		return Utility.getInstanceName(logName);
	}
	
	public void addSelector(String table, String colName)
	{
		System.out.println("adding selector " + table + "    +   " + colName);
		// the table can be null

		if(table != null && colName != null && !colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)) // this is a derived data
		{
			SEMOSSQueryHelper.addSingleReturnVarToQuery(getVarName(colName, true), semossQuery);
		}
		else if(colName == null || colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER))
		{
			SEMOSSQueryHelper.addSingleReturnVarToQuery(getVarName(table, false), semossQuery);
		}
	}
	
//	private String addNode(String table){
//		// get the node uri from the owl (how....?)
//		table = Utility.getInstanceName(engine.getTransformedNodeName(Constants.DISPLAY_URI+table, false));
//		String nodeURI = engine.getConceptUri4PhysicalName(table);
//		
//		SEMOSSQueryHelper.addConceptTypeTripleToQuery(getVarName(table, false), nodeURI, false, semossQuery, table);
//		return nodeURI;
//	}
	
	private String addNode(String table){
		// get the correct nodeURI from the owl
		String nodeURI = engine.getPhysicalUriFromConceptualUri("http://semoss.org/ontologies/Concept/"+table);
		// add the node to the query
		SEMOSSQueryHelper.addConceptTypeTripleToQuery(getVarName(table, false), nodeURI, false, semossQuery, table);
		return nodeURI;
	}
	
//	private String addNodeProperty(String table, String col){
//		String nodeURI = addNode(table);
//		col = Utility.getInstanceName(engine.getTransformedNodeName(Constants.DISPLAY_URI+col, false));
//		// get the prop uri from the owl (how....?)
//		String propURI = "Unable to get prop uri";
//		List<String> props = this.engine.getProperties4Concept(nodeURI, false);
//		for(String prop : props){
//			if(Utility.getInstanceName(prop).equals(col)){
//				propURI = prop;
//				break;
//			}
//		}
//		
//		SEMOSSQueryHelper.addGenericTriple(getVarName(table, false), TriplePart.VARIABLE, propURI, TriplePart.URI, getVarName(col, true), TriplePart.VARIABLE, false, semossQuery, table);
//		
//		return propURI;
//	}

	private String addNodeProperty(String table, String col){
		addNode(table);
		String propURI = engine.getPhysicalUriFromConceptualUri("http://semoss.org/ontologies/Relation/Contains/"+col);
//		col = Utility.getInstanceName(engine.getTransformedNodeName(Constants.DISPLAY_URI+col, false));
//		// get the prop uri from the owl (how....?)
//		String propURI = "Unable to get prop uri";
//		List<String> props = this.engine.getProperties4Concept(nodeURI, false);
//		for(String prop : props){
//			if(Utility.getInstanceName(prop).equals(col)){
//				propURI = prop;
//				break;
//			}
//		}
		
		SEMOSSQueryHelper.addGenericTriple(getVarName(table, false), TriplePart.VARIABLE, propURI, TriplePart.URI, getVarName(col, true), TriplePart.VARIABLE, false, semossQuery, table);
		
		return propURI;
	}
	
	public void addSelectors()
	{
		Enumeration <String> selections = qs.selectors.keys();
		while(selections.hasMoreElements())
		{
			String tableName = selections.nextElement();
			Vector <String> columns = qs.selectors.get(tableName);

			for(int colIndex = 0;colIndex < columns.size(); colIndex++){
				addSelector(tableName, columns.get(colIndex));
				addFrom(tableName, columns.get(colIndex));
			}
		}
	}
	
	public void addFilters()
	{
		if(!addedJoins){
			addJoins();
		}
		Enumeration <String> concepts = qs.andfilters.keys();
		
		while(concepts.hasMoreElements())
		{
			String concept_property = concepts.nextElement();
			// inside this is a hashtable of all the comparators
			Hashtable <String, Vector> compHash = qs.andfilters.get(concept_property);
			Enumeration <String> comps = compHash.keys();
			String concept = concept_property;
			String property = null;
			if(concept_property.contains("__")) {
				concept = concept_property.substring(0, concept_property.indexOf("__"));
				property = concept_property.substring(concept_property.indexOf("__")+2);
			}
			
			// the comparator between the concept is an and so block it that way
			// I need to specify to it that I am doing something new here
			// ok.. what I mean is this
			// say I have > 50
			// and then  < 80
			// I need someway to tell the adder that this is an end 
			while(comps.hasMoreElements())
			{
				String thisComparator = comps.nextElement();
				
				Vector options = compHash.get(thisComparator);
				
				// and the final one goes here					
				
				// now I get all of them and I start adding them
				// usually these are or ?
				// so I am saying if something is

				addFilter(concept, property, thisComparator, options);
			}
		}
	}
	
	
	private void addFilter(String concept, String property, String thisComparator, Vector objects) {
		// Here are the rules for adding a filter to a sparql query
		// 1. We want to use bind and bindings rather than filter whenever possible as it speeds up processing
		// 2. Using bindings at the end of the query is the same as putting filter within the clause for that concept
		// 3. If the concept we are filtering on is optional, the filter must be outside of its clause (aka can't use bindings)
		
		//TODO:
		//TODO:
		// need to tell the FE to not pass this... this currently occurs when you hit on the 
		// concept when you try to traverse in graph
		if(objects == null || objects.size() == 0) {
			return;
		}

		// should expose this on the engien itself
		boolean isProp = false;
		if(property != null || engine.getParentOfProperty(concept) != null)
			isProp = true;
		
		concept = Utility.getInstanceName(engine.getTransformedNodeName(Constants.DISPLAY_URI+concept, false));
		if(objects.get(0) instanceof String) // ok this is a string ------ must be " = " comparator ... currently not handling regex
		{
			List<Object> cleanedObjects = new Vector<Object>();
			if(objects.get(0).toString().indexOf(engine.getNodeBaseUri()) >= 0 ) // then they are uris and don't need to be cleaned
			{
				cleanedObjects = objects;
			}
			else
			{
				if(!isProp) {
					// then these are all uris that need to be cleaned
					for(Object object : objects){
						String myobject = (""+object).replace("\"", ""); 
						// get rid of the space
						// myobject = myobject.replaceAll("\\s+","_");
						myobject = Utility.cleanString(myobject, true, true, false);
						myobject = myobject.trim();
						myobject = engine.getNodeBaseUri() + concept+"/"+ myobject;
						cleanedObjects.add(myobject);
					}
				} else {
					//TODO: this is null when it is a filter defined from the data and is stored as a concept
					//TODO: need the add implicit filters based on parent/concept
					if(property == null) {
						property = concept;
					}
					// need to cast objects appropriately
					for(Object object : objects) {
						Object myobject = object.toString().replace("\"", "");
						String type = Utility.findTypes(myobject + "")[0] + "";
						if(type.equalsIgnoreCase("Date")) {
							myobject = Utility.getDate(myobject + "");
						} else if(type.equalsIgnoreCase("Double")) {
		    				myobject = Utility.getDouble(myobject + "");
						}
						cleanedObjects.add(myobject);
					}
				}
			}
			if(isProp) {
				// literals cannot use bind or bindings because we need to use the comparator
				SEMOSSQueryHelper.addURIFilterPhrase(getVarName(property, true), TriplePart.VARIABLE, cleanedObjects, TriplePart.LITERAL, " = ", true, semossQuery);
			} else if(cleanedObjects.size()==1){ // we can always just add a bind to the main clause... never want this to be optional
				SEMOSSQueryHelper.addBindPhrase(cleanedObjects.get(0)+"", TriplePart.URI, concept, semossQuery);
			}
			else if (!semossQuery.hasBindings() && !semossQuery.clauseIsOptional(concept)){ // bindings is only valid if the clause isn't optional and bindings hasn't already been used
				SEMOSSQueryHelper.addBindingsToQuery(cleanedObjects, TriplePart.URI, concept, semossQuery);
			}
			else { // filter can always be added the main clause... never want this to be optional
				SEMOSSQueryHelper.addURIFilterPhrase(concept, TriplePart.VARIABLE, cleanedObjects, TriplePart.URI, " = ", true, semossQuery);
			}
		}
		else { // literals cannot use bind or bindings because we need to use the comparator
			if(isProp) {
				SEMOSSQueryHelper.addURIFilterPhrase(getVarName(property, true), TriplePart.VARIABLE, objects, TriplePart.LITERAL, " = ", true, semossQuery);
			} else {
				SEMOSSQueryHelper.addURIFilterPhrase(getVarName(concept, true), TriplePart.VARIABLE, objects, TriplePart.LITERAL, " = ", true, semossQuery);
			}
//			SEMOSSQueryHelper.addRegexFilterPhrase(getVarName(property, true), TriplePart.VARIABLE, objects, objType, false, true, semossQuery, true);
		}
	}
	
	public void addJoins()
	{
		if(addedJoins){
			return;
		}
		addedJoins = true;
		// full and final and we are here
		Enumeration <String> concepts = qs.relations.keys();
		
		while(concepts.hasMoreElements())
		{
			String concept_property = concepts.nextElement();
			// inside this is a hashtable of all the comparators
			Hashtable <String, Vector> compHash = qs.relations.get(concept_property);
			Enumeration <String> comps = compHash.keys();
			
			// the comparator between the concept is an and so block it that way
			// I need to specify to it that I am doing something new here
			// ok.. what I mean is this
			// say I have > 50
			// and then  < 80
			// I need someway to tell the adder that this is an end 
			while(comps.hasMoreElements())
			{
				String thisComparator = comps.nextElement();
				
				Vector <String> options = compHash.get(thisComparator);
				
				// and the final one goes here					
				
				// now I get all of them and I start adding them
				// usually these are or ?
				// so I am saying if something is

				for(int optIndex = 0;optIndex < options.size(); optIndex++) {
					addJoin(concept_property, thisComparator, options.get(optIndex));
				}
			}
		}		
	}
	
	private void addJoin(String fromString, String thisComparator, String toString) {
		// this needs to be revamped pretty extensively
		// I need to add this back to the from because I might not be projecting everything
		if(!fromString.contains("__") && !toString.contains("__")){
			String fromURI = addNode(fromString);
			String toURI = addNode(toString);
			
			// need to figure out what the predicate is from the owl
			// also need to determine the direction of the relationship -- if it is forward or backward
			String query = "SELECT ?relationship WHERE {<" + fromURI + "> ?relationship <" + toURI + "> } ORDER BY DESC(?relationship)";
			TupleQueryResult res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
			String predURI = " unable to get pred from owl for " + fromURI + " and " + toURI;
			try {
				if(res.hasNext()){
					predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
				}
				else {
					query = "SELECT ?relationship WHERE {<" + toURI + "> ?relationship <" + fromURI + "> } ORDER BY DESC(?relationship)";
					res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
					if(res.hasNext()){
						predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
					}
				}
			} catch (QueryEvaluationException e) {
				System.out.println(predURI);
			}
			if(predURI == null){
				System.err.println("Unable to add join because we are unable to find the predicate on the owl");
				return;
			}
			
			TriplePart conPart = new TriplePart(getVarName(fromString, false), TriplePart.VARIABLE);
			TriplePart predPart = new TriplePart(predURI, TriplePart.URI);
			TriplePart toConPart = new TriplePart(getVarName(toString, false), TriplePart.VARIABLE);
			
			if(thisComparator.contains("inner.join")) { // if it is inner it doesn't matter which clause i add it to
				semossQuery.addTriple(conPart, predPart, toConPart, false);
			}
			else if(thisComparator.contains("right.outer")){ // if right outer, I want everything in the right table, so left is optional
				semossQuery.addTriple(conPart, predPart, toConPart, false, fromString);
				semossQuery.setClauseOptional(fromString, true);
			}
			else if(thisComparator.contains("left.outer")){
				semossQuery.addTriple(conPart, predPart, toConPart, false, toString);
				semossQuery.setClauseOptional(toString, true);
			}
			else if(thisComparator.contains("outer")){
				semossQuery.addTriple(conPart, predPart, toConPart, false);
				semossQuery.setClauseOptional(toString, true);
				semossQuery.setClauseOptional(fromString, true);
			}
		}
		else if(fromString.contains("__") && !toString.contains("__")){
			// joining on a concept that is actually a property
			String[] nodePropSplit = fromString.split("__");
			addNodeProperty(nodePropSplit[0], nodePropSplit[1]);
		}
		else if(!fromString.contains("__") && toString.contains("__")){
			// joining on a concept that is actually a property
			String[] nodePropSplit = toString.split("__");
			addNodeProperty(nodePropSplit[0], nodePropSplit[1]);
		}
		else { // both have properties defined... no idea how to link this... filter match?
			
		}
	}

}
