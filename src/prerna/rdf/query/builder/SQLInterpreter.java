package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class SQLInterpreter implements IQueryInterpreter{
	
	// core class to convert the query struct into a sql query
	QueryStruct qs = null;
	private static Hashtable <String,String> aliases = new Hashtable<String,String>();
	Hashtable <String, String> tableProcessed = new Hashtable<String, String>();
	IEngine engine; // engine can be null
	
	// where the wheres are all kept
	// key is always a combination of concept and comparator
	// and the values are values
	Hashtable <String, String> whereHash = new Hashtable<String, String>();

	Hashtable <String, String> relationHash = new Hashtable<String, String>();

	List<String> joinsArr = new ArrayList<String>();
	List<String> leftJoinsArr = new ArrayList<String>();
	List<String> rightJoinsArr = new ArrayList<String>();

	private SQLQueryUtil queryUtil;
	private transient Map<String, String> primaryKeyCache = new HashMap<String, String>();
	private transient Map<String, String[]> relationshipConceptPropertiesMap = new HashMap<String, String[]>();
	
	String selectors = "";
	String froms = "";
	String curWhere = "";
	String allWhere = "";
	
	public SQLInterpreter(IEngine engine)
	{
		this.engine = engine;
	}

	public SQLInterpreter(){
		
	}
	
	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}
	
	public String composeQuery()
	{
		String query = null;
		addSelectors();
		addJoins();
		addFilters();
		
		//System.out.println("Select ..  " + selectors);
		//System.out.println("From ..  " + froms);
		//System.out.println("Where ..  " + whereHash);
		//System.out.println("With Join ..  " + relationHash);
		
		// the final step where the equation is balanced and the anamoly revealed
		query = "SELECT  DISTINCT " + selectors + "  FROM " + froms;
		boolean firstTime = true;

		Enumeration joins = relationHash.keys();
		while(joins.hasMoreElements())
		{
			String value = relationHash.get(joins.nextElement());

			if(firstTime)
			{
				query = query + " " + value;
				firstTime = false;
			}
			else
				query = query + " " + value;
		}
		
		// filters
//		Enumeration wheres = whereHash.keys();
//		firstTime = true;
//		while(wheres.hasMoreElements())
//		{
//			String value = whereHash.get(wheres.nextElement());
////			if(value.contains(" OR "))
//				value = " ( " + value + " ) ";
//			
//			if(firstTime)
//			{
//				query = query + " WHERE " + value;
//				firstTime = false;
//			}
//			else
//				query = query + " AND " + value;
//		}
		
		firstTime = true;
		for (String key : whereHash.keySet())
		{
			String value = whereHash.get(key);
			
			String[] conceptKey = key.split(":::");
			String concept = conceptKey[0];
			String property = conceptKey[1];
			String comparator = conceptKey[2];
			
			String conceptString = "";
			if(comparator.trim().equals("=")) {
				conceptString = getAlias(concept) + "." + property +" IN ";
			}
			
			if(comparator.trim().equals("=") || value.contains(" OR ")) {
				value = " ( " + value + " ) ";
			}
			
			if(firstTime)
			{
				query = query + " WHERE " + conceptString + value;
				firstTime = false;
			}
			else
				query = query + " AND " + conceptString + value;
		}

		System.out.println("QUERY....  " + query);
		return query;
	}

//	private void addFrom(String tableName)
//	{
//		String alias = getAlias(tableName);
//		if(!tableProcessed.containsKey(tableName))
//		{
//			tableProcessed.put(tableName, "true");
//			String fromText =  tableName + "  " + alias;
//			if(froms.length() > 0){
//				froms = froms + " , " + fromText;
//				//rightJoinsArr.add(queryUtil.getDialectOuterJoinRight(fromText));
//				//leftJoinsArr.add(queryUtil.getDialectOuterJoinLeft(fromText));
//			} else {
//				froms = fromText;
//				//rightJoinsArr.add(fromText);
//				//leftJoinsArr.add(fromText);
//			}
//		}
//	}
	
	/**
	 * Adds the form statement for each table
	 * @param tableName				The name of the table
	 */
	private void addFrom(String tableName)
	{
		/*
		 * First implementation of this
		 * Assumption is that the table name being passed in is the conceptual name
		 */
		
		String alias = getAlias(tableName);
		// we dont want to add the from table multiple times as this is invalid in sql
		if(!tableProcessed.containsKey(tableName))
		{
			tableProcessed.put(tableName, "true");
			if(engine != null) {
				// we will get the physical tableName from the engine
				String conceptualURI = "http://semoss.org/ontologies/Concept/" + tableName;
				tableName = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);
				// table name is the instance name of the given uri
					tableName = Utility.getInstanceName(tableName);
			}
			
			String fromText =  tableName + "  " + alias;
			if(froms.length() > 0){
				froms = froms + " , " + fromText;
			} else {
				froms = fromText;
			}
		}
	}

	
	// add from
//	public void addSelector(String table, String colName)
//	{
//		// the table can be null
//		String colName2Use = colName;
//		if(table != null) // this is a derived data
//		{
//			String tableAlias = getAlias(table);
//			String logicalName = table;
//			if(engine != null) { // Can only get logical names and primary keys if engine is defined (requires owl)
//				if(colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)){
//					colName2Use = this.getPrimKey4Table(table);
//				}
//				// get the logical name for the column
//				String physUri = "http://semoss.org/ontologies/Concept/" + colName2Use + "/" + table;
//				logicalName = engine.getTransformedNodeName(physUri, true);
//				if(physUri.equals(logicalName)){ // this means it didn't find the logical name. this means that its a property rather than a node
//					logicalName = engine.getTransformedNodeName("http://semoss.org/ontologies/Relation/Contains/" + colName2Use, true);
//				}
//			}
//			
//			colName2Use = tableAlias + "." + colName2Use + " AS " + Utility.getInstanceName(logicalName);
//		}
//		if(selectors.length() == 0)
//			selectors = colName2Use;
//		else
//			selectors = selectors + " , " + colName2Use;
//	}
	
	/**
	 * Adds the selector required for a table and column name
	 * @param table				The name of the table
	 * @param colName			The column in the table
	 */
	public void addSelector(String table, String colName)
	{
		/*
		 * First implementation of this
		 * We will start by assuming the information being passed is the conceptual name
		 * We will also assume the conceptual name is the logical name to return...
		 * 
		 * TODO: The conceptual name being the returned logical name will need to be changed in the future
		 */
		
		String colName2Use = colName;
		if(table != null) // this is a derived data
		{
			String tableAlias = getAlias(table);
			String logicalName = table;
			
			// if engine is not null, get the info from the engine
			if(engine != null) {
				// get the correct column name
				// if it is a prim_key_placeholder
				// get the prim key for the table
				if(colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)){
					colName2Use = this.getPrimKey4Table(table);
					
					// TODO: what should the logical name actually be in this situation? 
					// TODO: fix this later...
					logicalName = table;
					
				} else {
					// default assumption is the info being passed is the conceptual name
					// get the physical from the conceptual
					String conceptualURI = "http://semoss.org/ontologies/Relation/Contains/" + colName;
					colName2Use = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);
					
					// TODO: this occurs when we have the old version of the OWL file
					if(colName2Use.equals(conceptualURI)) {
						// in this case, just use the instance name
						colName2Use = Utility.getInstanceName(colName2Use);
					} else {
						// this should be the default case once new OWL is only possibility
						colName2Use = Utility.getClassName(colName2Use);
					}
					logicalName = colName;
				}
				
				// get the logical name for the column
//				String physUri = "http://semoss.org/ontologies/Concept/" + colName2Use + "/" + table;
//				logicalName = engine.getTransformedNodeName(physUri, true);
//				if(physUri.equals(logicalName)){ // this means it didn't find the logical name. this means that its a property rather than a node
//					logicalName = engine.getTransformedNodeName("http://semoss.org/ontologies/Relation/Contains/" + colName2Use, true);
//				}
			}
			
			colName2Use = tableAlias + "." + colName2Use + " AS " + Utility.getInstanceName(logicalName);
		}
		
		if(selectors.length() == 0)
			selectors = colName2Use;
		else
			selectors = selectors + " , " + colName2Use;
	}

	
	public void addSelectors() {
		Enumeration <String> selections = qs.selectors.keys();
		while(selections.hasMoreElements())
		{
			String tableName = selections.nextElement();
			Vector <String> columns = qs.selectors.get(tableName);
			
			for(int colIndex = 0;colIndex < columns.size(); colIndex++)
			{
				addSelector(tableName, columns.get(colIndex));
				// adds the from if it isn't part of a join
				if(notUsedInJoin(tableName)){
					addFrom(tableName);
				}
			}
		}
	}
	
	private boolean notUsedInJoin(String tableName) {
		for(String key : qs.relations.keySet() ) {
			Hashtable<String, Vector> comparatorOptions = qs.relations.get(key);
			
			String[] conProp = getConceptProperty(key);
			String concept = conProp[0];
			
			for(String comparator : comparatorOptions.keySet()) {
				Vector otherConceptsRelated = comparatorOptions.get(comparator);
				for(Object otherConceptObj : otherConceptsRelated) {
					String otherConceptStr = otherConceptObj + "";
					
					//check if actually a prop added as a concept
					if(otherConceptStr.contains("__")) {
						return false;
					}
					
					String[] otherConProp = getConceptProperty(otherConceptStr);
					String otherConcept = otherConProp[0];
					
					String[] relationships = getRelationshipConceptProperties(concept, otherConcept);
					String toConcept = relationships[2];
					
					if(toConcept.equals(tableName)){
						return false;
					}
				}
			}
		}
		return true;
		
		//old logic
//		Collection<Hashtable<String, Vector>> options = qs.relations.values();
//		for(Hashtable<String, Vector> opt : options){
//			Collection<Vector> collection = opt.values();
//			for(Vector vec : collection){
//				for(Object obj : vec){
//					String objString = obj + "";
//					
//					String[] conProp = getConceptProperty(objString);
//					String concept = conProp[0];
//					
////					String[] relationships = getRelationshipConceptProperties(fromCol, toCol);
//					
//					if(concept.equals(tableName)){
//						return false;
//					}
//				}
//			}
//		}
//		return true;
	}
	
	public void addFilters()
	{
		Enumeration <String> concepts = qs.andfilters.keys();
		
		while(concepts.hasMoreElements())
		{
			String concept_property = concepts.nextElement();
			
			// inside this is a hashtable of all the comparators
			Hashtable <String, Vector> compHash = qs.andfilters.get(concept_property);
			Enumeration <String> comps = compHash.keys();
			
			// when adding implicit filtering from the dataframe as a pretrans that gets appended into the QS
			// we store the value without the parent__, so need to check here if it is stored as a prop in the engine
			if(engine != null) {
				String parent = engine.getParentOfProperty(concept_property);
				if(parent != null) {
					concept_property = Utility.getClassName(parent) + "__" + concept_property;
				}
			}
			String[] conProp = getConceptProperty(concept_property);
			String concept = conProp[0];
			String property = conProp[1];
			
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

				if(thisComparator == null || thisComparator.trim().equals("=")) {
					for(int optIndex = 0;optIndex < options.size(); optIndex++){
						addEqualsFilter(concept, property, thisComparator, options.get(optIndex) + "");
					}
				} else {
					for(int optIndex = 0;optIndex < options.size(); optIndex++){
						addFilter(concept, property, thisComparator, options.get(optIndex)+"");
					}
				}
			}
		}
	}
	
	
	private void addFilter(String concept, String property, String thisComparator, String object) {
		String thisWhere = "";
		String key = concept +":::"+ property +":::"+ thisComparator;
		if(!whereHash.containsKey(key))
		{
			if(object instanceof String) // ok this is a string
			{
				object = object.replace("\"", ""); // get rid of the space
				object = object.replaceAll("'", "''");
				object = object.trim();
				object = "\'" + object + "\'";
				//thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;
			}
			thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;		
			
		}
		else
		{
			thisWhere = whereHash.get(key);
			if(object instanceof String) // ok this is a string
			{
				object = object.replaceAll("\"", ""); // get rid of the space
				object = object.replaceAll("'", "''");
				object = object.trim();
				object = "\'" + object + "\'";
				//thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;
			}
			thisWhere = thisWhere + " OR " + getAlias(concept) + "." + property + " " + thisComparator + " " + object;						
		}
	//	System.out.println("WHERE " + thisWhere);
		whereHash.put(key, thisWhere);
	}
	
	//we want the filter query to be: "... where table.column in ('value1', 'value2', ...) when the comparator is '='
	private void addEqualsFilter(String concept, String property, String thisComparator, String object) {
		String thisWhere = "";
		String key = concept +":::"+ property +":::"+ thisComparator;
		if(!whereHash.containsKey(key))
		{
			if(object instanceof String) // ok this is a string
			{
				object = object.replace("\"", ""); // get rid of the space
				object = object.replaceAll("'", "''");
				object = object.trim();
				object = "\'" + object + "\'";
				//thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;
			}
			thisWhere = object;		
			
		}
		else
		{
			thisWhere = whereHash.get(key);
			if(object instanceof String) // ok this is a string
			{
				object = object.replaceAll("\"", ""); // get rid of the space
				object = object.replaceAll("'", "''");
				object = object.trim();
				object = "\'" + object + "\'";
				//thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;
			}
			thisWhere = thisWhere + ", " +object;						
		}
	//	System.out.println("WHERE " + thisWhere);
		whereHash.put(key, thisWhere);
	}
	public void addJoins()
	{
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

				for(int optIndex = 0; optIndex < options.size(); optIndex++) {
					String joinCols = options.get(optIndex);
					// when joining on a concept that is actually a property, you don't need this, just addFrom to get table
					if(joinCols.contains("__")) {
						String table = joinCols.substring(0, joinCols.indexOf("__"));
						if(table.equals(concept_property)) {
							addFrom(concept_property);
							continue;
						}
					} 
					//TODO: need to check if this is still possible
					else if(joinCols.equals(concept_property)) {
						addFrom(concept_property);
						continue;
					}
					addJoin(concept_property, thisComparator, options.get(optIndex));
				}
			}
		}		
	}
	
	private void addJoin(String fromCol,
			String thisComparator, String toCol) {
		// this needs to be revamped pretty extensively
		// I need to add this back to the from because I might not be projecting everything
		String thisWhere = "";
		String[] relConProp = getRelationshipConceptProperties(fromCol, toCol);
		String concept = relConProp[0];
		String property = relConProp[1];
		String toConcept = relConProp[2];
		String toProperty = relConProp[3];
		
//		String key = toConcept + toProperty + thisComparator;
		String key = toConcept + thisComparator;
		if(notUsedInJoin(concept)){
			addFrom(concept);
		}
//		addFrom(toConcept);
		if(!relationHash.containsKey(key))
		{
			String compName = thisComparator.replace(".", "  ");	
			thisWhere = compName + "  " + toConcept+ " " + getAlias(toConcept) + " ON " + getAlias(concept) + "." + property + " = " + getAlias(toConcept) + "." + toProperty;			
		}
		else
		{
			thisWhere = relationHash.get(key);
			thisWhere = thisWhere + " AND " + getAlias(concept) + "." + property + " = " + getAlias(toConcept) + "." + toProperty;			
		}
		relationHash.put(key, thisWhere);
	}


	public static String getAlias(String tableName)
	{
		String response = null;
		if(aliases.containsKey(tableName))
			response = aliases.get(tableName);
		else
		{
			boolean aliasComplete = false;
			int count = 0;
			String tryAlias = "";
			while(!aliasComplete)
			{
				if(tryAlias.length()>0){
					tryAlias+="_";//prevent an error where you may create an alias that is a reserved word (ie, we did this with "as")
				}
				tryAlias = (tryAlias + tableName.charAt(count)).toUpperCase();
				aliasComplete = !aliases.containsValue(tryAlias);
				count++;
			}
			response = tryAlias;
			aliases.put(tableName, tryAlias);
		}
		return response;
	}

	/*
	 * Gets the table uri
	 * Parses uri for prim key
	 */
	private String getPrimKey4Table(String table){
		if(primaryKeyCache.containsKey(table)){
			return primaryKeyCache.get(table);
		}
		else if (engine != null){
			if(primaryKeyCache.containsKey(table)) {
				return primaryKeyCache.get(table);
			}
			
			String conceptualURI = "http://semoss.org/ontologies/Concept/" + table;
			String physicalURI = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);
			
			String primKey = "";
			// TODO: this occurs when we have the old version of the OWL file
			if(conceptualURI.equals(physicalURI)) {
				// in this case, just use the instance name
				primKey = Utility.getInstanceName(physicalURI);
			} else {
				// this should be the default case once new OWL is only possibility
				primKey = Utility.getClassName(physicalURI);
			}
			
			primaryKeyCache.put(table, primKey);
			return primKey;
		}
		return table;
	}
	
	/*
	 * Single method to handle the parsing of the strings for "__"
	 * If string does not contain "__" we get the primary key
	 */
	private String[] getConceptProperty(String concept_property){
		String concept = concept_property;
		String property = null;
		if(concept_property.contains("__")){
			concept = concept_property.substring(0, concept_property.indexOf("__"));
			property = concept_property.substring(concept_property.indexOf("__")+2);
		}
		else{
			property = getPrimKey4Table(concept);
		}
		return new String[]{concept, property};
	}
	
	private String[] getRelationshipConceptProperties(String fromString, String toString){
		if(relationshipConceptPropertiesMap.containsKey(fromString + "__" + toString)) {
			return relationshipConceptPropertiesMap.get(fromString + "__" + toString);
		}
		
		String fromTable = null;
		String fromCol = null;
		String toTable = null;
		String toCol = null;
		
		if(fromString.contains("__")){
			fromTable = fromString.substring(0, fromString.indexOf("__"));
			fromCol = fromString.substring(fromString.indexOf("__")+2);
		}
		
		if(toString.contains("__")){
			toTable = toString.substring(0, toString.indexOf("__"));
			toCol = toString.substring(toString.indexOf("__")+2);
		}
		
		// if one has a property specified, all we can do is get prim key for the other
		// if both have a property no lookup is needed at all
		// if neither has a property specified, use owl to look up foreign key relationship
		if(fromTable != null && toTable == null){
			String[] toConProp = getConceptProperty(toString);
			toTable = toConProp[0];
			toCol = toConProp[1];
		}
		
		else if(fromTable == null && toTable != null){
			String[] fromConProp = getConceptProperty(fromString);
			fromTable = fromConProp[0];
			fromCol = fromConProp[1];
		}
		
		else if(engine != null && (fromCol == null && toCol == null)) // in this case neither has a property specified. time to go to owl to get fk relationship
		{
			String fromURI = this.engine.getConceptUri4PhysicalName(fromString);
			String toURI = this.engine.getConceptUri4PhysicalName(toString);
			
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
			String[] predPieces = Utility.getInstanceName(predURI).split("[.]");
			fromTable = predPieces[0];
			fromCol = predPieces[1];
			toTable = predPieces[2];
			toCol = predPieces[3];
		}
		
		String[] retArr = new String[]{fromTable, fromCol, toTable, toCol};
		relationshipConceptPropertiesMap.put(fromString + "__" + toString, retArr);
		
		return retArr;
	}

}
