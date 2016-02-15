package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.util.sql.SQLQueryUtil;

public class SQLInterpreter {
	
	// core class to convert the query struct into a sql query
	QueryStruct qs = null;
	private static Hashtable <String,String> aliases = new Hashtable<String,String>();
	Hashtable <String, String> tableProcessed = new Hashtable<String, String>();
	
	// where the wheres are all kept
	// key is always a combination of concept and comparator
	// and the values are values
	Hashtable <String, String> whereHash = new Hashtable<String, String>();

	Hashtable <String, String> relationHash = new Hashtable<String, String>();

	List<String> joinsArr = new ArrayList<String>();
	List<String> leftJoinsArr = new ArrayList<String>();
	List<String> rightJoinsArr = new ArrayList<String>();

	private SQLQueryUtil queryUtil;

	String selectors = "";
	String froms = "";
	String curWhere = "";
	String allWhere = "";
	
	public SQLInterpreter(QueryStruct qs)
	{
		this.qs = qs;
		
	}
	
	public String composeQuery()
	{
		String query = null;
		addSelectors();
		addFilters();
		addJoins();
		
		//System.out.println("Select ..  " + selectors);
		//System.out.println("From ..  " + froms);
		//System.out.println("Where ..  " + whereHash);
		//System.out.println("With Join ..  " + relationHash);
		
		// the final step where the equation is balanced and the anamoly revealed
		query = "SELECT  " + selectors + "  FROM " + froms + " WHERE ";
		boolean firstTime = true;
		
		// filters
		Enumeration wheres = whereHash.keys();
		while(wheres.hasMoreElements())
		{
			String value = whereHash.get(wheres.nextElement());
			if(value.contains(" OR "))
				value = " ( " + value + " ) ";
			
			if(firstTime)
			{
				query = query + " " + value;
				firstTime = false;
			}
			else
				query = query + " AND " + value;
		}

		Enumeration joins = relationHash.keys();
		firstTime = true;
		while(joins.hasMoreElements())
		{
			String value = relationHash.get(joins.nextElement());

			if(firstTime)
			{
				query = query + " " + value;
				firstTime = false;
			}
			else
				query = query + " AND " + value;
		}

		System.out.println("QUERY....  " + query);
		return query;
	}

	private void addFrom(String tableName)
	{
		String alias = getAlias(tableName);
		if(!tableProcessed.containsKey(tableName))
		{
			tableProcessed.put(tableName,"true");
			String fromText =  tableName + "  " + alias;
			if(froms.length() > 0){
				froms = froms + " , " + fromText;
				//rightJoinsArr.add(queryUtil.getDialectOuterJoinRight(fromText));
				//leftJoinsArr.add(queryUtil.getDialectOuterJoinLeft(fromText));
			} else {
				froms = fromText;
				//rightJoinsArr.add(fromText);
				//leftJoinsArr.add(fromText);
			}
		}
	}

	
	// add from
	public void addSelector(String table, String colName)
	{
		// the table can be null
		if(table != null) // this is a derived data
		{
			String tableAlias = getAlias(table);
			colName = tableAlias + "." + colName;
		}
		if(selectors.length() == 0)
			selectors = colName;
		else
			selectors = selectors + " , " + colName;
	}

	
	public void addSelectors()
	{
		Enumeration <String> selections = qs.selectors.keys();
		while(selections.hasMoreElements())
		{
			String tableName = selections.nextElement();
			Vector <String> columns = qs.selectors.get(tableName);
			
			for(int colIndex = 0;colIndex < columns.size();addSelector(tableName, columns.get(colIndex)), addFrom(tableName), colIndex++); // adds the from as well
		}
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
			String concept = concept_property.substring(0, concept_property.indexOf("__"));
			String property = concept_property.substring(concept_property.indexOf("__")+2);
			
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

				for(int optIndex = 0;optIndex < options.size(); addFilter(concept, property, thisComparator, options.get(optIndex) + ""), optIndex++);
			}
		}
	}
	
	
	private void addFilter(String concept, String property,
			String thisComparator, String object) {
		// TODO Auto-generated method stub
		String thisWhere = "";
		String key = concept + property + thisComparator;
		if(!whereHash.containsKey(key))
		{
			if(object.indexOf("\"") >= 0) // ok this is a string
			{
				object = object.replace("\"", ""); // get rid of the space
				object = object.trim();
				object = "\"" + object + "\"";
				//thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;
			}
			thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;		
			
		}
		else
		{
			thisWhere = whereHash.get(key);
			if(object.indexOf("\"") >= 0) // ok this is a string
			{
				object = object.replaceAll("\"", ""); // get rid of the space
				object = object.trim();
				object = "\"" + object + "\"";
				//thisWhere = getAlias(concept) + "." + property + " " + thisComparator + " " + object;
			}
			thisWhere = thisWhere + " OR " + getAlias(concept) + "." + property + " " + thisComparator + " " + object;						
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
			String concept = concept_property.substring(0, concept_property.indexOf("__"));
			String property = concept_property.substring(concept_property.indexOf("__")+2);
			
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

				for(int optIndex = 0;optIndex < options.size(); addJoin(concept, property, thisComparator, options.get(optIndex)), optIndex++);
			}
		}		
	}
	
	private void addJoin(String concept, String property,
			String thisComparator, String toCol) {
		// TODO Auto-generated method stub
		// this needs to be revamped pretty extensively
		// I need to add this back to the from because I might not be projecting everything
		String thisWhere = "";
		String key = concept + property + thisComparator;
		String toConcept = toCol.substring(0,toCol.indexOf("__"));
		String toProperty = toCol.substring(toCol.indexOf("__")+2);
		addFrom(concept);
		addFrom(toConcept);
		if(!relationHash.containsKey(key))
		{
			String compName = thisComparator.replace(".", "  ");	
			thisWhere = compName + "  " + toConcept + " ON " + getAlias(concept) + "." + property + " = " + getAlias(toConcept) + "." + toProperty;			
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


}
