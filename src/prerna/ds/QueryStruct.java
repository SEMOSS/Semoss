package prerna.ds;

import java.util.Hashtable;
import java.util.Vector;

public class QueryStruct {

	
	
	// what is in a query
	// 1. selectors - what is it that we want to give back
	// 2. filters - what are the things that we want to filter ooo.. profound
	// Title = ["Ants story", "etc"] etc..
	// Studio = ["WB", "Fox"]
	// 3. How you want to join
	// Title.Title Inner_Join Studio.Title_Fk
		
	Hashtable <String, Vector<String>> selectors = new Hashtable<String, Vector<String>>();
	
	// there could be multiple comparators for the same thing
	// for instance I could say
	// moviebudget > 200 and < 300
	// so it would go as
	// | Movie Budget |  > | Vector(200) | // or this could be a whole object / vector
	//				  |  < | Vector(300) |
	Hashtable <String, Hashtable<String, Vector>> andfilters = new Hashtable<String, Hashtable<String, Vector>>();

	//Hashtable <String, Hashtable<String, Vector>> orfilters = new Hashtable<String, Hashtable<String, Vector>>();
	// relations are of the form
	// item = <relation vector>
	Hashtable <String, Vector<String>> relations = new Hashtable<String, Vector<String>>();
		
	public void addCompoundSelector(String selector)
	{
		// need to break it and then add it
	}

	public void addSelector(String concept, String property)
	{
		if(property == null)
			property = concept;
		
		addToHash(concept, property, selectors);
		
	}
	
	
	public void addFilter(String concept, String property, String comparator, Vector filterData)
	{
		// the filter data is typically of the format
		// there could be more than one comparator
		
		// find if this property is there
		// ok if the logical name stops being unique this will have some weird results
		Hashtable <String, Vector> compHash = new Hashtable<String, Vector>();
		if(andfilters.containsKey(property))
			compHash = andfilters.get(property);
		
		Vector curData = new Vector();
		// next piece is to see if we have the comparator
		if(compHash.containsKey(comparator))
			curData = compHash.get(comparator);
		
		curData.addAll(filterData);
		
		// put it back
		compHash.put(comparator, curData);	
		
		// put it back
		andfilters.put(property, compHash);
	}
	
	public void addRelation(String fromConcept, String toConcept, String comparator)
	{
		// I need pick the keys from the table based on relationship and then add that to the relation
		// need to figure out type of 
		addToHash(fromConcept, toConcept, relations);
	}
	
	private void addToHash(String concept, String property, Hashtable <String, Vector<String>> hash)
	{
		// group it by table and you are done
		Vector <String> propList = new Vector<String>();
		
		if(hash.containsKey(concept))
			propList = hash.get(concept);
		
		propList.add(property);
		
		hash.put(concept, propList);

	}
}
