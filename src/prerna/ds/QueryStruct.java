package prerna.ds;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.rdf.query.builder.SPARQLInterpreter;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QueryStruct {

	
	
	// what is in a query
	// 1. selectors - what is it that we want to give back
	// 2. filters - what are the things that we want to filter ooo.. profound
	// Title = ["Ants story", "etc"] etc..
	// Studio = ["WB", "Fox"]
	// 3. How you want to join
	// Title.Title Inner_Join Studio.Title_Fk
		
	public Hashtable <String, Vector<String>> selectors = new Hashtable<String, Vector<String>>();
	
	// there could be multiple comparators for the same thing
	// for instance I could say
	// moviebudget > 200 and < 300
	// so it would go as
	// | Movie Budget |  > | Vector(200) | // or this could be a whole object / vector
	//				  |  < | Vector(300) |
	public Hashtable <String, Hashtable<String, Vector>> andfilters = new Hashtable<String, Hashtable<String, Vector>>();

	//Hashtable <String, Hashtable<String, Vector>> orfilters = new Hashtable<String, Hashtable<String, Vector>>();
	// relations are of the form
	// item = <relation vector>
	// concept = type of join toCol
	// Movie	 InnerJoin Studio, Genre
	//			 OuterJoin Nominated
	public Hashtable <String, Hashtable<String, Vector>> relations = new Hashtable<String, Hashtable<String, Vector>>();
	
	public static String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";
		
	public void addCompoundSelector(String selector)
	{
		// need to break it and then add it
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
		Hashtable <String, Vector> compHash = new Hashtable<String, Vector>();
		if(andfilters.containsKey(fromCol))
			compHash = andfilters.get(fromCol);
		
		Vector curData = new Vector();
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
		
		
		Hashtable <String, Vector> compHash = new Hashtable<String, Vector>();
		if(relations.containsKey(fromConcept))
			compHash = relations.get(fromConcept);
		
		Vector curData = new Vector();
		// next piece is to see if we have the comparator
		if(compHash.containsKey(comparator))
			curData = compHash.get(comparator);
		
		curData.add(toConcept);
		
		// put it back
		compHash.put(comparator, curData);	
		
		// put it back
		relations.put(fromConcept, compHash);
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

	public void print() {
		// TODO Auto-generated method stub
		System.out.println("SELECTORS " + selectors);
		System.out.println("FILTERS.. " + andfilters);
		System.out.println("RELATIONS.. " + relations);
	}
	
	public Hashtable<String, Hashtable<String, Vector>> getRelations(){
		return this.relations;
	}
	
	public Hashtable<String, Vector<String>> getSelectors(){
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

		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		// First need to iterate through properties
		// These are the easiest to capture
		// Need to make sure valid return value

		for(String selectorKey: this.selectors.keySet()){
			Vector<String> props = this.selectors.get(selectorKey);
			Set<String> downNodeTypes = edgeHash.get(selectorKey);
			if(downNodeTypes == null){
				downNodeTypes = new HashSet<String>();
				edgeHash.put(selectorKey, downNodeTypes);
			}
			props.remove(this.PRIM_KEY_PLACEHOLDER); // make sure we don't add a node to itself (e.g. Title__Title)
			for(String prop : props){
				downNodeTypes.add(selectorKey + "__" + prop); //mergeQSEdgeHash needs this... plus need to keep it consistent with relations
			}
		}
		
		if(this.relations != null){
			for(String relationsKey : this.relations.keySet()){
				// get the concept for the relation
				String item = storeRelationsKey(relationsKey, edgeHash);
				Set<String> downNodeTypes = edgeHash.get(item);
				Map<String, Vector> relHash = this.relations.get(relationsKey);
				
				for(Vector<String> vec : relHash.values()){
					// need to get the concept for each in the vector
					for(String downString : vec){
						String downItem = storeRelationsKey(downString, edgeHash);
						downNodeTypes.add(downItem);
					}
				}

			}
		}

		return edgeHash;
	}
	
	private String storeRelationsKey(String relationsKey, Map<String, Set<String>> edgeHash){
		String item = relationsKey;
		if(!relationsKey.contains("__")){
			Set<String> downNodeTypes = edgeHash.get(relationsKey);
			if(downNodeTypes == null){
				downNodeTypes = new HashSet<String>();
			}
			edgeHash.put(relationsKey, downNodeTypes);
		}
		else {
			String relConcept = relationsKey.substring(0, relationsKey.indexOf("__"));
			Set<String> downNodeTypes = edgeHash.get(relationsKey);
			if(downNodeTypes == null){
				downNodeTypes = new HashSet<String>();
			}
			edgeHash.put(relationsKey, downNodeTypes);
			
			// also store concept -> property
			Set<String> downNodeTypes2 = edgeHash.get(relConcept);
			if(downNodeTypes2 == null){
				downNodeTypes2 = new HashSet<String>();
			}
			downNodeTypes2.add(relationsKey);
			edgeHash.put(relConcept, downNodeTypes2);
		}
		return item;
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
	
	public static void main(String [] args) throws Exception
	{
		QueryStruct qs = new QueryStruct();
		qs.addSelector("Title", "Title");
		qs.addFilter("Title__Title", "=", Arrays.asList(new String[]{"WB", "ABC"}));
		qs.addRelation("Title__Title", "Actor__Title_FK", "inner.join");
		
		Gson gson = new Gson();
		System.out.println(gson.toJson(qs));
		
		loadEngine4Test();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp("Movie_DB"); 
		SPARQLInterpreter in = new SPARQLInterpreter(engine);
		
		in.setQueryStruct(qs);
		String query = in.composeQuery();
		System.out.println(query);
	}

	private static void loadEngine4Test(){
		DIHelper.getInstance().loadCoreProp("C:\\Users\\bisutton\\workspace\\SEMOSSDev\\RDF_Map.prop");
		FileInputStream fileIn = null;
		try{
			Properties prop = new Properties();
			String fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\UpdatedRDBMSMovies.smss";
			fileIn = new FileInputStream(fileName);
			prop.load(fileIn);
			System.err.println("Loading DB " + fileName);
			Utility.loadEngine(fileName, prop);
			fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\Movie_DB.smss";
			fileIn = new FileInputStream(fileName);
			prop.load(fileIn);
			System.err.println("Loading DB " + fileName);
			Utility.loadEngine(fileName, prop);
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
}
