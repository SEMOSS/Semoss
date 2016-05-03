package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.engine.api.IApi;
import prerna.engine.impl.rdf.QueryAPI;

public class ApiReactor extends AbstractReactor {


	public ApiReactor()
	{
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS, "KEY_VALUE"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.API;
	}


	@Override
	public Iterator process() {

		System.out.println("Processed.. " + myStore);
		// TODO Auto-generated method stub
		/*
		 * Come to you in a min
		 * 
		 */
		//System.out.println("Will pull the data on this one and then make the calls to add to other things");


		Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
		Vector <String> tinkerSelectors = new Vector<String>();

		String engine = (String)myStore.get("ENGINE");
		String insight = (String)myStore.get("INSIGHT");
		String fileLocation = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("file");
		
		
		// I need to instantiate the engine here
		// for now hard coding it
		IApi qapi = new QueryAPI();
		if(engine != null && !engine.equals("csvFile")) {
			qapi.set("ENGINE", engine);
		} else {
			qapi.set("ENGINE", fileLocation);
		}

		Vector <String> selectors = new Vector<String>();
		Vector <Hashtable> filters = new Vector<Hashtable>();
		Vector <Hashtable> joins = new Vector<Hashtable>();

		if(myStore.containsKey(PKQLEnum.COL_CSV) && ((Vector)myStore.get(PKQLEnum.COL_CSV)).size() > 0)
			selectors = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
		if(myStore.containsKey(PKQLEnum.FILTER) && ((Vector)myStore.get(PKQLEnum.FILTER)).size() > 0)
			filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
		if(myStore.containsKey(PKQLEnum.JOINS) && ((Vector)myStore.get(PKQLEnum.JOINS)).size() > 0)
			joins = (Vector<Hashtable>) myStore.get(PKQLEnum.JOINS);

		QueryStruct qs = new QueryStruct();
		processQueryStruct(qs, selectors, filters, joins);
		qapi.set("QUERY_STRUCT", qs);
		Iterator thisIterator = qapi.process();

		Map<String, Set<String>> edgeHash = qs.getReturnConnectionsHash();
		this.put("EDGE_HASH", edgeHash);

		String nodeStr = (String)myStore.get(whoAmI);
		myStore.put(nodeStr, thisIterator);

		// eventually I need this iterator to set this back for this particular node
		return null;
	}

	public void processQueryStruct(QueryStruct qs, Vector <String> selectors, Vector <Hashtable> filters, Vector <Hashtable> joins)
	{

		for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
		{
			String thisSelector = selectors.get(selectIndex);
			if(thisSelector.contains("__")){
				String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
				String property = thisSelector.substring(thisSelector.indexOf("__")+2);
				qs.addSelector(concept, property);
			}
			else
			{
				qs.addSelector(thisSelector, null);
			}
		}
		for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
		{
			Object thisObject = filters.get(filterIndex);
			System.out.println(thisObject.getClass());
			if(thisObject instanceof Hashtable)
				System.out.println("I just dont what is wrong.. ");

			Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
			String fromCol = (String)thisFilter.get("FROM_COL");
			String toCol = null;
			Vector filterData = new Vector();
			if(thisFilter.containsKey("TO_COL"))
			{
				toCol = (String)thisFilter.get("TO_COL");
				//filtersToBeElaborated.add(thisFilter);
				//tinkerSelectors.add(toCol);
				// need to pull this from tinker frame and do the due
				// interestingly this could be join
			}
			else
			{
				// this is a vector do some processing here					
				filterData = (Vector)thisFilter.get("TO_DATA");
				String comparator = (String)thisFilter.get("COMPARATOR");
				//				String concept = fromCol.substring(0, fromCol.indexOf("__"));
				//				String property = fromCol.substring(fromCol.indexOf("__")+2);
				qs.addFilter(fromCol, comparator, filterData);
			}
		}
		for(int joinIndex = 0;joinIndex < joins.size();joinIndex++)
		{
			Hashtable thisJoin = (Hashtable)joins.get(joinIndex);

			String fromCol = (String)thisJoin.get("FROM_COL");
			String toCol = (String)thisJoin.get("TO_COL");

			String relation = (String)thisJoin.get("REL_TYPE");	
			qs.addRelation(fromCol, toCol, relation);
		}	
	}
}
