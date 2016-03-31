package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.engine.api.IApi;
import prerna.engine.impl.rdf.QueryAPI;

public class ApiReactor extends AbstractReactor {
	
	
	public ApiReactor()
	{
		String [] thisReacts = {TokenEnum.COL_CSV, TokenEnum.FILTER, TokenEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.API;
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
			
			// I need to instantiate the engine here
			// for now hard coding it
			IApi qapi = new QueryAPI();
			qapi.set("ENGINE", engine);

			Vector <String> selectors = new Vector<String>();
			Vector <Hashtable> filters = new Vector<Hashtable>();
			Vector <Hashtable> joins = new Vector<Hashtable>();
			
			if(myStore.containsKey(TokenEnum.COL_CSV) && ((Vector)myStore.get(TokenEnum.COL_CSV)).size() > 0)
				selectors = (Vector<String>) myStore.get(TokenEnum.COL_CSV);
			if(myStore.containsKey(TokenEnum.FILTER) && ((Vector)myStore.get(TokenEnum.FILTER)).size() > 0)
				filters = (Vector<Hashtable>) myStore.get(TokenEnum.FILTER);
			if(myStore.containsKey(TokenEnum.JOINS) && ((Vector)myStore.get(TokenEnum.JOINS)).size() > 0)
				joins = (Vector<Hashtable>) myStore.get(TokenEnum.JOINS);

			QueryStruct qs = new QueryStruct();
			processQueryStruct(qs, selectors, filters, joins);
			qapi.set("QUERY_STRUCT", qs);
			this.put("QUERY_STRUCT", qs);
			
			Iterator thisIterator = qapi.process();
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
