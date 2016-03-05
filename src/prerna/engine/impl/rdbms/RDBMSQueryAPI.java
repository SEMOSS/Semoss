package prerna.engine.impl.rdbms;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.engine.api.IApi;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter;
import prerna.util.DIHelper;

public class RDBMSQueryAPI implements IApi {
	
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	String [] params = {"SELECTORS", "FILTERS", "JOINS", "DB"};
	
	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		values.put(key, value);
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// get basic data
		Vector <String> selectors = (Vector<String>)values.get(params[0]);
		Vector <Hashtable> filters = ((Vector<Hashtable>)values.get(params[1]));
		Vector <Hashtable> joins = ((Vector<Hashtable>)values.get(params[2]));

		// compose
		QueryStruct qs = new QueryStruct();
		processQueryStruct(qs,selectors, filters, joins);
		qs.print();
		SQLInterpreter sqlI = new SQLInterpreter(qs);
		String query = sqlI.composeQuery();
		
		// play
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(values.get(params[3]) + ""); 
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		
		// give back
		return wrapper;
		//return null;
	}

	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return params;
	}
	
	public void processQueryStruct(QueryStruct qs, Vector <String> selectors, Vector <Hashtable> filters, Vector <Hashtable> joins)
	{
		
		for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
		{
			String thisSelector = selectors.get(selectIndex);
			String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
			String property = thisSelector.substring(thisSelector.indexOf("__")+2);
			qs.addSelector(concept, property);
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
				String concept = fromCol.substring(0, fromCol.indexOf("__"));
				String property = fromCol.substring(fromCol.indexOf("__")+2);
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
