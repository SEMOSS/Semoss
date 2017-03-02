package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.ds.h2.H2Frame;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Constants;

public class H2FrameApiReactor extends AbstractApiReactor {

	public Iterator process() {
		super.process();
		
		this.put((String) getValue(PKQLEnum.API), this.qs);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		return null;
	}
	
	@Override
	public void processQueryStruct(Vector <String> selectors, Vector <Hashtable> filters, Vector <Hashtable> joins, int limit, int offset) {
		Map<String, Map<String, Object>> varMap = (Map<String, Map<String, Object>>) myStore.get("VARMAP");
		H2Frame frame = (H2Frame)myStore.get("G");
		String tableName = frame.getBuilder().getTableName();
		
		for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
		{
			String thisSelector = selectors.get(selectIndex);
			this.qs.addSelector(tableName, thisSelector);
			
			// For this column, see if there is a param set that references it
			// If so, grab it's value and add as a filter to apply that param
			for(String var : varMap.keySet()) {
				Map<String, Object> paramValues = varMap.get(var);
				if(paramValues != null && paramValues.get(Constants.TYPE).equals(thisSelector)) {
					Vector<String> filterValues = new Vector<String>();
					filterValues.add(paramValues.get(Constants.VALUE).toString());
					this.qs.addFilter(thisSelector, "=", filterValues);
				}
			}
		}
		for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
		{
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
				
				// For this column filter, see if there is a param set that references it
				// If so, grab it's value and add as a filter to apply that param
				for(String var : varMap.keySet()) {
					Map<String, Object> paramValues = varMap.get(var);
					if(paramValues != null && paramValues.get(Constants.TYPE).equals(fromCol)) {
						filterData.clear();
						filterData.add(paramValues.get(Constants.VALUE).toString());
					}
				}
				
				this.qs.addFilter(tableName+"__"+fromCol, comparator, filterData);
			}
		}
		for(int joinIndex = 0;joinIndex < joins.size();joinIndex++)
		{
			Hashtable thisJoin = (Hashtable)joins.get(joinIndex);

			String fromCol = (String)thisJoin.get("FROM_COL");
			String toCol = (String)thisJoin.get("TO_COL");

			String relation = (String)thisJoin.get("REL_TYPE");	
			this.qs.addRelation(fromCol, toCol, relation);
		}
		
		this.qs.setLimit(limit);
		this.qs.setOffSet(offset);
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		return null;
	}
}
