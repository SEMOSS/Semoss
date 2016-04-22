package prerna.sablecc;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;

public class ColFilterReactor extends AbstractReactor{

//	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public ColFilterReactor() {
		String [] thisReacts = {PKQLEnum.FILTER}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.FILTER_DATA;
	}
	
	@Override
	public Iterator process() {
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		
		Vector<Hashtable> filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
		this.processFilters(frame, filters);
		return null;
	}
	
	private void processFilters(ITableDataFrame frame, Vector<Hashtable> filters) {
		
		for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
		{
			Object thisObject = filters.get(filterIndex);
			
			Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
			String fromCol = (String)thisFilter.get("FROM_COL");
			String toCol = null;
			Vector filterData = new Vector();
			if(thisFilter.containsKey("TO_COL")) {
				toCol = (String)thisFilter.get("TO_COL");
			}
			else {
				filterData = (Vector)thisFilter.get("TO_DATA");
				List<Object> cleanedFilterData = new ArrayList<>(filterData.size());
				for(Object data : filterData) {
					String cleandata = data.toString();
					cleandata = cleandata.replace("\"", "").trim();
					cleanedFilterData.add(cleandata);
				}
				String comparator = (String)thisFilter.get("COMPARATOR");
				frame.filter(fromCol, cleanedFilterData);
				myStore.put("STATUS", "SUCCESS");
				myStore.put("FILTER_VALUES", cleanedFilterData.toString());
			}
		}
	}

}
