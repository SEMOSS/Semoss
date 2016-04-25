package prerna.sablecc;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.Utility;

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
					String cleandata = data.toString().trim();
					if((cleandata.startsWith("\"") && cleandata.endsWith("\"")) || (cleandata.startsWith("'") && cleandata.endsWith("'"))) {
						cleandata = cleandata.substring(1, cleandata.length() - 1);
					}
					cleandata = Utility.cleanString(cleandata, true, true, false);
					cleanedFilterData.add(cleandata);
				}
				String comparator = (String)thisFilter.get("COMPARATOR");
				try {
					frame.filter(fromCol, cleanedFilterData, comparator);
					myStore.put("STATUS", "SUCCESS");
					myStore.put("FILTER_RESPONSE", "Filtered Column: " + fromCol);
				} catch(IllegalArgumentException e) {
					myStore.put("FILTER_RESPONSE", e.getMessage());
				}
			}
		}
	}

}
