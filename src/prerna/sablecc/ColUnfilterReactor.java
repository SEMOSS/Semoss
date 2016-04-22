package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;

public class ColUnfilterReactor extends AbstractReactor{

	public ColUnfilterReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.UNFILTER_DATA;
	}
	
	@Override
	public Iterator process() {
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
//		System.out.println("My Store on COL CSV " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		
		Vector<String> column = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
		
		for(String c : column) {
			frame.unfilter(c);
			myStore.put("STATUS", "SUCCESS");
			myStore.put("FILTER_COLUMN", c);
		}
		return null;
	}

}
