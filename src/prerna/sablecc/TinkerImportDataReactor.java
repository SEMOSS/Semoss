package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class TinkerImportDataReactor extends ImportDataReactor{

	@Override
	public Iterator process() {
		// get the frame
		TinkerFrame frame = (TinkerFrame) myStore.get("G");

		// additional logic required for loops
		// we need to also take into consideration a meta level header
		// that doesn't match the actual instance name
		// i.e. we create a meta level System_1 vertex
		// but the instances are still only referenced as System
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if(joins!=null){
			for(Map<String,String> join : joins) {
				String fromCol = join.get(PKQLEnum.FROM_COL);
				// here we replace the to column with the actual instance name
				fromCol = frame.getValueForUniqueName(fromCol);
				join.put(PKQLEnum.FROM_COL, fromCol);
			}
		}
		
		// use the import data reactor to go through the logic to get the necessary data 
		super.process();
		
		// cardinality helps determine the relationship between the instance values in a column
		// this is helpful in optimizing the extraction of the upstream node and downstream node when creating
		// these vertices in the TinkerFrame
		Map<Integer, Set<Integer>> cardinality = Utility.getCardinalityOfValues(this.newHeaders, this.edgeHash);;
		
		while(this.dataIterator.hasNext()){
			IHeadersDataRow ss = this.dataIterator.next();
			
			if(isPrimKey) {
				frame.addRow(ss.getValues(), this.newHeaders);
			} else {
				frame.addRelationship(this.newHeaders, ss.getValues(), cardinality, this.modifyNamesMap);
			}
		}
		
		// store the response string
		inputResponseString(this.dataIterator, this.newHeaders);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
}
