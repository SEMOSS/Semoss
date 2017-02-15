package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc.PKQLRunner.STATUS;

public class H2ImportDataReactor extends MetaH2ImportDataReactor {
	
	@Override
	public Iterator process() {
		// use the import data reactor to go through the logic to get the necessary data 
		super.process();
		
		// can we optimize by just doing an insert?
		if(isFrameEmpty) {
			frame.addRowsViaIterator(dataIterator);
		// are we revisiting paths already traveled and need to update existing values?
		} else if(allHeadersAccounted(startingHeaders, newHeaders)) {
			// we can just do a merge
			joinCols = updateJoinsCols(joinCols);
			// loop through the join cols and merge based on all overlapping column headers
			List<String> mergeJoinCols = new Vector<String>();
			for(String existingHeader : startingHeaders) {
				for(String newHeader : newHeaders) {
					if(existingHeader.equals(newHeader)) {
						mergeJoinCols.add(newHeader);
					}
				}
			}
			frame.mergeRowsViaIterator(dataIterator, newHeaders, startingHeaders, mergeJoinCols.toArray(new String[]{}));
		}
		// we are expanding to new paths not recently traveled
		else {
			
			// if logicalToValue is null, it will create it within the processIterator method
			
			//TODO: figure out processIterator to not have to do this...
			joinCols = updateJoinsCols(joinCols);
			frame.processIterator(dataIterator, newHeaders, modifyNamesMap, joinCols, joinType);
		}
		
		// store the response string
		inputResponseString(dataIterator, newHeaders);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
	//TODO: figure out processIterator to not have to do this...
	private Vector<Map<String, String>> updateJoinsCols(Vector<Map<String, String>> joinCols) {
		if(joinCols != null && !joinCols.isEmpty()) {
			Vector<Map<String, String>> newJoins = new Vector<Map<String, String>>();
			for(int i = 0; i < joinCols.size(); i++) {
				Map<String, String> join = joinCols.get(i);
				
				Map<String, String> newJoin = new Hashtable<String, String>();
				for(String name : join.keySet()) {
					newJoin.put(join.get(name), join.get(name));
				}
				newJoins.add(newJoin);
			}
			
			joinCols = newJoins;
		}
		return joinCols;
	}
}