package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class H2ImportDataReactor extends MetaH2ImportDataReactor {
	
	@Override
	public Iterator process() {
//		if(myStore.get(PKQLEnum.CHILD_ERROR) != null && (boolean) myStore.get(PKQLEnum.CHILD_ERROR)) {
//			myStore.put("STATUS", STATUS.ERROR);
//			String nodeStr = (String)myStore.get(PKQLEnum.EXPR_TERM);
//			if(myStore.get(PKQLEnum.CHILD_ERROR_MESSAGE) != null) {
//				myStore.put(nodeStr, myStore.get(PKQLEnum.CHILD_ERROR_MESSAGE));
//			}
//			return null;
//		}
//		// use the import data reactor to go through the logic to get the necessary data 
		super.process();
//		
//		// can we optimize by just doing an insert?
//		if(isFrameEmpty) {
//			frame.addRowsViaIterator(dataIterator);
//		// are we revisiting paths already traveled and need to update existing values?
//		} else if(!enableLoops && allHeadersAccounted(startingHeaders, newHeaders) ) {
//			// we can just do a merge
//			joinCols = updateJoinsCols(joinCols);
//			// loop through the join cols and merge based on all overlapping column headers
//			List<String> mergeJoinCols = new Vector<String>();
//			for(String existingHeader : startingHeaders) {
//				for(String newHeader : newHeaders) {
//					if(existingHeader.equals(newHeader)) {
//						mergeJoinCols.add(newHeader);
//					}
//				}
//			}
//			frame.mergeRowsViaIterator(dataIterator, newHeaders, startingHeaders, mergeJoinCols.toArray(new String[]{}));
//		}
//		// we are expanding to new paths not recently traveled
//		else {
//			// join cols is not used
//			// we clean the names such that the headers that we want to join on will match
//			// and the logic inside the h2builder will figure that out...
//			// might not want that in the future, but oh well.. its in now
//			joinCols = updateJoinsCols(joinCols);
//			
//			// update the headers to reflect both the join column changes AND the logicalToValueMap
//			int newHeaderSize = newHeaders.length;
//			String[] modifiedNewHeaders = new String[newHeaderSize];
//			for(int i = 0; i < newHeaderSize; i++) {
//				String newHead = newHeaders[i];
//				if(modifyNamesMap.containsKey(newHead)) {
//					modifiedNewHeaders[i] = modifyNamesMap.get(newHead);
//				} else {
//					modifiedNewHeaders[i] = newHead;
//				}
//				
//			}
//			
//			frame.processIterator(dataIterator, startingHeaders, modifiedNewHeaders, joinType);
//		}
//		
//		// store the response string
//		inputResponseString(dataIterator, newHeaders);
//		// set status to success
//		myStore.put("STATUS", STATUS.SUCCESS);
		
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