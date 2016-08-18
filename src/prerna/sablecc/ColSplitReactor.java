package prerna.sablecc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;

public class ColSplitReactor extends AbstractReactor {

ITableDataFrame frame;
	
	public ColSplitReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.WORD_OR_NUM}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_SPLIT;
	}
	
	@Override
	public Iterator process() {
		
		ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
		frame.getEdgeHash();
		
		List<String> columns = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		String column = columns.get(0);
		
		String colSplitBase = column+"_SPLIT_";
		List<String> delimiters = (List<String>)myStore.get(PKQLEnum.WORD_OR_NUM);
		String delimiter = delimiters.get(0);
		
		Iterator<Object> colIterator = frame.uniqueValueIterator(column, false, false);
		
		int highestIndex = 0;
		//first update table
		while(colIterator.hasNext()) {
			
			String nextVal = colIterator.next().toString();
			String[] newVals = nextVal.split(delimiter);
			
			Map<String, Object> origMap = new LinkedHashMap<>();
			Map<String, Object> newMap = new LinkedHashMap<>();
			origMap.put(column, nextVal);
			newMap.put(column, nextVal);
			
			if(newVals.length > highestIndex) {
				Map<String, Set<String>> newEdgeHash = new LinkedHashMap<>();
				Set<String> set = new LinkedHashSet<>();
				for(int i = highestIndex; i < newVals.length; i++) {
					
					set.add(colSplitBase+i);
				}
				newEdgeHash.put(column, set);
				//TODO: empty  hashmap will default types to string, need to also be able to create other type columns
				//		in cases of splitting dates and decimals
				frame.mergeEdgeHash(newEdgeHash, new HashMap<>());
				highestIndex = newVals.length;
			}
			
			
			for(int i = 0; i < newVals.length; i++) {
				newMap.put(colSplitBase+i, newVals[i]);
			}
			
			frame.addRelationship(newMap, origMap);	//cleanRow, rawRow		
		}	
		//then update meta data
		
		//remove column
//		frame.removeColumn(column);
		frame.updateDataId();
		return null;
	}
}
