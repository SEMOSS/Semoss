package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.TinkerFrame;

public class TinkerColSplitReactor extends AbstractReactor {

	public TinkerColSplitReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.WORD_OR_NUM}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_SPLIT;
	}

	@Override
	public Iterator process() {
		TinkerFrame frame = (TinkerFrame)myStore.get("G");

		Vector<String> columns = (Vector<String>)myStore.get(PKQLEnum.COL_DEF);
		String column = columns.get(0);

		String colSplitBase = column+"_SPLIT_";
		String delimiter = (String) myStore.get(PKQLEnum.WORD_OR_NUM);

		Iterator<Object> colIterator = frame.uniqueValueIterator(column, false);

		int highestIndex = 0;
		List<String> addedColumns = new Vector<String>();
		// iterate through the unique values
		while(colIterator.hasNext()) {
			String nextVal = colIterator.next().toString();
			String[] newVals = nextVal.split(delimiter);

			Map<String, Object> newMap = new LinkedHashMap<>();
			newMap.put(column, nextVal);

			// since we do not know how many possible new columns will be generated
			// we need to check each time if we need to create a new "column" if not already present
			if(newVals.length > highestIndex) {
				Map<String, Set<String>> newEdgeHash = new LinkedHashMap<>();
				Set<String> set = new LinkedHashSet<>();
				for(int i = highestIndex; i < newVals.length; i++) {
					set.add(colSplitBase+i);
					addedColumns.add(colSplitBase+i);
				}
				newEdgeHash.put(column, set);
				// TODO: empty  HashMap will default types to string, need to also be able to create other type columns
				// in cases of splitting dates and decimals
				frame.mergeEdgeHash(newEdgeHash, new HashMap<>());
				highestIndex = newVals.length;
			}

			for(int i = 0; i < newVals.length; i++) {
				newMap.put(colSplitBase+i, newVals[i]);
			}

			frame.addRelationship(newMap);	//cleanRow, rawRow		
		}
		
		// since we need to keep the table structure present
		// we need to loop through the data again such that values that do not have
		// the necessary "empty" nodes are complete with respect to having the 
		// maximum number of columns
		frame.insertBlanks(column, addedColumns);
		
		// update the data id
		// so FE knows BE data has been modified
		frame.updateDataId();
		
		return null;
	}
}
