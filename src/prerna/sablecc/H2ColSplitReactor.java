package prerna.sablecc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.H2Frame;

public class H2ColSplitReactor extends AbstractReactor {

	public H2ColSplitReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.WORD_OR_NUM}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_SPLIT;
	}

	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame) myStore.get("G");
		PreparedStatement ps = null;
		
		Vector<String> columns = (Vector<String>)myStore.get(PKQLEnum.COL_DEF);
		String column = columns.get(0);

		String colSplitBase = column+"_SPLIT_";
		String delimiter = (String) myStore.get(PKQLEnum.WORD_OR_NUM);

		Iterator<Object> colIterator = frame.uniqueValueIterator(column, false);

		int highestIndex = 0;
		List<String> addedColumns = new Vector<String>();
		
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;
					
		try {
			// iterate through the unique values
			while(colIterator.hasNext()) {
				// hold the existing value
				String nextVal = colIterator.next().toString();
				
				// hold the array for the complex split
				String[] newVals = nextVal.split(delimiter);
	
				// since we do not know how many possible new columns will be generated
				// we need to check each time if we need to create a new "column" if not already present
				if(newVals.length > highestIndex) {
					if(ps != null) {
						// since the update query now needs to change
						// flush all the current values in that were
						// not in the last batch
						ps.executeBatch();
					}
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
					
					ps = frame.createUpdatePreparedStatement(addedColumns.toArray(new String[]{}), new String[]{column});
				}
				
				int colIndex = 0;
				for(; colIndex < newVals.length; colIndex++) {
					ps.setString(colIndex+1, newVals[colIndex]);
				}
				// need to set empty values for the other columns
				// even if this split doesn't reach the end
				// otherwise the statement will error
				for(; colIndex < highestIndex; colIndex++) {
					ps.setString(colIndex+1, "");
				}
				
				// now set the where variable in the ps
				ps.setString(colIndex+1, nextVal); 
				
				// add the update into the batch
				ps.addBatch();
				
				// batch commit based on size
				if(++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			
			// do not forget to add the final things in the batch that have not been committed!
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// update the data id
		// so FE knows BE data has been modified
		frame.updateDataId();
		
		return null;
	}

}
