package prerna.sablecc2.reactor.frame.rdbms;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class SplitColumnReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		PreparedStatement ps = null;
		GenRowStruct inputsGRS = this.getCurRow();
		String delimiter = "";

		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			delimiter = inputsGRS.getNoun(0).getValue() + "";
			for(int i = 1; i < inputsGRS.size(); i++) {
				String column = inputsGRS.getNoun(i).getValue() + "";
				QueryStruct2 qs = new QueryStruct2();
				QueryColumnSelector selector = new QueryColumnSelector(column);
				qs.addSelector(selector);

				String table = frame.getTableName();
				if(column.contains("__")) {
					String[] split = column.split("__");
					column = split[1];
					table = split[0];
				}
				String colSplitBase = column+"_SPLIT_";
				Iterator<IHeadersDataRow> colIterator = frame.query(qs);

				int highestIndex = 0;
				List<String> addedColumns = new Vector<String>();
				
				// keep a batch size so we dont get heapspace
				final int batchSize = 5000;
				int count = 0;
				try {
					// iterate through the unique values
					while(colIterator.hasNext()) {
						// hold the existing value
						String nextVal = (String) colIterator.next().getRawValues()[0];
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
							for(int j = highestIndex; j < newVals.length; j++) {
								set.add(colSplitBase+j);
								addedColumns.add(colSplitBase+j);
							}
							newEdgeHash.put(column, set);
							// TODO: empty  HashMap will default types to string, need to also be able to create other type columns
							// in cases of splitting dates and decimals
							highestIndex = newVals.length;
							String[] columnTypes = new String[addedColumns.size()];
							String[] newColumns = new String[addedColumns.size()];
							for(int k = 0; k < columnTypes.length; k++) {
								newColumns[k] = addedColumns.get(k);
								columnTypes[k] = "STRING";
							}
							frame.addNewColumn(newColumns, columnTypes, frame.getTableName());
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
				frame.syncHeaders();
			}
		}
		
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
