package prerna.sablecc2.reactor.frame.rdbms;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import prerna.ds.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class SplitColumnReactor extends AbstractFrameReactor {

	private static final String COLUMNS_KEY = "columns";
	private static final String SEPARATOR_KEY = "sep";
	private static final String SEARCH_TYPE = "search";
	
	private static final String REGEX = "Regex";
	
	@Override
	public NounMetadata execute() {
		List<String> cols = getColumns();
		String separator = getSeparator();
		boolean isRegex = isRegex();
		if(!isRegex) {
			separator = Pattern.quote(separator);
		}
		H2Frame frame = (H2Frame) getFrame();
		PreparedStatement ps = null;

		for(int i = 1; i < cols.size(); i++) {
			String column = cols.get(i);
			SelectQueryStruct qs = new SelectQueryStruct();
			QueryColumnSelector selector = new QueryColumnSelector(column);
			qs.addSelector(selector);

			String table = frame.getName();
			if(column.contains("__")) {
				String[] split = column.split("__");
				column = split[1];
				table = split[0];
			}
			String colSplitBase = column + "_SPLIT_";
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
					String[] newVals = nextVal.split(separator);

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
						frame.addNewColumn(newColumns, columnTypes, frame.getName());
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

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}


	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getSeparator() {
		GenRowStruct separatorGrs = this.store.getNoun(SEPARATOR_KEY);
		if(separatorGrs == null || separatorGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to define a separator to split the column with");
		}
		String separator = separatorGrs.get(0).toString();
		if(separator.isEmpty()) {
			throw new IllegalArgumentException("Need to define a separator to split the column with");
		}
		return separator;
	}

	private boolean isRegex() {
		GenRowStruct regexGrs = this.store.getNoun(SEARCH_TYPE);
		if(regexGrs == null || regexGrs.isEmpty()) {
			return true;
		}
		String val = regexGrs.get(0).toString();
		if(val.equalsIgnoreCase(REGEX)) {
			return true;
		}
		return false;
	}

	private List<String> getColumns() {
		List<String> cols = new ArrayList<String>();

		// try its own key
		GenRowStruct colsGrs = this.store.getNoun(COLUMNS_KEY);
		if(colsGrs != null && !colsGrs.isEmpty()) {
			int size = colsGrs.size();
			for(int i = 0; i < size; i++) {
				cols.add(colsGrs.get(i).toString());
			}
			return cols;
		}

		int inputSize = this.getCurRow().size();
		if(inputSize > 0) {
			for(int i = 0; i < inputSize; i++) {
				cols.add(this.getCurRow().get(i).toString());
			}
			return cols;
		}

		throw new IllegalArgumentException("Need to define the columns to split");
	}

}
