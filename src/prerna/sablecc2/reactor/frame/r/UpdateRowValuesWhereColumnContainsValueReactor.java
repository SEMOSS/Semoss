package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Set;

import prerna.ds.r.RDataTable;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.QueryFilter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class UpdateRowValuesWhereColumnContainsValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values to a new value based on a filter condition
	 * (where a column equals a specified value)
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the new value
	 * 3) the filter condition
	 */

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		//get frame name
		String table = frame.getTableName();
		
		//get inputs
		String updateCol = getUpdateColumn();
		// separate the column name from the frame name
		if (updateCol.contains("__")) {
			updateCol = updateCol.split("__")[1];
		} 

		// second noun will be the value to update (the new value)
		String value = getNewValue();

		// the third noun will be a filter; we can get the qs from this
		QueryStruct2 qs = getQueryStruct();
		// get all of the filters from this querystruct
		GenRowFilters grf = qs.getFilters();
		Set<String> filteredColumns = grf.getAllFilteredColumns();
		for (String filColumn : filteredColumns) {
			List<QueryFilter> filterList = grf.getAllQueryFiltersContainingColumn(filColumn);
			for (QueryFilter queryFilter : filterList) {
				// col to values
				// the column name will be the left comparison value
				NounMetadata leftComp = queryFilter.getLComparison();
				String columnComp = leftComp.getValue() + "";
				// split if contains the table name in the column name
				if (columnComp.contains("__")) {
					columnComp = columnComp.split("__")[1];
					}
					// get the comparator from the queryFilter
					String nounComparator = queryFilter.getComparator();
					// clean nounComparator for rScript
					if (nounComparator.trim().equals("=")) {
						nounComparator = "==";
					} else if (nounComparator.equals("<>")) {
						nounComparator = "!=";
					}
					// the right comparison will be the comparison value
					NounMetadata rightComp = queryFilter.getRComparison();
					String valueComp = rightComp.getValue() + "";

					// account for quotes in the script where needed
					String comparisonColType = getColumnType(table, columnComp);
					if (comparisonColType.contains("character") || comparisonColType.contains("string") || comparisonColType.contains("factor")) {
						valueComp = "\"" + valueComp + "\"";
					}

					// get data type of column being updated
					String updateDataType = getColumnType(table, updateCol);

					// account for quotes in the script where needed
					if (updateDataType.contains("character") || updateDataType.contains("string") || updateDataType.contains("factor")) {
						value = "\"" + value + "\"";
					}

					// define the r script to be executed
					// script is of the form:
					// FRAME$Nominated[FRAME$Nominated=="Y"] <- "N"
					String script = table + "$" + updateCol + "[" + table + "$" + columnComp + nounComparator + valueComp + "] <- " + value;

					// execute the r script
					frame.executeRScript(script);
				}
			}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getUpdateColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to update
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullUpdateCol = noun1.getValue() + "";
			if (fullUpdateCol.length() == 0) {
				throw new IllegalArgumentException("Need to define column to update");
			}
			return fullUpdateCol;
		}
		throw new IllegalArgumentException("Need to define column to update");
	}
	
	private String getNewValue() {
		NounMetadata noun2 = this.getCurRow().getNoun(1);
		String value = noun2.getValue() + "";
		return value;
	}
	
	private QueryStruct2 getQueryStruct() {
		NounMetadata filterNoun = this.getCurRow().getNoun(2);
		// filter is query struct pksl type
		// the qs is the value of the filterNoun
		QueryStruct2 qs = (QueryStruct2) filterNoun.getValue();
		if (qs == null) {
			throw new IllegalArgumentException("Need to define filter condition");
		}
		return qs;
	}
}
