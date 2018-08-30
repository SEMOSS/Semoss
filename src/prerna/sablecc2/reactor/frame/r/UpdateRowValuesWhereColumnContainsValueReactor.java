package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class UpdateRowValuesWhereColumnContainsValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values to a new value based on a filter condition
	 * (where a column equals a specified value)
	 * The inputs to the reactor are: 
	 * 1) the column to update
	 * 2) the new value
	 * 3) the filter condition
	 */
	
	public UpdateRowValuesWhereColumnContainsValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String table = frame.getTableName();
		
		// create a string builder to keep track of the scripts to execute
		StringBuilder sb = new StringBuilder();
		
		// get inputs
		String updateCol = getUpdateColumn();
		// separate the column name from the frame name
		if (updateCol.contains("__")) {
			updateCol = updateCol.split("__")[1];
		} 

		// second noun will be the value to update (the new value)
		String value = getNewValue();
		
		// get data type of column being updated
		String updateDataType = getColumnType(table, updateCol);

		// account for quotes around the new value if needed
		if (updateDataType.contains("character") || updateDataType.contains("string")
				|| updateDataType.contains("factor")) {
			value = "\"" + value + "\"";
		}
		
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		///////////////////// QUERY FILTER//////////////////////////////////
		///////////////////////////////////////////////////////////////////
		
		// the third noun will be a filter; we can get the qs from this
		SelectQueryStruct qs = getQueryStruct();
		// get all of the filters from this querystruct
		GenRowFilters grf = qs.getExplicitFilters();
		Set<String> filteredColumns = grf.getAllFilteredColumns();
		// create a string builder to keep track of our r scripts so that we
		// only have to execute them once

		for (String filColumn : filteredColumns) {
			List<SimpleQueryFilter> filterList = grf.getAllSimpleQueryFiltersContainingColumn(filColumn);
			for (SimpleQueryFilter queryFilter : filterList) {
				// col to values
				// the column name will be the left comparison value
				NounMetadata leftComp = queryFilter.getLComparison();
				String columnComp = ((QueryColumnSelector) leftComp.getValue()).getQueryStructName();
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
				
				////////////////////////////////////////////////////////////////////
				////////////////////////////////////////////////////////////////////
				/////////////////////QUERY FILTER RIGHT COMPARISON/////////////////
				///////////////////////////////////////////////////////////////////
			
				// the right comparison will be the comparison value
				NounMetadata rightComp = queryFilter.getRComparison();

				// account for quotes in the script where needed
				String comparisonColType = getColumnType(table, columnComp);		

				// now deal with the values that are being updated; there can be more than one (e.g., Genre == "Albert", "Alex", "Amy")
				// this will give us the right side object - which may be a vector
				Object rightCompValue = rightComp.getValue();

				if (rightCompValue instanceof Vector) {
					Vector rightVector = (Vector) rightCompValue;
					// iterate through to get each value on the right side of the filter
					for (int i = 0; i < rightVector.size(); i++) {
						String valueComp = rightVector.get(i) + "";
						// account for quotes needed for the r script
						if (comparisonColType.contains("character") || comparisonColType.contains("string") || comparisonColType.contains("factor")) {
							valueComp = "\"" + valueComp + "\"";
						}
						// we will make multiple scripts - once each time we
						// loop through; and we will append all of these to the
						// string builder to be executed at once
						String script = table + "$" + updateCol + "[" + table + "$" + columnComp + nounComparator + valueComp + "] <- " + value + ";";
						sb.append(script);
					}

				} else {
					String valueComp = rightComp.getValue() + "";
					if (comparisonColType.contains("character") || comparisonColType.contains("string") || comparisonColType.contains("factor")) {
						valueComp = "\"" + valueComp + "\"";
					}
					// define the r script to be executed
					// script is of the form:
					// FRAME$Nominated[FRAME$Nominated=="Y"] <- "N"
					String script = table + "$" + updateCol + "[" + table + "$" + columnComp + nounComparator
							+ valueComp + "] <- " + value + ";";
					// append the single script to the string builder
					sb.append(script);
				}
			}
		}

		//execute the r scripts
		if (sb.length() > 0) {
			this.rJavaTranslator.runR(sb.toString());
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"UpdateRowValues", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
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
	
	private SelectQueryStruct getQueryStruct() {
		NounMetadata filterNoun = this.getCurRow().getNoun(2);
		// filter is query struct pksl type
		// the qs is the value of the filterNoun
		SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
		if (qs == null) {
			throw new IllegalArgumentException("Need to define filter condition");
		}
		return qs;
	}
}
