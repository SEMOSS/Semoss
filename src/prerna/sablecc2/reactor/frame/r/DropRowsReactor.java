package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import prerna.ds.r.RDataTable;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class DropRowsReactor extends AbstractRFrameReactor {

	/**
	 * This reactor drops rows based on a comparison The inputs to the reactor
	 * are: 1) the filter comparison for dropping rows
	 */

	public DropRowsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator - we will need this for method to get the data type
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		// we will use a filter to determine which rows to drop
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// use a stringbuilder to keep track of the growing script
			StringBuilder script = new StringBuilder(table).append(" <- ").append(table).append("[!( ");
			NounMetadata filterNoun = inputsGRS.getNoun(0);
			PixelDataType filterNounType = filterNoun.getNounType();
			if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
				// the first noun will be a query struct - the filter
				SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
				// get the filters from the query struct 
				// and iterate through each filtered column
				GenRowFilters grf = qs.getExplicitFilters();
				Set<String> filteredColumns = grf.getAllFilteredColumns();
				for (String filColumn : filteredColumns) {
					List<SimpleQueryFilter> filterList = grf.getAllSimpleQueryFiltersContainingColumn(filColumn);
					for (SimpleQueryFilter queryFilter : filterList) {
						// col to values the left comparison will be the column
						NounMetadata leftComp = queryFilter.getLComparison();
						String column = ((IQuerySelector) leftComp.getValue()).getQueryStructName();
						// separate the column name from the frame name
						if (column.contains("__")) {
							String[] split = column.split("__");
							table = split[0];
							column = split[1];
						}
						String nounComparator = queryFilter.getComparator();
						//Validate column exists
						String[] existCols = getColNames(table);
						if (Arrays.asList(existCols).contains(column) != true) {
							throw new IllegalArgumentException("Column doesn't exist.");
						}						
						// clean nounComparator for rScript
						if (nounComparator.trim().equals("=")) {
							nounComparator = "==";
						} else if (nounComparator.equals("<>")) {
							nounComparator = "!=";
						} else if (nounComparator.equals("?like")) {
							nounComparator = "like";
						}
						// the right comparison will be the comparison value
						NounMetadata rightComp = queryFilter.getRComparison();
						Object value = rightComp.getValue();
						String frameExpression = table + "$" + column;
						String dataType = getColumnType(table, column);

						// take into account different possibilities for the comparison value
						// if an object
						if (value instanceof Object[]) {
							Object[] arr = (Object[]) value;
							Object val = arr[0];
							if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("character")) {
								if (val.toString().equalsIgnoreCase("NULL") || val.toString().equalsIgnoreCase("NA")) {
									script.append("is.na(").append(frameExpression).append(") ");
								} else {
									if (nounComparator.equals("like")) {
										script.append("like(").append(frameExpression).append(",").append("\"").append(val).append("\")");
									} else {
										script.append(frameExpression).append(nounComparator).append("\"").append(val).append("\"");
									}
								}
							} else {
								//value is instance of object but the data type is not a string - we dont need quotes
								script.append(nounComparator).append(val);
							}
							for (int i = 1; i < arr.length; i++) {
								val = arr[i];
								if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("character")) {
									if (val.toString().equalsIgnoreCase("NULL") || val.toString().equalsIgnoreCase("NA")) {
										script.append(" | is.na(").append(frameExpression).append(") ");
									} else {
										if (nounComparator.equals("like")) {
											script.append(" | ").append("like(").append(frameExpression).append(",").append("\"").append(val).append("\")");
										} else {
											script.append(" | ").append(frameExpression).append(nounComparator).append("\"").append(val).append("\"");
										}
									}
								} else {
									script.append(" | ").append(frameExpression).append(nounComparator).append(val);
								}
							}
						} else if (value instanceof Double[]) {
							Double[] arr = (Double[]) value;
							Double val = arr[0];
							script.append(frameExpression).append(nounComparator).append(val);
							for (int i = 1; i < arr.length; i++) {
								val = arr[i];
								script.append(" | ").append(frameExpression).append(nounComparator).append(val);
							}
						} else if (value instanceof Integer[]) {
							Integer[] arr = (Integer[]) value;
							Integer val = arr[0];
							script.append(frameExpression).append(nounComparator).append(val);
							for (int i = 1; i < arr.length; i++) {
								val = arr[i];
								script.append(" | ").append(frameExpression).append(nounComparator).append(val);
							}
						} else if (value instanceof double[]) {
							double[] arr = (double[]) value;
							double val = arr[0];
							script.append(frameExpression).append(nounComparator).append(val);
							for (int i = 1; i < arr.length; i++) {
								val = arr[i];
								script.append(" | ").append(frameExpression).append(nounComparator).append(val);
							}
						} else if (value instanceof int[]) {
							int[] arr = (int[]) value;
							int val = arr[0];
							script.append(frameExpression).append(nounComparator).append(val);
							for (int i = 1; i < arr.length; i++) {
								val = arr[i];
								script.append(" | ").append(frameExpression).append(nounComparator).append(val);
							}
						} else {
							if (dataType.equalsIgnoreCase("character") || dataType.equalsIgnoreCase("string")) {
								if (value.toString().equalsIgnoreCase("NULL") || value.toString().equalsIgnoreCase("NA")) {
									script.append("is.na(").append(frameExpression).append(") ");
								} else {
									if (nounComparator.equals("like")) {
										script.append("like(").append(frameExpression).append(",").append("\"").append(value).append("\")");
									} else {
										script.append(frameExpression).append(nounComparator).append("\"").append(value).append("\"");
									}
								}
							} else {
								script.append(frameExpression).append(nounComparator).append(value);
							}
						}
					}
				}
			}
			script.append("),]");
			// execute the r script
			// script is of the form - FRAME <- FRAME[!( FRAME$Director == "value"),]
			frame.executeRScript(script.toString());
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"DropRows", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
