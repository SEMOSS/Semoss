package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Set;

import prerna.ds.r.RDataTable;
import prerna.query.querystruct.GenRowFilters;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.QueryFilter;

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
		//initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			String updateCol = ""; 
			//first noun will be the column to update
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullUpdateCol = noun1.getValue() + ""; 
			//separate the column name from the frame name
			if (fullUpdateCol.contains("__")) {
				updateCol = fullUpdateCol.split("__")[1];
			} else {
				updateCol = fullUpdateCol; 
			}

			//second noun will be the value to update (the new value)
			NounMetadata noun2 = inputsGRS.getNoun(1); 
			String value = noun2.getValue() + "";

			//the third noun will be a filter
			NounMetadata filterNoun = inputsGRS.getNoun(2);
			PixelDataType filterNounType = filterNoun.getNounType();
			//filter is query struct pksl type
			if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
				//the qs is the value of the filterNoun
				QueryStruct2 qs = (QueryStruct2) filterNoun.getValue(); 
				//get all of the filters from this querystruct
				GenRowFilters grf = qs.getFilters();
				Set<String> filteredColumns = grf.getAllFilteredColumns();
				for (String filColumn : filteredColumns) {
					List<QueryFilter> filterList = grf.getAllQueryFiltersContainingColumn(filColumn);
					for (QueryFilter queryFilter : filterList) {
						String columnComp = "";
						//col to values
						//the column name will be the left comparison value
						NounMetadata leftComp = queryFilter.getLComparison();
						String fullColumnComp = leftComp.getValue() + "";
						//split if contains the table name in the column name	
						if (fullColumnComp.contains("__")) {
							String[] split = fullColumnComp.split("__");
							table = split[0];
							columnComp = split[1]; 
						}
						//get the comparator from the queryFilter
						String nounComparator = queryFilter.getComparator(); 
						//clean nounComparator for rScript
						if (nounComparator.trim().equals("=")) {
							nounComparator = "==";
						} else if (nounComparator.equals("<>")) {
							nounComparator = "!=";
						}
						//the right comparison will be the comparison value		
						NounMetadata rightComp = queryFilter.getRComparison();
						String valueComp = rightComp.getValue() + "";

						//get data type of column being updated
						String updateDataType = getColumnType(table, updateCol);
						String updateQuote = "";
						if (updateDataType.contains("character") || updateDataType.contains("string") || updateDataType.contains("factor")) {
							updateQuote = "\"";
						}

						//account for quotes in the script where needed
						String conditionColQuote = "";
						if (updateDataType.contains("character") || updateDataType.contains("string")) {
							conditionColQuote = "\"";
						}

						//define the r script to be executed
						//script is of the form: FRAME$Nominated[FRAME$Nominated=="Y"] <- "N"
						String script = table + "$" + updateCol + "[" + table + "$" + columnComp + nounComparator
								+ conditionColQuote + valueComp + conditionColQuote + "] <- " + updateQuote
								+ value + updateQuote;

						//execute the r script
						frame.executeRScript(script);
					}
				}	
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
