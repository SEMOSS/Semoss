package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import prerna.ds.h2.H2Frame;
import prerna.poi.main.HeadersException;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class UpdateRowValuesReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		String updateColumnInput = "";
		String updateTable = "";
		String updateColumn = "";
		String updateValueSQL = "";
		GenRowStruct inputsGRS = this.getCurRow();
		String sqlStatements = "";
		// get update column
		updateColumnInput = inputsGRS.getNoun(0).getValue() + "";
		if (updateColumnInput.contains("__")) {
			String[] split = updateColumnInput.split("__");
			updateTable = split[0];
			updateColumn = split[1];
		} else {
			updateTable = frame.getName();
			updateColumn = updateColumnInput;
		}
		// get update value
		NounMetadata noun = inputsGRS.getNoun(1);
		PixelDataType nounType = noun.getNounType();
		if (nounType.equals(PixelDataType.CONST_STRING)) {
			updateValueSQL = noun.getValue() + "";
		}

		// validate updateColumn exists
		String[] existCols = getColNames(updateTable);
		if (Arrays.asList(existCols).contains(updateColumn) != true) {
			throw new IllegalArgumentException("Column " + updateColumn + " doesn't exist.");
		}

		// clean updateValueSQL value
		HeadersException colNameChecker = HeadersException.getInstance();
		updateValueSQL = "'" + colNameChecker.removeIllegalCharacters(updateValueSQL) + "'";

		// get filters
		String update = "UPDATE " + updateTable + " SET " + updateColumn + " = " + updateValueSQL + " WHERE ";
		NounMetadata filterNoun = inputsGRS.getNoun(2);
		PixelDataType filterNounType = filterNoun.getNounType();
		if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			GenRowFilters grf = qs.getExplicitFilters();
			Set<String> filteredColumns = grf.getAllFilteredColumns();
			for (String column : filteredColumns) {
				List<SimpleQueryFilter> filterList = grf.getAllSimpleQueryFiltersContainingColumn(column);
				for (SimpleQueryFilter queryFilter : filterList) {
					String sqlCondition = "";
					// col to values
					NounMetadata leftComp = queryFilter.getLComparison();
					String columnComp = leftComp.getValue() + "";
					if (columnComp.contains("__")) {
						String[] split = columnComp.split("__");
						sqlCondition += split[1];
					}
					// does sqlCondition exist
					if (Arrays.asList(existCols).contains(sqlCondition) != true) {
						throw new IllegalArgumentException("Column " + sqlCondition + " doesn't exist.");
					}
					String nounComparator = queryFilter.getComparator();
					// clean nounComparator for sql statement
					if (nounComparator.equals("==")) {
						nounComparator = "=";
					} else if (nounComparator.equals("<>")) {
						nounComparator = "!=";
					}
					sqlCondition += " " + nounComparator + " ";
					// rightComp has values
					NounMetadata rightComp = queryFilter.getRComparison();
					Object value = rightComp.getValue();

					// clean value
					value = colNameChecker.removeIllegalCharacters(String.valueOf(value));

					// if it is a string put quotes
					if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
						sqlStatements += update + sqlCondition + "'" + value + "' ; ";
					} else {
						sqlStatements += update + sqlCondition + "'" + value + "' ; ";
					}
				}
			}
		}
		try {
			frame.getBuilder().runQuery(sqlStatements);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
