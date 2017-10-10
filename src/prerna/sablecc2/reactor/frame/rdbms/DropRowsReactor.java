package prerna.sablecc2.reactor.frame.rdbms;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import prerna.ds.h2.H2Frame;
import prerna.query.querystruct.GenRowFilters;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.QueryFilter;
import prerna.sablecc2.om.QueryFilter.FILTER_TYPE;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class DropRowsReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		H2Frame frame = (H2Frame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		String sqlStatements = "";
		NounMetadata filterNoun = inputsGRS.getNoun(0);
		PixelDataType filterNounType = filterNoun.getNounType();
		if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
			QueryStruct2 qs = (QueryStruct2) filterNoun.getValue();
			GenRowFilters grf = qs.getFilters();
			Set<String> filteredColumns = grf.getAllFilteredColumns();
			for (String filColumn : filteredColumns) {
				List<QueryFilter> filterList = grf.getAllQueryFiltersContainingColumn(filColumn);
				for (QueryFilter queryFilter : filterList) {
					String table = "";
					String column = "";
					FILTER_TYPE type = QueryFilter.determineFilterType(queryFilter);
					// col to values
					NounMetadata leftComp = queryFilter.getLComparison();
					String columnComp = leftComp.getValue() + "";
					if (columnComp.contains("__")) {
						String[] split = columnComp.split("__");
						table = split[0];
						column = split[1];
					}
					String nounComparator = queryFilter.getComparator();
					// clean nounComparator for sql statement
					if (nounComparator.equals("==")) {
						nounComparator = "=";
					} else if (nounComparator.equals("<>")) {
						nounComparator = "!=";
					}
					NounMetadata rightComp = queryFilter.getRComparison();
					Object value = rightComp.getValue();

					//escape single quote for sql
					if (String.valueOf(value).contains("'")){
						value = String.valueOf(value).replaceAll("'", "''");
					}
					
					// check the column exists, if not then throw warning
					String[] allCol = getColNames(table);
					if (Arrays.asList(allCol).contains(column) != true) {
						throw new IllegalArgumentException("Column doesn't exist.");
					}
					
					//put quotes if string
					if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
						sqlStatements += "DELETE FROM " + table + " WHERE " + column + " " + nounComparator + " '" + value + "'; ";
					}
					else {
						sqlStatements += "DELETE FROM " + table + " WHERE " + column + " " + nounComparator + " " + value + "; ";
					}
				}
			}
		}
		if (frame != null) {
			try {
				frame.getBuilder().runQuery(sqlStatements);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
