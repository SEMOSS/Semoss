package prerna.reactor.frame.rdbms;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DropRowsReactor extends AbstractFrameReactor {

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		String sqlStatements = "";
		NounMetadata filterNoun = inputsGRS.getNoun(0);
		PixelDataType filterNounType = filterNoun.getNounType();
		if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			GenRowFilters grf = qs.getExplicitFilters();
			Set<String> filteredColumns = grf.getAllFilteredColumns();
			for (String filColumn : filteredColumns) {
				List<SimpleQueryFilter> filterList = grf.getAllSimpleQueryFiltersContainingColumn(filColumn);
				for (SimpleQueryFilter queryFilter : filterList) {
					String table = "";
					String column = "";
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

					// escape single quote for sql
					if (String.valueOf(value).contains("'")) {
						value = String.valueOf(value).replaceAll("'", "''");
					}

					// check the column exists, if not then throw warning
					String[] allCol = getColNames(frame);
					if (Arrays.asList(allCol).contains(column) != true) {
						throw new IllegalArgumentException("Column doesn't exist.");
					}

					// put quotes if string
					if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
						sqlStatements += "DELETE FROM " + table + " WHERE " + column + " " + nounComparator + " '" + value + "'; ";
					} else {
						sqlStatements += "DELETE FROM " + table + " WHERE " + column + " " + nounComparator + " " + value + "; ";
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
