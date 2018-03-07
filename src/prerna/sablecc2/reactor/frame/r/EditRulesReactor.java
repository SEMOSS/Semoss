package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EditRulesReactor  extends AbstractRFrameReactor {
	public EditRulesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		RDataTable frame = (RDataTable) getFrame();
		String dfName = frame.getTableName();
		String[] packages = new String[] {"lpSolveAPI", "igraph", "editRules"};
		NounMetadata filterNoun = this.getCurRow().getNoun(0);
		// filter is query struct pksl type
		// the qs is the value of the filterNoun
		QueryStruct2 qs = (QueryStruct2) filterNoun.getValue();
		if (qs == null) {
			throw new IllegalArgumentException("Need to define filter condition");
		}
		
		// write edit rules file
		String editRulesFilePath = getBaseFolder() + "\\R\\EditRules\\editRules.txt";
		try {
			PrintWriter pw = new PrintWriter(new File(editRulesFilePath));
			StringBuilder sb = new StringBuilder();
			String rule = "";
			
			GenRowFilters grf = qs.getExplicitFilters();
			Set<String> filteredColumns = grf.getAllFilteredColumns();
			List<IQueryFilter> filterList2 = grf.getFilters();
			String sqlStatements = "";
			for (String column : filteredColumns) {
				String update = "";
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
//					if (Arrays.asList(null).contains(sqlCondition) != true) {
//						throw new IllegalArgumentException("Column " + sqlCondition + " doesn't exist.");
//					}
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


					// if it is a string put quotes
					if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
						sqlStatements += update + sqlCondition + "'" + value + "' ; ";
					} else {
						sqlStatements += update + sqlCondition + "'" + value + "' ; ";
					}
				}
			}
			sb.append(rule);
			pw.write(sb.toString());
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.CODE_EXECUTION);
	}

}
