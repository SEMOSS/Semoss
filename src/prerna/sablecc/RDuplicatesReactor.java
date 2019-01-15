package prerna.sablecc;

import java.util.Iterator;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.sablecc.PKQLRunner.STATUS;

public class RDuplicatesReactor extends DataFrameDuplicatesReactor {

	@Override
	public Iterator process() {
		// This reactor checks for duplicates
		boolean hasDuplicates = false;
		RDataTable dataframe = (RDataTable) myStore.get("G");
		String rVarName = dataframe.getName();
		Vector<String> columns = (Vector) myStore.get(PKQLEnum.COL_CSV);

		StringBuilder columnSelectBuilder = new StringBuilder("c(");
		int numColumns = columns.size();
		for(int i = 0; i < numColumns; i++) {
			if( (i+1)  == numColumns) {
				columnSelectBuilder.append("\"").append(columns.get(i)).append("\"");
			} else {
				columnSelectBuilder.append("\"").append(columns.get(i)).append("\", ");
			}
		}
		columnSelectBuilder.append(")");
		String colSelect = columnSelectBuilder.toString();
		
		int val1 = dataframe.getNumRows(rVarName + "[ ," + colSelect + "]");
		int val2 = dataframe.getNumRows("unique( " + rVarName + "[ ," + colSelect + "] )");

		hasDuplicates = val1 != val2;

		myStore.put("hasDuplicates", hasDuplicates);
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}
}
