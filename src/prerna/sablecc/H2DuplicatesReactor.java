package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.PKQLRunner.STATUS;

public class H2DuplicatesReactor extends DataFrameDuplicatesReactor {

	@Override
	public Iterator process() {
		// This reactor checks for duplicates
		boolean hasDuplicates = false;
		H2Frame dataframe = (H2Frame) myStore.get("G");
		String tableName = dataframe.getTableName();
		Vector<String> columns = (Vector) myStore.get(PKQLEnum.COL_CSV);

		String query = "SELECT COUNT(*) FROM (SELECT ";
		// Append columns to query
		for (String c : columns) {
			query += c + ", ";
		}
		// remove extra characters
		query = query.substring(0, query.length() - 2);
		query += " FROM " + tableName + ") AS CountDuplicates;";

		// results from query
		List<Object[]> queryTable = dataframe.getFlatTableFromQuery(query);

		String distinctQuery = "SELECT COUNT(*) FROM (SELECT DISTINCT ";
		
		//concat for multiple columns
		if (columns.size() > 1) {
			distinctQuery += "CONCAT(";
			// Append columns to query
			for (String c : columns) {
				distinctQuery += c + ", ";
			}
			// remove extra characters
			distinctQuery = distinctQuery.substring(0, distinctQuery.length() - 2);
			distinctQuery += ")";
		}
		else{
			distinctQuery += columns.get(0);
		}
		distinctQuery += " FROM " + tableName + ") AS CountDuplicates;";

		List<Object[]> queryTable2 = dataframe.getFlatTableFromQuery(distinctQuery);

		// check if values are the same if so no duplicates
		Long val1 = (Long) queryTable.get(0)[0];
		Long val2 = (Long) queryTable2.get(0)[0];
		hasDuplicates = (long) val1 != (long) val2;

		myStore.put("hasDuplicates", hasDuplicates);
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}
}
