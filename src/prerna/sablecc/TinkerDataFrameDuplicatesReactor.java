package prerna.sablecc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLRunner.STATUS;

public class TinkerDataFrameDuplicatesReactor extends DataFrameDuplicatesReactor{

	@Override
	public Iterator process() {
		ITableDataFrame table = null;
		table = (ITableDataFrame) (ITableDataFrame) myStore.get("G");

		Vector<String> columns = (Vector) myStore.get(PKQLEnum.COL_CSV);
		String[] columnHeaders = table.getColumnHeaders();
		Map<String, Integer> columnMap = new HashMap<>();
		for (int i = 0; i < columnHeaders.length; i++) {
			columnMap.put(columnHeaders[i], i);
		}
		Iterator<Object[]> iterator = table.iterator();
		int numRows = table.getNumRows();
		Set<String> comboSet = new HashSet<String>(numRows);
		int rowCount = 1;
		boolean hasDuplicates = false;
		while (iterator.hasNext()) {
			Object[] nextRow = iterator.next();
			String comboValue = "";
			for (String c : columns) {
				int i = columnMap.get(c);
				comboValue = comboValue + nextRow[i];
			}
			comboSet.add(comboValue);

			if (comboSet.size() < rowCount) {
				hasDuplicates = true;
				break;
			}

			rowCount++;
		}
		
		myStore.put("hasDuplicates", hasDuplicates);
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}
}
