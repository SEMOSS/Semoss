package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.util.Utility;

public class CsvTableWrapper implements Iterator{

	private List<Vector<Object>> values;
	private String[] headers;
	private int currIndex = 0;
	
	public CsvTableWrapper(List<Vector<Object>> values) {
		this.values = values;
		this.headers = values.get(0).toArray(new String[]{});
		currIndex = 1;
	}
	
	@Override
	public boolean hasNext() {
		if(values.size() > currIndex) {
			return true;
		}
		return false;
	}

	@Override
	public IHeadersDataRow next() {
		Object[] currRow = values.get(currIndex).toArray();
		Object[] cleanRow = new Object[currRow.length];
		for(int i = 0; i < currRow.length; i++) {
			String val = currRow[i] + "";
			String type = Utility.findTypes(val)[0] + "";
			if(type.equalsIgnoreCase("Date")) {
				cleanRow[i] = Utility.getDate(val);
			} else if(type.equalsIgnoreCase("Double")) {
				cleanRow[i] = Utility.getDouble(val);
			} else {
				cleanRow[i] = Utility.cleanString(val, true, true, false);
			}
		}
		IHeadersDataRow retObj = new HeadersDataRow(this.headers, cleanRow, currRow);
		currIndex++;
		return retObj;
	}
}
