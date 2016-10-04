package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.util.Utility;

public class CsvTableWrapper implements Iterator<IHeadersDataRow>{

	private List<Vector<Object>> values;
	private String[] headers;
	private String[] types;
	private int currIndex = 0;

	public CsvTableWrapper(List<Vector<Object>> values) {
		this.values = values;
		this.headers = values.get(0).toArray(new String[]{});
		currIndex = 1;
	}

	public String[] getTypes() {
		if(types != null) {
			return types;
		}
		
		types = new String[headers.length];
		int size = values.size();
		int counter = 0;
		for(int i = 0; i < headers.length; i++) {
			String type = null;
			WHILE_FOR : for(int j =0; j < size; j++) {
				String val = values.get(j).get(i) + "";
				if(val.isEmpty()) {
					continue;
				}
				String newTypePred = (Utility.findTypes(val)[0] + "").toUpperCase();
				if(newTypePred.contains("VARCHAR")) {
					type = newTypePred;
					break WHILE_FOR;
				}

				// need to also add the type null check for the first row
				if(!newTypePred.equals(type) && type != null) {
					// this means there are multiple types in one column
					// assume it is a string 
					if( (type.equals("INT") || type.equals("DOUBLE")) && (newTypePred.equals("INT") || 
							newTypePred.equals("INT") || newTypePred.equals("DOUBLE") ) ){
						// for simplicity, make it a double and call it a day
						// TODO: see if we want to impl the logic to choose the greater of the newest
						// this would require more checks though
						type = "DOUBLE";
					} else {
						// should only enter here when there are numbers and dates
						// TODO: need to figure out what to handle this case
						// for now, making assumption to put it as a string
						type = "VARCHAR(800)";
						break WHILE_FOR;
					}
				} else {
					// type is the same as the new predicated type
					// or type is null on first iteration
					type = newTypePred;
				}
			}
			types[counter] = type;
			counter++;
		}

		return types;
	}

	public String[] getHeaders() {
		return this.headers;
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
	
	public int getNumRecords() {
		return this.values.size();
	}
}
