package prerna.ds.r;

import java.util.Iterator;
import java.util.Map;

public class RIterator implements Iterator<Object[]>{

	private AbstractRBuilder builder;
	
	// keep track of the frame name
	private String tableName = "datatable";
	
	// keep track of current position in the data table
	// note, R indices start at 1, not 0
	private int rowIndex = 1;
	
	// keep track of the size of the data table
	private int dataTableSize = 1;
	
	// R returns the vector as a map
	// need to keep the headers so we know the order to return
	private String[] headers;
	private String headerString;
	
	public RIterator(AbstractRBuilder builder, String[] headers, int limit, int offset) {
		this.builder = builder;
		this.tableName = builder.getTableName();
		this.headers = headers;
		this.headerString = RSyntaxHelper.createStringRColVec(headers);
		this.dataTableSize = this.builder.getNumRows();
		
		
		// we communicate with FE as base 0
		this.rowIndex = offset + 1;
		// adjust the size based on the limit and offset
		if(limit > 0 && this.dataTableSize > (limit + offset) ) {
			this.dataTableSize = (limit + offset);
		}
	}
	
	@Override
	public boolean hasNext() {
		if(rowIndex < dataTableSize) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Object[] next() {
		// grab the rowIndex from the data table
		Map<String, Object> data = this.builder.getMapReturn(this.tableName + "[" + rowIndex + " , " + headerString + ", with=FALSE]");
		
		// iterate through the list and fill into an object array to return
		Object[] retArray = new Object[data.keySet().size()];
		for(int colIndex = 0; colIndex < headers.length; colIndex++) {
			Object val = data.get(headers[colIndex]);
			if(val instanceof Object[]) {
				retArray[colIndex] = ((Object[]) val)[0];
			} else if(val instanceof double[]) {
				retArray[colIndex] = ((double[]) val)[0];
			} else if( val instanceof int[]) {
				retArray[colIndex] = ((int[]) val)[0];
			} else {
				retArray[colIndex] = val;
			}
			
			// since FE cannot handle NaN values
			if(retArray[colIndex].equals(Double.NaN)) {
				retArray[colIndex] = "";
			}
		}
		
		// update the row index
		this.rowIndex++;

		return retArray;
	}
	
	
	
}
