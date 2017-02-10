package prerna.ds.r;

import java.util.Iterator;

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
			this.dataTableSize = (limit + offset) + 1;
		}
	}
	
	@Override
	public boolean hasNext() {
		if(rowIndex <= dataTableSize) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Object[] next() {
		// grab the rowIndex from the data table
		// the ordering is only used by the RServe version of R
		// JRI returns the array in the ordering of the headerString
		// RServe returns a map which isn't necessarily ordered
		Object[] retArray = this.builder.getDataRow(this.tableName + "[" + rowIndex + " , " + headerString + ", with=FALSE]", headers);
		// update the row index
		this.rowIndex++;
				
		return retArray;
	}
	
}
