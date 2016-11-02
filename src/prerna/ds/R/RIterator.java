package prerna.ds.R;

import java.util.Iterator;
import java.util.Map;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;

public class RIterator implements Iterator<Object[]>{

	private RBuilder builder;
	
	// keep track of current position in the data table
	// note, R indices start at 1, not 0
	private int rowIndex = 1;
	
	// keep track of the size of the data table
	private int dataTableSize = 1;
	
	// R returns the vector as a map
	// need to keep the headers so we know the order to return
	private String[] headers;
	private String headerString;
	
	public RIterator(RBuilder builder, String[] headers) {
		this.builder = builder;
		this.headers = headers;
		this.headerString = RSyntaxHelper.createStringRColVec(headers);
		this.dataTableSize = this.builder.getNumRows();
		
		//TODO: this is because the FE keeps trying to grab the entire grid
		//TODO: this is because the FE keeps trying to grab the entire grid
		//TODO: this is because the FE keeps trying to grab the entire grid
		if(this.dataTableSize > 500) {
			this.dataTableSize = 500;
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
		REXP result = this.builder.executeR(   this.builder.addTryEvalToScript( "datatable[" + rowIndex + " , " + headerString + ", with=FALSE]" )   );
		
		Map<String, Object> data = null;
		try {
			// grab as a generic list object
			data = (Map<String, Object>) result.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			this.builder.handleRException(result, e, "Error grabbing the row number " + rowIndex + " from the datatable");
		}
		
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
