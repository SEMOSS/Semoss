package prerna.ds.py;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class PandasIterator implements Iterator<IHeadersDataRow>{

	private String[] headers = null;
	private ArrayList <String> actHeaders = null;
	private boolean transform = false;

	// Current implementation.. pulls the data into the memory.. we will change it after
	public List fullData = null;
	public SemossDataType[] colTypes = null;
	public int initSize = 0;
	
	public PandasIterator(String [] headers, List fullData, SemossDataType [] colTypes) {
		this.headers = headers;
		this.fullData = fullData;
		this.colTypes = colTypes;

		this.initSize = fullData.size();
	}
	
	// set actual headers and transform
	public void setTransform(ArrayList <String> actHeaders, boolean transform) {
		this.transform = transform;
		this.actHeaders = actHeaders;
	}
		
	@Override
	public boolean hasNext() {
		return fullData.size() > 0;
	}

	@Override
	public IHeadersDataRow next() {
		Object [] values = ((List) fullData.remove(0)).toArray();
		if(transform) {
			// this is going to be interesting.. alrite let us see
			Object [] newValues = new Object[headers.length];
			for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
				int index = actHeaders.indexOf(headers[headerIndex]);
				newValues[headerIndex] = values [index];
			}
			values = newValues;
		}
		return new HeadersDataRow(headers, values);
	}
	
	public String [] getHeaders() {
		return headers;
	}
	
	public int getInitSize() {
		return initSize;
	}
}
