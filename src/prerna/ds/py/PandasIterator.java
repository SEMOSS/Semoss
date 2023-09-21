package prerna.ds.py;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class PandasIterator implements Iterator<IHeadersDataRow>{

	private String[] headers = null;
	private List<String> actHeaders = null;
	private boolean transform = false;
	// I will also set the query here
	// so it can be cached for future
	private String query = null;

	// Current implementation.. pulls the data into the memory.. we will change it after
	public List fullData = new ArrayList<Object>();;
	public SemossDataType[] colTypes = null;
	public int initSize = 0;
	
	public PandasIterator(String [] headers, List fullData, SemossDataType [] colTypes) {
		this.headers = headers;
		this.colTypes = colTypes;
		
		if (fullData != null) {
			this.fullData = fullData;
			this.initSize = fullData.size();
		}
	}
	
	// set actual headers and transform
	public void setTransform(List<String> actHeaders, boolean transform) {
		this.transform = transform;
		this.actHeaders = actHeaders;
	}
		
	@Override
	public boolean hasNext() {
		return fullData.size() > 0;
	}

	@Override
	public IHeadersDataRow next() {
		Object [] values = null;
		if(fullData.get(0) instanceof List)
		{
			values = ((List) fullData.remove(0)).toArray();
			if(transform) {
				// this is going to be interesting.. alrite let us see
				// I wonder if I can even change the headers on the fly too, I dont know
				Object [] newValues = new Object[headers.length];
				for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
					int index = actHeaders.indexOf(headers[headerIndex]);
					if(index == -1) // there is a possibility the person is coming in with a different query
						index = headerIndex;
					newValues[headerIndex] = values [index];
				}
				values = newValues;
			}
			
			// there is possibly another possibility here
			// it is an array list of arrays
			// basically it is a list of arrays
		}
		else
		{
			values = new Object[1];
			values[0] = fullData.remove(0);
		}
		IHeadersDataRow thisRow = new HeadersDataRow(headers, values);
		thisRow.setQuery(query);
		return thisRow;
	}
	
	public String [] getHeaders() {
		return headers;
	}
	
	public int getInitSize() {
		return initSize;
	}
	
	// sets the query
	public void setQuery(String query)
	{
		this.query = query;
	}
	
	public String getQuery()
	{
		return this.query;
		
	}
}
