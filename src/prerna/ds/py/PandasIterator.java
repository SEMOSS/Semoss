package prerna.ds.py;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class PandasIterator implements Iterator<IHeadersDataRow>{

	private String[] headers = null;
	public SemossDataType[] colTypes = null;
	private ArrayList <String> actHeaders = null;
	private boolean transform = false;

	// Current implementation.. pulls the data into the memory.. we will change it after
	public ArrayList fullData = null;

	public PandasIterator(String [] headers, ArrayList fullData, SemossDataType [] colTypes) {
		
		this.headers = headers;
		this.fullData = fullData;
		this.colTypes = colTypes;
		
	}
	
	// set actual headers and transform
	public void setTransform(ArrayList <String> actHeaders, boolean transform)
	{
		this.transform = transform;
		this.actHeaders = actHeaders;
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return fullData.size() > 0;
	}

	@Override
	public IHeadersDataRow next() {
		// TODO Auto-generated method stub
		Object [] values = ((ArrayList)fullData.remove(0)).toArray();
		if(transform)
		{
			// this is going to be interesting.. alrite let us see
			Object [] newValues = new Object[headers.length];
			for(int headerIndex = 0;headerIndex < headers.length;headerIndex++)
			{
				int index = actHeaders.indexOf(headers[headerIndex]);
				newValues[headerIndex] = values [index];
			}
			values = newValues;
		}
		return new HeadersDataRow(headers, values);
		
	}
	
	public String [] getHeaders()
	{
		return headers;
	}

}
