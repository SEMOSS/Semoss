package prerna.ds.py;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class PandasParquetIterator implements Iterator<IHeadersDataRow> {

	private String[] headers = null;
	private List<String> actHeaders = null;
	private boolean transform = false;
	// I will also set the query here
	// so it can be cached for future
	private String query = null;

	// Current implementation.. pulls the data into the memory.. we will change it after
	public SemossDataType[] colTypes = null;
	public Object blob = null;
	
	public Object [] finalList = null;
	public String [] finalTypes = null;
	public int finalSize = 0;
	public int cursor = 0; // where the current position is
	
	public PandasParquetIterator(String [] headers, Object blob, SemossDataType [] colTypes) 
	{
		this.headers = headers;
		this.blob = blob;
		this.colTypes = colTypes;

		preprocess();
	}
	
	// set actual headers and transform
	public void setTransform(List<String> actHeaders, boolean transform) {
		this.transform = transform;
		this.actHeaders = actHeaders;
	}
		
	@Override
	public boolean hasNext() {
		return cursor < finalSize;
	}

	@Override
	public IHeadersDataRow next() {
		Object [] values = new Object[finalList.length];

		for(int colIndex = 0;colIndex < finalList.length;colIndex++)
		{
			String type = finalTypes[colIndex];
			Object thisCol = finalList[colIndex];
			Object out = null;
			if(type.equalsIgnoreCase("list"))
				out = ((List)thisCol).get(cursor);
			if(type.equalsIgnoreCase("long"))
				out = ((long [])thisCol)[cursor];
			if(type.equalsIgnoreCase("int"))
				out = ((int [])thisCol)[cursor];
			if(type.equalsIgnoreCase("float"))
				out = ((float [])thisCol)[cursor];
			if(type.equalsIgnoreCase("double"))
				out = ((double [])thisCol)[cursor];
			
			values[colIndex] = out;
		}

		cursor++;
		IHeadersDataRow thisRow = new HeadersDataRow(headers, values);
		thisRow.setQuery(query);
		return thisRow;
	}
	
	public String [] getHeaders() {
		return headers;
	}
	
	public int getInitSize() {
		return finalSize;
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
	
	private void preprocess()
	{
		
		// my wager is converting things toarray or to list is more expensive than doing if loops
		if(blob instanceof List)
		{
			List daList = (List)blob;
			finalList = new Object[daList.size()];
			finalTypes = new String[daList.size()];
			
			for(int colIndex = 0;colIndex < daList.size();colIndex++)
			{
				Object elem = daList.get(colIndex);
				if(elem instanceof jep.NDArray)
					elem = ((jep.NDArray)elem).getData();

				if(elem instanceof long[])
				{
					System.out.println("Long Array ");
					finalList[colIndex] = (long [])elem;
					finalSize = ((long [])elem).length;
					
					finalTypes[colIndex] = "long";
				}
				if(elem instanceof List)
				{
					System.out.println("List ");
					finalList[colIndex] = (List)elem;
					finalSize = ((List)elem).size();
					finalTypes[colIndex] = "list";
				}
				if(elem instanceof double[])
				{
					System.out.println("double Array ");
					finalList[colIndex] = (double [])elem;
					finalSize = ((double [])elem).length;
					finalTypes[colIndex] = "double";
				}
				if(elem instanceof int[])
				{
					System.out.println("integer Array ");
					finalList[colIndex] = (int [])elem;
					finalSize = ((int [])elem).length;
					finalTypes[colIndex] = "int";
				}
				if(elem instanceof float[])
				{
					System.out.println("float Array ");
					finalList[colIndex] = (float [])elem;
					finalSize = ((float [])elem).length;
					finalTypes[colIndex] = "float";
					
				}
			}
		}
	}
}
