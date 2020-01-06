package prerna.ds.shared;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;

public class CachedIterator implements Iterator<IHeadersDataRow>{

	private String[] headers = null;
	private List<String> actHeaders = null;
	private boolean transform = false;
	// I will also set the query here
	// so it can be cached for future
	private String query = null;

	// Current implementation.. pulls the data into the memory.. we will change it after
	public List fullData = null;
	public SemossDataType[] colTypes = null;
	public int initSize = 0;
	List <IHeadersDataRow> values = new ArrayList<IHeadersDataRow>();
	transient ITableDataFrame frame = null;
	int counter = 0;
	int jcounter = 0;
	int finalSize = 0;
	int jsonSize = 0;
	boolean first = true;
	private List<String> jsonList = new ArrayList<String>();
	private StringBuilder allJson = new StringBuilder();
	
	
	public CachedIterator() {
	}
			
	@Override
	public boolean hasNext() {
		return counter < finalSize && jcounter < jsonSize;
	}

	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow retRow = values.get(counter);
		counter++;
		return retRow;
	}
	
	// set the headers
	public void setHeaders(String [] headers)
	{
		this.headers = headers;
	}
	
	public String [] getHeaders() {
		return headers;
	}
	
	public void setColTypes(SemossDataType [] colTypes)
	{
		this.colTypes = colTypes;
	}
	
	public SemossDataType [] getColTypes()
	{
		return this.colTypes;
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
	
	public void addNext(IHeadersDataRow row)
	{
		values.add(row);
	}
	
	public void setFrame(ITableDataFrame frame)
	{
		this.frame = frame;
		
	}
	
	public void addJson(String json)
	{
		jsonList.add(json);
		if(allJson.length() > 0)
			allJson.append(",");
		allJson.append(json);
	}
	
	public String getNextJson()
	{
		String json = jsonList.get(jcounter);
		jcounter++;
		return json;
	}
	
	public String getAllJson()
	{
		return allJson.toString();
	}
	
	public ITableDataFrame getFrame()
	{
		return this.frame;
	}
	
	public void processCache()
	{
		// I need to reset some stuff here ?
		finalSize = values.size();
		jsonSize = jsonList.size();
		counter = 0;
		jcounter = 0;
		if(frame instanceof PandasFrame)
			((PandasFrame)frame).cacheQuery(this);
		if(frame instanceof RDataTable)
			((RDataTable)frame).cacheQuery(this);
		first = false;
	}
}
