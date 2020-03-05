package prerna.ds.shared;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

public class CachedIterator implements Iterator<IHeadersDataRow> {

	private transient ITableDataFrame frame = null;

	// I will also set the query here
	// so it can be cached for future
	private String query = null;
	
	private String[] headers = null;
	private SemossDataType[] colTypes = null;

	// Current implementation.. pulls the data into the memory.. we will change it after
	private List<IHeadersDataRow> values = new ArrayList<IHeadersDataRow>();
	
	private int initSize = 0;
	private int counter = 0;
	private int jcounter = 0;
	private int finalSize = 0;
	private int jsonSize = 0;
	private boolean first = true;
	
	private StringBuilder allJson = new StringBuilder();
	private List<String> jsonList = new ArrayList<String>();
	
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
	
	public void setHeaders(String [] headers) {
		this.headers = headers;
	}
	
	public String[] getHeaders() {
		return headers;
	}
	
	public void setColTypes(SemossDataType [] colTypes) {
		this.colTypes = colTypes;
	}
	
	public SemossDataType [] getColTypes() {
		return this.colTypes;
	}
	
	public int getInitSize() {
		return initSize;
	}
	
	// sets the query
	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getQuery() {
		return this.query;
	}
	
	public void addNext(IHeadersDataRow row) {
		values.add(row);
	}
	
	public void setFrame(ITableDataFrame frame) {
		this.frame = frame;
	}
	
	public void addJson(String json) {
		this.jsonList.add(json);
		if(this.allJson.length() > 0) {
			this.allJson.append(",");
		}
		this.allJson.append(json);
	}
	
	public String getNextJson() {
		String json = this.jsonList.get(this.jcounter);
		this.jcounter++;
		return json;
	}
	
	public boolean getFirst() {
		return this.first;
	}
	
	public String getAllJson() {
		return this.allJson.toString();
	}
	
	public ITableDataFrame getFrame() {
		return this.frame;
	}
	
	public void processCache() {
		// I need to reset some stuff here ?
		this.finalSize = this.values.size();
		this.jsonSize = this.jsonList.size();
		this.counter = 0;
		this.jcounter = 0;
		this.first = false;
		this.frame.cacheQuery(this);
	}
}
