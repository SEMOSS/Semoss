package prerna.engine.impl.json;

import java.util.Hashtable;

import net.minidev.json.JSONArray;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;

public class JsonWrapper implements IRawSelectWrapper {
	
	protected IEngine engine;
	protected String separator = "_";
	protected int numRows = -1;
	protected int curRow = 0;
	
	// values for querying
	protected String query;
	
	// number of return columns
	protected int numColumns = 0;
	
	// values for return
	protected String[] headers;
	protected	SemossDataType[] types;
	
	// specific for this engine
	// json wrapper 2 deos not use this
	// but shares the above
	private JSONArray [] data = null;
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		this.data = (JSONArray [])output.get("DATA");

		this.headers = (String [])output.get("HEADERS");
		this.numColumns = this.headers.length;
		this.numRows = (Integer)output.get("COUNT");

		if(output.containsKey("SEPARATOR")) {
			separator = (String)output.get("SEPARATOR");
		}
		
		String[] strTypes = (String []) output.get("TYPES");
		this.types = new SemossDataType[this.numColumns];
		for(int i = 0; i < this.numColumns; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}

	@Override
	public boolean hasNext() {
		return curRow < numRows;
	}

	@Override
	public void setQuery(String query) {
		this.query = query;
	}
	
	@Override
	public String getQuery() {
		return this.query;
	}

	@Override
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}

	@Override
	public IHeadersDataRow next() {
		Object [] values = new Object[headers.length];
		for(int colIndex = 0;colIndex < headers.length;colIndex++) {
			JSONArray thisArray = data[colIndex];

			Object thisValue = thisArray.get(curRow);
			if(thisValue instanceof JSONArray) {
				// need to do the magic of delimiters etc. 
				JSONArray thisValueArray = (JSONArray)thisValue;
				StringBuffer output = new StringBuffer("");
				for(int valIndex = 0; valIndex < thisValueArray.size(); valIndex++) {
					if(valIndex != 0) {
						output.append(separator);
					}
					output.append(thisValueArray.get(valIndex));	
				}
				thisValue = output.toString();
			}

			values[colIndex] = thisValue;
		}
		curRow++;

		IHeadersDataRow retRow = new HeadersDataRow(this.headers, values);
		return retRow;
	}

	@Override
	public String[] getHeaders() {
		return this.headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		return types;
	}
	
	@Override
	public long getNumRows() {
		return this.numRows;
	}
	
	@Override
	public long getNumRecords() {
		return this.numRows * this.headers.length;
	}
	
	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	
}
