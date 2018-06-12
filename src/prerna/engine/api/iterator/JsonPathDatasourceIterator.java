package prerna.engine.api.iterator;

import java.util.Hashtable;

import net.minidev.json.JSONArray;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.json.JsonAPIEngine;
import prerna.om.HeadersDataRow;

public class JsonPathDatasourceIterator extends AbstractDatasourceIterator {

	public JsonAPIEngine engine;
	private JSONArray [] data = null;
	public String separator = "_";
	public int numRows = -1;
	public int curRow = 0;
	
	public JsonPathDatasourceIterator(JsonAPIEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		this.data = (JSONArray [])output.get("DATA");

		this.headers = (String [])output.get("HEADERS");
		this.rawHeaders = this.headers;
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
	public IHeadersDataRow next() {
		Object [] values = new Object[this.numColumns];
		for(int colIndex = 0; colIndex < this.numColumns; colIndex++) {
			JSONArray thisArray = this.data[colIndex];
			Object thisValue = thisArray.get(this.curRow);
			if(thisValue instanceof JSONArray) {
				// need to do the magic of delimiters etc. 
				JSONArray thisValueArray = (JSONArray)thisValue;
				StringBuffer output = new StringBuffer("");
				for(int valIndex = 0; valIndex < thisValueArray.size(); valIndex++) {
					if(valIndex != 0) {
						output.append(this.separator);
					}
					output.append(thisValueArray.get(valIndex));	
				}
				thisValue = output.toString();
			}
			values[colIndex] = thisValue;
		}
		this.curRow++;

		IHeadersDataRow retRow = new HeadersDataRow(this.headers, values);
		return retRow;
	}
	
	@Override
	public long getNumRecords() {
		return this.numRows;
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
