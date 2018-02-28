package prerna.engine.impl.json;

import java.util.Hashtable;
import java.util.Map;

import net.minidev.json.JSONArray;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;

public class JsonWrapper implements IRawSelectWrapper {
	
	// set all the data here
	JSONArray [] data = null;
	String [] headerNames = null;
	IEngine engine = null;
	String query = null;
	int numRows = -1;
	int curRow = 0;
	String [] types = null;
	
	
	public JsonWrapper()
	{
		//this.data = data;
	}

	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		data = (JSONArray [])output.get("DATA");
		headerNames = (String [])output.get("HEADERS");
		numRows = (Integer)output.get("COUNT");
		types = (String [])output.get("TYPES");
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return curRow < numRows;
	}

	@Override
	public void setQuery(String query) {

		this.query = query;
	}

	@Override
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}

	@Override
	public Map<String, Object> getResponseMeta() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IHeadersDataRow next() {
		
		Object [] values = new Object[headerNames.length];
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
		{
			JSONArray thisArray = data[colIndex];
			
			Object thisValue = thisArray.get(curRow);
			if(thisValue instanceof JSONArray)
			{
				// need to do the magic of delimiters etc. 
			}
			
			values[colIndex] = thisValue;
		}
		
		IHeadersDataRow retRow = new HeadersDataRow(headerNames, values);

		curRow = curRow + 1;
		return retRow;
	}

	@Override
	public String[] getDisplayVariables() {
		return headerNames;
	}

	@Override
	public String[] getPhysicalVariables() {
		return headerNames;
	}

	@Override
	public String[] getTypes() {
		return types;
	}
	
}
