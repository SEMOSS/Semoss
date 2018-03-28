package prerna.engine.impl.json;

import java.util.Hashtable;

import com.fasterxml.jackson.databind.node.ArrayNode;

import net.minidev.json.JSONArray;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class JsonWrapper2 extends JsonWrapper {
	
	// I need to implement the following methods
	// execute
	// 
	ArrayNode data = null;
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		if(output.containsKey("DATA")) {
			data = (ArrayNode)output.get("DATA");
		}
		
		if(output.containsKey("HEADERS")) {
			headerNames = (String [])output.get("HEADERS");
		}
	
		if(output.containsKey("COUNT")) {
			numRows = (Integer)output.get("COUNT");
		}
		
		if(output.containsKey("TYPES")) {
			types = (String [])output.get("TYPES");
		}
	}

	@Override
	public IHeadersDataRow next() {

		ArrayNode thisRow = (ArrayNode)data.get(curRow);

		Object [] values = new Object[headerNames.length];
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
		{
			Object thisValue = null;
			if(types[colIndex].equalsIgnoreCase("STRING")) {
				thisValue = thisRow.get(colIndex).asText("");
			} else if(types[colIndex].equalsIgnoreCase("NUMBER")) {
				thisValue = thisRow.get(colIndex).asDouble();
			} else if(types[colIndex].equalsIgnoreCase("INT")) {
				thisValue = thisRow.get(colIndex).asInt();
			} else if(thisValue instanceof JSONArray)
			{
				// need to do the magic of delimiters etc. 
			}

			values[colIndex] = thisValue;
		}

		IHeadersDataRow retRow = new HeadersDataRow(headerNames, values);

		curRow = curRow + 1;
		return retRow;
	}


}
