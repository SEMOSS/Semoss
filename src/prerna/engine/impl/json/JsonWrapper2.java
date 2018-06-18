package prerna.engine.impl.json;

import java.util.Hashtable;

import com.fasterxml.jackson.databind.node.ArrayNode;

import net.minidev.json.JSONArray;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class JsonWrapper2 extends JsonWrapper {
	
	private ArrayNode data = null;
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		this.data = (ArrayNode) output.get("DATA");

		this.headers = (String[]) output.get("HEADERS");
		this.numColumns = this.headers.length;
		this.numRows = (Integer) output.get("COUNT");

		String[] strTypes = (String[]) output.get("TYPES");
		this.types = new SemossDataType[this.numColumns];
		for(int i = 0; i < this.numColumns; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}

	@Override
	public IHeadersDataRow next() {
		ArrayNode thisRow = (ArrayNode) data.get(curRow);

		Object[] values = new Object[this.numColumns];
		for(int colIndex = 0; colIndex < this.numColumns; colIndex++) {
			Object thisValue = null;
			if(types[colIndex] == SemossDataType.STRING) {
				thisValue = thisRow.get(colIndex).asText("");
			} else if(types[colIndex] == SemossDataType.DOUBLE) {
				thisValue = thisRow.get(colIndex).asDouble();
			} else if(types[colIndex] == SemossDataType.INT) {
				thisValue = thisRow.get(colIndex).asInt();
			} else if(thisValue instanceof JSONArray) {
				// need to do the magic of delimiters etc. 
			}
			
			values[colIndex] = thisValue;
		}
		this.curRow++;

		IHeadersDataRow retRow = new HeadersDataRow(this.headers, values);
		return retRow;
	}

}
