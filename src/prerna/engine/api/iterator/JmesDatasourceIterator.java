package prerna.engine.api.iterator;

import java.util.Hashtable;

import com.fasterxml.jackson.databind.node.ArrayNode;

import net.minidev.json.JSONArray;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.json.JsonAPIEngine2;
import prerna.om.HeadersDataRow;

public class JmesDatasourceIterator extends AbstractDatasourceIterator {

	public JsonAPIEngine2 engine;
	private ArrayNode data = null;
	public int numRows = -1;
	public int curRow = 0;
	
	public JmesDatasourceIterator(JsonAPIEngine2 engine) {
		this.engine = engine;
	}
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		this.data = (ArrayNode) output.get("DATA");

		this.headers = (String[]) output.get("HEADERS");
		this.rawHeaders = this.headers;
		this.numColumns = this.headers.length;
		this.numRows = (Integer) output.get("COUNT");

		String[] strTypes = (String[]) output.get("TYPES");
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
