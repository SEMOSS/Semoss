package prerna.engine.impl.web;

import java.util.Hashtable;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.json.JsonWrapper;
import prerna.om.HeadersDataRow;

public class WebWrapper extends JsonWrapper {

	private List<String[]> rows = null;
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		rows = (List)output.get("ROWS");
		headers = (String [])output.get("HEADERS");
		this.numColumns = this.headers.length;
		numRows = rows.size();
		String[] strTypes = (String[]) output.get("TYPES");
		this.types = new SemossDataType[this.numColumns];
		for(int i = 0; i < this.numColumns; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}
	
	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow retRow = new HeadersDataRow(headers, rows.get(curRow));
		curRow++;
		return retRow;
	}
	

	
	
}
