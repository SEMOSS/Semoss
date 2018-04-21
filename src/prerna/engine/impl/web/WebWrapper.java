package prerna.engine.impl.web;

import java.util.Hashtable;
import java.util.List;

import net.minidev.json.JSONArray;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.json.JsonWrapper;
import prerna.om.HeadersDataRow;

public class WebWrapper extends JsonWrapper {

	List <String [] >rows = null;
	String [] headers = null;
	
	@Override
	public void execute() {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable)engine.execQuery(query);
		rows = (List)output.get("ROWS");
		headers = (String [])output.get("HEADERS");
		numRows = rows.size();
		if(output.containsKey("TYPES"))
			types = (String [])output.get("TYPES");
	}
	
	@Override
	public IHeadersDataRow next() {
		
		IHeadersDataRow retRow = new HeadersDataRow(headers, rows.get(curRow));

		curRow = curRow + 1;
		return retRow;
	}
	

	
	
}
