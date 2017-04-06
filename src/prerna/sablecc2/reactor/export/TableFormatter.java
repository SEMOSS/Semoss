package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;

public class TableFormatter implements Formatter {

	private List<Object[]> data;
	private String[] headers;
	private String name;
//	private ArrayList<HashMap<String, Object>> headers;
	
	public TableFormatter() {
		data = new ArrayList<>(100);
	}
	
	public void addData(IHeadersDataRow nextData) {
		Object[] values = nextData.getValues();
		this.data.add(values);
	}
	
	public Object getFormattedData() {
//		Map<String, Object> returnData = new HashMap<>();
//		returnData.put("dataTableValues", data);
//		returnData.put("dataTableKeys", headers);
//		return returnData;
		return data;
	}
	
	@Override
	public void addHeader(String[] keys) {
		this.headers = keys;
	}

	@Override
	public void clear() {
		data = new ArrayList<>(100);
		headers = null;
	}

	@Override
	public void setIdentifier(String name) {
		// TODO Auto-generated method stub
		this.name = name;
	}
	
	@Override
	public String getIdentifier() {
		return this.name;
	}
}
