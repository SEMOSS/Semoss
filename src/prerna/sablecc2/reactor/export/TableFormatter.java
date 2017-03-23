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
	private ArrayList<HashMap<String, Object>> headers;
	
	public TableFormatter() {
		data = new ArrayList<>(100);
		headers = new ArrayList<>();
	}
	public void addData(IHeadersDataRow nextData) {
		Object[] values = nextData.getValues();
		this.data.add(values);
	}
	
	public Object getFormattedData() {
		Map<String, Object> returnData = new HashMap<>();
		returnData.put("dataTableValues", data);
		returnData.put("dataTableKeys", headers);
		return returnData;
	}
	@Override
	public void addHeader(String[] keys) {
		HashMap<String,Object> header = new HashMap<String,Object>();
		header.put("varKey", keys[0]);
		header.put("type", keys[1]);
		header.put("vizType", keys[2]);
		headers.add(header);
	}
	
}
