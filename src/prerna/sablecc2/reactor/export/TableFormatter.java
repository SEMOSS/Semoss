package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;

public class TableFormatter implements Formatter {

	private List<Object[]> data;
	private String[] headers;
	
	public TableFormatter() {
		data = new ArrayList<>(100);
	}
	public void addData(IHeadersDataRow nextData) {
		if(this.headers == null) {
			this.headers = nextData.getHeaders();
		}
		Object[] values = nextData.getValues();
		this.data.add(values);
	}
	
	public Object getFormattedData() {
		Map<String, Object> returnData = new HashMap<>();
		returnData.put("data", data);
		returnData.put("headers", headers);
		return returnData;
	}
	
	
}
