package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;

public class TableFormatter extends AbstractFormatter {

	private List<Object[]> data;
	private String[] headers;
	
	public TableFormatter() {
		data = new ArrayList<>(100);
	}
	
	public void addData(IHeadersDataRow nextData) {
		headers = nextData.getHeaders();
		this.data.add(nextData.getValues());
	}
	
	public Object getFormattedData() {
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", data);
		returnData.put("headers", headers);
		return returnData;
	}

	@Override
	public void clear() {
		data = new ArrayList<>(100);
		headers = null;
	}

}
