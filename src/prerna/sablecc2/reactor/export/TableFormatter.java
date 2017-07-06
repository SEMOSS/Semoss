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
		this.data = new ArrayList<>(100);
	}
	
	@Override
	public void addData(IHeadersDataRow nextData) {
		this.headers = nextData.getHeaders();
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
		this.data = new ArrayList<>(100);
		this.headers = null;
	}

}
