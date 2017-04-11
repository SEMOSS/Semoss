package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IHeadersDataRow;

public class TableFormatter extends AbstractFormatter {

	private List<Object[]> data;

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
	public void clear() {
		data = new ArrayList<>(100);
		headers = null;
	}

}
