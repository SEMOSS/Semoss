package prerna.reactor.export;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;

public class TableFormatter extends AbstractFormatter {

	public static final String FORMAT_TYPE = "TABLE";
	
	private List<Object[]> data;
	private String[] headers;
	private String[] rawHeaders;

	public TableFormatter() {
		this.data = new ArrayList<>(100);
		this.headers = new String[0];
		this.rawHeaders = new String[0];
	}
	
	@Override
	public void addData(IHeadersDataRow nextData) {
		this.headers = nextData.getHeaders();
		this.rawHeaders = nextData.getRawHeaders();
		this.data.add(nextData.getValues());
	}
	
	public Object getFormattedData() {
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", this.data);
		returnData.put("headers", this.headers);
		returnData.put("rawHeaders", this.rawHeaders);
		return returnData;
	}

	@Override
	public void clear() {
		this.data = new ArrayList<>(100);
		this.headers = new String[0];
	}

	@Override
	public String getFormatType() {
		return TableFormatter.FORMAT_TYPE;
	}
	
	public String[] getHeaders() {
		return this.headers;
	}
	
	public List<Object[]> getData() {
		return this.data;
	}
}
