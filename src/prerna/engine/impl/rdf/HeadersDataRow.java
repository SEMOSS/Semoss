package prerna.engine.impl.rdf;

import prerna.engine.api.IHeadersDataRow;
public class HeadersDataRow implements IHeadersDataRow{

	String[] headers;
	Object[] values;
	Object[] rawValues;
	
	public HeadersDataRow(String[] headers, Object[] values, Object[] rawValues) {
		if(headers.length != values.length && values.length != rawValues.length) {
			throw new IllegalArgumentException("Length of parameters not equal");
		}
		this.headers = headers;
		this.values = values;
		this.rawValues = rawValues;
	}
	
	@Override
	public int getRecordLength() {
		return headers.length;
	}

	@Override
	public String[] getHeaders() {
		return headers;
	}

	@Override
	public Object[] getValues() {
		return values;
	}

	@Override
	public Object[] getRawValues() {
		return rawValues;
	}
}
