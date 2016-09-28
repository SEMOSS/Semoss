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
	
	public String toString() {
		StringBuilder ret = new StringBuilder();
		int size = headers.length;
		int index = 0;
		ret.append("START ROW\n");
		for(; index < size; index++) {
			ret.append("\tHEADER=").append(headers[index]).append("\tVALUE=").append(values[index]).append("\n");
		}
		ret.append("END ROW\n");
		
		return ret.toString();
	}

	@Override
	public String toRawString() {
		StringBuilder ret = new StringBuilder();
		int size = headers.length;
		int index = 0;
		ret.append("START ROW\n");
		for(; index < size; index++) {
			ret.append("\tHEADER=").append(headers[index]).append("\tVALUE=").append(rawValues[index]).append("\n");
		}
		ret.append("END ROW\n");
		
		return ret.toString();
	}
}
