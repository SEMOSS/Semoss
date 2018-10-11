package prerna.om;

import java.util.Arrays;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.engine.api.IHeadersDataRow;
import prerna.util.gson.GsonUtility;

public class HeadersDataRow implements IHeadersDataRow{

	/**
	 * Base components corresponding to a header row
	 */
	private String[] headers;
	private String[] rawHeaders;
	private Object[] values;
	private Object[] rawValues;
	private int recordLength;
	
	private Vector <Object> vecValues = null;
	private Vector <String> vecHeaders = null;
	
	public HeadersDataRow(String[] headers, Object[] values) {
		this(headers, headers, values, values);
	}
	
	public HeadersDataRow(String[] headers, String[] rawHeaders, Object[] values) {
		this(headers, rawHeaders, values, values);
	}

	public HeadersDataRow(String[] headers, Object[] values, Object[] rawValues) {
		this(headers, headers, values, rawValues);
	}

	public HeadersDataRow(String[] headers, String[] rawHeaders, Object[] values, Object[] rawValues) {
		if(headers.length != values.length && values.length != rawValues.length) {
			throw new IllegalArgumentException("Length of parameters not equal");
		}
		
		this.headers = headers;
		this.rawHeaders = rawHeaders;
		if(this.rawHeaders == null) {
			this.rawHeaders = this.headers;
		}
		this.values = values;
		this.rawValues = rawValues;
		
		this.recordLength = headers.length;
	}

	@Override
	public int getRecordLength() {
		return this.recordLength;
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
	
	@Override
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
	
	@Override
	public void addFields(String[] addHeaders, Object[] addValues) {
		int newValuesLength = addHeaders.length;
		if(newValuesLength != addValues.length) {
			throw new IllegalArgumentException("Length of parameters not equal");
		}
		
		// we will make new arrays and copy over the values
		int totalLength = this.recordLength + newValuesLength;
		String[] newHeaders = new String[totalLength];
		Object[] newValues = new Object[totalLength];
		
		System.arraycopy(this.headers, 0, newHeaders, 0, this.recordLength);
		System.arraycopy(this.values, 0, newValues, 0, this.recordLength);
		
		// add the new values into the new headers / values
		int counter = 0;
		for(int i = 0; i < newValuesLength; i++) {
			newHeaders[this.recordLength + i] = addHeaders[counter];
			newValues[this.recordLength + i] = addValues[counter];
			counter++;
		}
		
		// adjust references
		this.headers = newHeaders;
		this.values = newValues;
		// TODO: expose raw headers and raw values as well
		this.rawHeaders = this.headers;
		this.rawValues = this.values;
	}
	
	@Override
	public void addFields(String addHeader, Object addValues) {
		// we will make new arrays and copy over the values
		int totalLength = this.recordLength + 1;
		String[] newHeaders = new String[totalLength];
		Object[] newValues = new Object[totalLength];
		
		System.arraycopy(this.headers, 0, newHeaders, 0, this.recordLength);
		System.arraycopy(this.values, 0, newValues, 0, this.recordLength);
		
		// add the new values into the new headers / values
		newHeaders[this.recordLength] = addHeader;
		newValues[this.recordLength] = addValues;
		
		// adjust references
		this.headers = newHeaders;
		this.values = newValues;
		// TODO: expose raw headers and raw values as well
		this.rawHeaders = this.headers;
		this.rawValues = this.values;
	}
	
	@Override
	public IHeadersDataRow copy() {
		// convert the main portions and return new
		Gson gson = GsonUtility.getDefaultGson();
		String[] cHeaders = gson.fromJson(gson.toJson(this.headers), String[].class);
		String[] cRawHeaders = gson.fromJson(gson.toJson(this.rawHeaders), String[].class);
		Object[] cValues = gson.fromJson(gson.toJson(this.values), Object[].class);
		Object[] cRawValues = gson.fromJson(gson.toJson(this.rawValues), Object[].class);
		
		return new HeadersDataRow(cHeaders, cRawHeaders, cValues, cRawValues);
	}
	
	
	
	
	
	
	/////////////////////////////////////////////


	@Override
	public String toJson() {
		return null;
	}

	@Override
	public void open() {
		vecHeaders = new Vector<String>();
		vecValues = new Vector<Object>();
		vecHeaders.addAll(Arrays.asList(headers));
		vecValues.addAll(Arrays.asList(values));
	}

	@Override
	public void addField(String fieldName, Object value) {
		vecHeaders.add(fieldName);
		vecValues.add(value);
	}
	
	public boolean isValue(String fieldName) {
		return vecHeaders.indexOf(fieldName) >= 0;
	}

	@Override
	public Object getField(String fieldName) {
		int fieldIndex = vecHeaders.indexOf(fieldName);
		if(fieldIndex >= 0) {
			return vecValues.elementAt(fieldIndex);
		}
		return null;
	}

	@Override
	public String[] getRawHeaders() {
		return rawHeaders;
	}

	@Override
	public HEADERS_DATA_ROW_TYPE getHeaderType() {
		return IHeadersDataRow.HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW;
	}
}
