package prerna.engine.impl.rdf;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;

import jodd.util.ArraysUtil;
import prerna.engine.api.IHeadersDataRow;
public class HeadersDataRow implements IHeadersDataRow{

	String[] headers;
	Object[] values;
	Object[] rawValues;
	
	
	
	Vector <Object> vecValues = null;
	Vector <String> vecHeaders = null;
	Hashtable <String, Integer> headerCardinalityHash = new Hashtable <String, Integer>();
	
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


	@Override
	public String toJson() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		vecHeaders = new Vector<String>();
		vecValues = new Vector<Object>();
		vecHeaders.addAll(Arrays.asList(headers));
		vecValues.addAll(Arrays.asList(values));
	}
		

	@Override
	public void addField(String fieldName, Object value) {
		// TODO Auto-generated method stub
		vecHeaders.add(fieldName);
		vecValues.add(value);
	}
	
	public boolean isValue(String fieldName)
	{
		return vecHeaders.indexOf(fieldName) >= 0;
	}

	@Override
	public Object getField(String fieldName) {
		// TODO Auto-generated method stub
		int fieldIndex = vecHeaders.indexOf(fieldName);
		if(fieldIndex >= 0)
			return vecValues.elementAt(fieldIndex);
		
		return null;
	}
}
