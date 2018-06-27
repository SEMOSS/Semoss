package prerna.poi.main.helper;

import prerna.engine.api.IHeadersDataRow;

public class ExcelHeadersDataRow implements IHeadersDataRow {

	String [] headers = null;
	Object [] data = null;
	
	public ExcelHeadersDataRow(String[] headers2, Object[] data) {
		// TODO Auto-generated constructor stub
		this.headers = headers;
		this.data = data;
	}

	@Override
	public String[] getHeaders() {
		// TODO Auto-generated method stub
		return this.headers;
	}

	@Override
	public String[] getRawHeaders() {
		// TODO Auto-generated method stub
		return headers;
	}

	@Override
	public Object[] getValues() {
		// TODO Auto-generated method stub
		return data;
	}

	@Override
	public Object[] getRawValues() {
		// TODO Auto-generated method stub
		return data;
	}

	@Override
	public int getRecordLength() {
		// TODO Auto-generated method stub
		return data.length;
	}

	@Override
	public String toRawString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addFields(String[] addHeaders, Object[] addValues) {
		// TODO Auto-generated method stub
		// hmmm this is mutable now ? oh I see when we have lambdas ok

	}

	@Override
	public IHeadersDataRow copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toJson() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub

	}

	@Override
	public void addField(String fieldName, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getField(String fieldName) {
		// TODO Auto-generated method stub
		return null;
	}

}
