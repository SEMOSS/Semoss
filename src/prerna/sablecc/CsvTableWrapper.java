package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;

public class CsvTableWrapper implements Iterator{

	private List<Vector<Object>> values;
	private String[] headers;
	private int currIndex = 0;
	
	public CsvTableWrapper(List<Vector<Object>> values) {
		this.values = values;
		this.headers = values.get(0).toArray(new String[]{});
		currIndex = 1;
	}
	
	@Override
	public boolean hasNext() {
		if(values.size() > currIndex) {
			return true;
		}
		return false;
	}

	@Override
	public IHeadersDataRow next() {
		Object[] currRow = values.get(currIndex).toArray();
		InnerRetObj retObj = new InnerRetObj(this.headers, currRow);
		currIndex++;
		return retObj;
	}
	
	
	public class InnerRetObj implements IHeadersDataRow {

		private String[] headers;
		private Object[] dataRow;
		
		public InnerRetObj(String[] headers, Object[] dataRow) {
			this.headers = headers;
			this.dataRow = dataRow;
		}
		
		@Override
		public int getRecordLength() {
			return this.headers.length;
		}

		@Override
		public String[] getHeaders() {
			return this.headers;
		}

		@Override
		public Object[] getValues() {
			return this.dataRow;
		}

		@Override
		public Object[] getRawValues() {
			return this.dataRow;
		}
	}

}
