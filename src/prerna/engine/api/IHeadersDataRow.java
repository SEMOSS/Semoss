package prerna.engine.api;

public interface IHeadersDataRow {

	public int getRecordLength();
	
	public String[] getHeaders();
	
	public Object[] getValues();
	
	public Object[] getRawValues();
	
	public String toRawString();
}
