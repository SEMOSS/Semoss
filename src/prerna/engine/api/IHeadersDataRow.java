package prerna.engine.api;

import java.util.Iterator;

public interface IHeadersDataRow{

	public int getRecordLength();
	
	public String[] getHeaders();
	
	public Object[] getValues();
	
	public Object[] getRawValues();
	
	public String toRawString();
	// <<<<<<< Methods to be used for other purposes
	
	public String toJson();	
	
	// gets a particular value
	public void open();
		
	// add a tuple
	public void addField(String fieldName, Object value);
	
	// gets a particular field
	public Object getField(String fieldName);
	
}
