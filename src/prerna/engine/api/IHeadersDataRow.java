package prerna.engine.api;

import com.google.gson.TypeAdapter;

import prerna.util.gson.HeadersDataRowAdapter;
import prerna.util.gson.IHeadersDataRowAdapter;

public interface IHeadersDataRow{

	// Right now there is only one implementation of IHeadersDataRow
	enum HEADERS_DATA_ROW_TYPE {HEADERS_DATA_ROW};
	
	/**
	 * Get the type of the header
	 * @return
	 */
	HEADERS_DATA_ROW_TYPE getHeaderType();
	
	/**
	 * Get the headers corresponding to the values by index
	 * @return
	 */
	String[] getHeaders();

	/**
	 * Get the raw headers
	 * This is useful when we alias headers to be unique during loops
	 * @return
	 */
	String[] getRawHeaders();

	/**
	 * Get the values of the row
	 * @return
	 */
	Object[] getValues();
	
	/**
	 * Get the raw values
	 * This is useful if you want to see full URIs from a RDF engine
	 * @return
	 */
	Object[] getRawValues();

	/**
	 * Get the number of records in the row
	 * @return
	 */
	int getRecordLength();

	/**
	 * This is really only for testing purposes
	 * @return
	 */
	String toRawString();

	/**
	 * Add new values into an existing headers data row
	 * @param addHeaders
	 * @param addValues
	 */
	void addFields(String[] addHeaders, Object[] addValues);

	/**
	 * Add a single new column and value
	 * @param addHeader
	 * @param addValues
	 */
	void addFields(String addHeader, Object addValues);

	
	/**
	 * Copy the headers row
	 * @return
	 */
	IHeadersDataRow copy();
	
	
	// <<<<<<< Methods to be used for other purposes
	
	String toJson();	
	
	// gets a particular value
	void open();
		
	// add a tuple
	void addField(String fieldName, Object value);
	
	// gets a particular field
	Object getField(String fieldName);

	/*
	 * 
	 * Methods around serialization
	 * 
	 */
	
	// Right now only one ;)
	static TypeAdapter getAdapterForHeader(HEADERS_DATA_ROW_TYPE type) {
		if(type == HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW) {
			return new HeadersDataRowAdapter();
		}		
		return null;
	}
	
	/**
	 * Convert string to SELECTOR_TYPE
	 * @param s
	 * @return
	 */
	static HEADERS_DATA_ROW_TYPE convertStringToHeaderType(String s) {
		if(s.equals(HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW.toString())) {
			return HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW;
		}
		return null;
	}
	
	
}
