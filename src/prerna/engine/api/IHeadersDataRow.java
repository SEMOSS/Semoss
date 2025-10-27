package prerna.engine.api;

import java.util.Map;

import com.google.gson.TypeAdapter;

import prerna.util.gson.HeadersDataRowAdapter;

/**
 * Interface representing a data row with associated column headers for query results.
 * 
 * <p>This interface provides a structured way to handle tabular data results from
 * database queries, API calls, and other data sources. Each row contains both the
 * data values and their corresponding column headers, enabling proper data interpretation
 * and manipulation.</p>
 * 
 * <p>Key features include:</p>
 * <ul>
 *   <li><strong>Header Management:</strong> Access to column names and aliases</li>
 *   <li><strong>Data Access:</strong> Retrieve values by index or field name</li>
 *   <li><strong>Raw Data Support:</strong> Access to unprocessed headers and values</li>
 *   <li><strong>Dynamic Extension:</strong> Add new fields to existing rows</li>
 *   <li><strong>Serialization:</strong> Convert to JSON and other formats</li>
 * </ul>
 * 
 * <p>The interface supports both raw and processed data access, which is particularly
 * useful when working with RDF engines where URIs may be shortened for display
 * but full URIs are needed for certain operations.</p>
 * 
 * @see {@link IApi} for API operations that produce these data rows
 * @see {@link HeadersDataRowAdapter} for JSON serialization support
 * @author SEMOSS
 */
public interface IHeadersDataRow{

	/** Enumeration of available header data row implementation types */
	enum HEADERS_DATA_ROW_TYPE {
		/** Standard headers data row implementation */
		HEADERS_DATA_ROW
	};
	
	/**
	 * Gets the implementation type of this headers data row.
	 * 
	 * @return The {@link HEADERS_DATA_ROW_TYPE} indicating the implementation type
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

	String getQuery();
	
	void setQuery(String query);
	
	Map<String, Object> flushRowToMap();
	
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
