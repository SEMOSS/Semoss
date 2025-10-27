package prerna.engine.api;

import java.util.Iterator;

/**
 * Core API interface for data processing connectors in the SEMOSS platform.
 * 
 * <p>This interface defines the fundamental contract for API connectors that handle
 * data retrieval, transformation, and processing operations. API implementations
 * typically support operations such as:</p>
 * <ul>
 *   <li><strong>Selectors:</strong> Specify which data fields or columns to retrieve</li>
 *   <li><strong>Filters:</strong> Apply conditions to limit the data returned</li>
 *   <li><strong>Joins:</strong> Combine data from multiple sources or tables</li>
 * </ul>
 * 
 * <p>The interface follows a pattern where parameters are configured via the
 * {@link #set(String, Object)} method, and processing is triggered via the
 * {@link #process()} method which returns an iterator for streaming results.</p>
 * 
 * @see {@link IHeadersDataRow} for the data row structure returned by processing
 * @author SEMOSS
 */
public interface IApi{
	
	/**
	 * Gets all parameter names required by this API connector.
	 * 
	 * <p>This method returns an array of parameter names that the connector
	 * needs to be configured with before processing can begin. These parameters
	 * typically include connection details, query specifications, and processing
	 * options that the connector will listen for or require.</p>
	 * 
	 * @return Array of parameter names required by this connector
	 */
	public String[] getParams();

	/**
	 * Sets a configuration parameter for this API connector.
	 * 
	 * <p>This method is used to configure the connector with the necessary
	 * parameters before processing. Parameters may include connection strings,
	 * query specifications, filter conditions, or other configuration data
	 * needed for the connector to operate properly.</p>
	 * 
	 * @param key The parameter name to set
	 * @param value The parameter value, which may be a simple value or array
	 */
	public void set(String key, Object value);
		
	/**
	 * Processes the configured API request and returns an iterator of results.
	 * 
	 * <p>This method executes the configured API operation using the parameters
	 * that have been set via {@link #set(String, Object)} and returns an iterator
	 * that can be used to stream through the results. The iterator provides
	 * {@link IHeadersDataRow} objects that contain both headers and data values.</p>
	 * 
	 * @return Iterator of data rows containing headers and values
	 * @see {@link IHeadersDataRow} for the structure of returned data rows
	 */
	public Iterator<IHeadersDataRow> process();		
	
}
