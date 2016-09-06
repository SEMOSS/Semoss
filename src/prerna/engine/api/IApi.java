package prerna.engine.api;

import java.util.Iterator;

public interface IApi{

	// the methods are not cohesive
	// but basically it has
	// SELECTORS
	// FILTERS
	// JOINS
	
	// gets all the hashtable with things needed by this connector
	public String[] getParams(); // this can be the same as listeners we are listening to

	// bunch of set data goes here
	public void set(String key, Object value); // I wonder if the value can be string [] - which is the actual name and value
		
	// process and get the iterator
	// hopefully, the iterator is a map of selectors
	public Iterator<IHeadersDataRow> process();		
	
}
