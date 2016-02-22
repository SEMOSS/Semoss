package prerna.engine.api;

import java.util.Hashtable;
import java.util.Iterator;

public interface IApi{

	// the methods are not cohesive
	// but basically it has
	// SELECTORS
	// FILTERS
	// JOINS
	
	// gets all the hashtable with things needed by this connector
	public String[] getParams();
	
	// bunch of set data goes here
	public void set(String key, Object value);
		
	// process and get the iterator
	// hopefully, the iterator is a map of selectors
	public Iterator process();		
}
