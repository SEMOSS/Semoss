package prerna.ds;

import java.util.Hashtable;
import java.util.UUID;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;

public class BTreeStore extends Hashtable<String, ITableDataFrame> {

	private static final Logger LOGGER = LogManager.getLogger(BTreeStore.class.getName());

	/**
	 * If we serialize all table data frames currently held in memory
	 */
	private static final long serialVersionUID = 6873641582272949789L;
	
	/**
	 * Singleton for the class
	 */
	private static BTreeStore store;
	
	/**
	 * Constructor for class
	 */
	private BTreeStore() {
		// do nothing
	}

	public static BTreeStore getInstance() {
		if(store == null) {
			store = new BTreeStore();
		}
		return store; 
	}
	
	/**
	 * Adds a table to be kept in memory while returning a unique key to retrieve the data-frame
	 * @param data					The data-frame to kept in storage
	 * @return						The unique id for the data-frame
	 */
	public String put(ITableDataFrame data) {
		String uniqueID = UUID.randomUUID().toString();
		super.put(uniqueID, data);
		
		return uniqueID;
	}
	
}
