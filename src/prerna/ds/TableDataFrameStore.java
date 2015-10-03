package prerna.ds;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;

public class TableDataFrameStore extends Hashtable<String, ITableDataFrame> {

	private static final Logger LOGGER = LogManager.getLogger(TableDataFrameStore.class.getName());
	private Map<String, Set<String>> sessionIdHash = new Hashtable<String, Set<String>>();
	
	/**
	 * If we serialize all table data frames currently held in memory
	 */
	private static final long serialVersionUID = 6873641582272949789L;
	
	/**
	 * Singleton for the class
	 */
	private static TableDataFrameStore store;
	
	/**
	 * Constructor for class
	 */
	private TableDataFrameStore() {
		// do nothing
	}

	public static TableDataFrameStore getInstance() {
		if(store == null) {
			store = new TableDataFrameStore();
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
	
	/**
	 * Returns a boolean true/false if data was successfully remove using the key
	 * @param key					The unique id for the data-frame
	 * @return						boolean true if the key was successful at removing data, false otherwise
	 */
	public boolean remove(String key) {
		ITableDataFrame data = super.remove(key);
		if(data != null) {
			return true;
		} else {
			return false;
		}
	}
	
	public void addToSessionHash(String sessionID, String tableID) {
		Set<String> tableIDs = null;
		
		if(sessionIdHash.containsKey(sessionID)) {
			tableIDs = sessionIdHash.get(sessionID);
			if(tableIDs == null) {
				tableIDs = new HashSet<String>();
			}
			tableIDs.add(tableID);
		} else {
			tableIDs = new HashSet<String>();
			tableIDs.add(tableID);
		}
		
		sessionIdHash.put(sessionID, tableIDs);
	}
	
	public boolean removeFromSessionHash(String sessionID, String tableID) {
		if(!sessionIdHash.containsKey(sessionID)) {
			return false;
		}
		Set<String> tableIDs = sessionIdHash.get(sessionID);
		if(tableIDs.contains(tableID)) {
			tableIDs.remove(tableID);
			return true;
		} 

		return false;
	}
	
	public Set<String> getTableIDsForSession(String sessionID) {
		return sessionIdHash.get(sessionID);
	}
}
