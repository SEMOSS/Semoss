package prerna.cache;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class FileStore extends Hashtable<String, String> {

	private Map<String, Set<String>> sessionIdHash = new Hashtable<String, Set<String>>();
	
	/**
	 * Singleton for the class
	 */
	private static FileStore store;
	
	/**
	 * Constructor for class
	 */
	private FileStore() {
		// do nothing
	}

	/**
	 * Returns the single insight store instance in the application
	 * @return
	 */
	public static FileStore getInstance() {
		if(store == null) {
			store = new FileStore();
		}
		return store; 
	}
	
	/**
	 * Adds an insight to be kept in memory while returning a unique key to retrieve the insight
	 * @param data					The insight to kept in storage
	 * @return						The unique id for the insight
	 */
	public String put(String fileLoc) {
		String uniqueID = UUID.randomUUID().toString();
		super.put(uniqueID, fileLoc);
		uniqueID.replace("-", "_");
		return uniqueID;
	}
	
	public void addToSessionHash(String sessionID, String insightID) {
		Set<String> insightIDs = null;
		
		if(sessionIdHash.containsKey(sessionID)) {
			insightIDs = sessionIdHash.get(sessionID);
			if(insightIDs == null) {
				insightIDs = new HashSet<String>();
			}
			insightIDs.add(insightID);
		} else {
			insightIDs = new HashSet<String>();
			insightIDs.add(insightID);
		}
		
		sessionIdHash.put(sessionID, insightIDs);
	}
	
	public boolean removeFromSessionHash(String sessionID, String insightID) {
		if(!sessionIdHash.containsKey(sessionID)) {
			return false;
		}
		Set<String> insightIDs = sessionIdHash.get(sessionID);
		if(insightIDs.contains(insightID)) {
			insightIDs.remove(insightID);
			return true;
		} 

		return false;
	}
	
	public Set<String> getInsightIDsForSession(String sessionID) {
		return sessionIdHash.get(sessionID);
	}
}
