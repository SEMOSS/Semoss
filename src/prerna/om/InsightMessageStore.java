package prerna.om;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class InsightMessageStore extends Hashtable<String, List<String>> {

	/**
	 * Singleton for the class
	 */
	private static InsightMessageStore store;
	
	/**
	 * Constructor for class
	 */
	private InsightMessageStore() {
		// do nothing
	}

	/**
	 * Returns the single insight store instance in the application
	 * @return
	 */
	public static InsightMessageStore getInstance() {
		if(store == null) {
			store = new InsightMessageStore();
		}
		return store; 
	}
	
	/**
	 * Adds an insight to be kept in memory while returning a unique key to retrieve the insight
	 * @param data					The insight to kept in storage
	 * @return						The unique id for the insight
	 */
	public void put(String insightId) {
		super.put(insightId, new Vector<String>());
	}
	
	/**
	 * Returns a boolean true/false if insight was successfully remove using the key
	 * @param key					The unique id for the data-frame
	 * @return						boolean true if the key was successful at removing data, false otherwise
	 */
	public boolean remove(String key) {
		List<String> messages = super.remove(key);
		if(messages != null) {
			return true;
		} else {
			return false;
		}
	}
	
	public void addNewMessage(String insightId, String newMessage) {
		List<String> messages = get(insightId);
		messages.add(newMessage);
	}
	
	public List<String> getAllMessages(String insightId) {
		List<String> messages = super.get(insightId);
		// reset to a new list so we do not resend the same messages
		super.put(insightId, new Vector<String>());
		return messages;
	}
}
