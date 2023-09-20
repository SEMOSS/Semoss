package prerna.om;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InsightStore extends Hashtable<String, Insight> {

	private static final Logger classLogger = LogManager.getLogger(InsightStore.class);
	
	private Map<String, Set<String>> sessionIdHash = new Hashtable<String, Set<String>>();
	
	// required for thick client
	public static Insight activeInsight = null;
	public static int idCount = 0;
	
	/**
	 * Singleton for the class
	 */
	private static InsightStore store;
	
	/**
	 * Constructor for class
	 */
	private InsightStore() {
		// do nothing
	}

	/**
	 * Returns the single insight store instance in the application
	 * @return
	 */
	public static InsightStore getInstance() {
		if(store == null) {
			store = new InsightStore();
		}
		return store; 
	}
	
	/**
	 * Adds an insight to be kept in memory while returning a unique key to retrieve the insight
	 * @param data					The insight to kept in storage
	 * @return						The unique id for the insight
	 */
	public String put(Insight data) {
		String uniqueID = data.getInsightId();
		super.put(uniqueID, data);
		return uniqueID;
	}
	
	/**
	 * Returns a boolean true/false if insight was successfully remove using the key
	 * @param key					The unique id for the data-frame
	 * @return						boolean true if the key was successful at removing data, false otherwise
	 */
	public boolean remove(String key) {
		Insight data = super.remove(key);
		if(activeInsight != null && activeInsight.getInsightId().equalsIgnoreCase(key)) {
			activeInsight = null;
		}
		
		if(data != null) {
			return true;
		} else {
			return false;
		}
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
	
	public boolean removeFromSessionHash(String sessionId, String insightId) {
		if(!sessionIdHash.containsKey(sessionId)) {
			return false;
		}
		Set<String> insightIDs = sessionIdHash.get(sessionId);
		if(insightIDs.contains(insightId)) {
			insightIDs.remove(insightId);
			return true;
		} 

		return false;
	}
	
	public Set<String> getInsightIDsForSession(String sessionId) {
		return sessionIdHash.get(sessionId);
	}
	
	public Insight findInsightInStore(String engineName, String rdbmsId) {
		Insight retIn = null;
		INSIGHT_LOOP : for(String insightKey : this.keySet()) {
			Insight in = this.get(insightKey);
			String inEngineName = in.getProjectId();
			String inRdbmsId = in.getRdbmsId();
			if(engineName.equals(inEngineName) && rdbmsId.equals(inRdbmsId)) {
				retIn = in;
				break INSIGHT_LOOP;
			}
		}
		return retIn;
	}
	
	
	////////////////CODE FOR THICK CLIENT///////////////////////////
	public void setActiveInsight(Insight insight) {
		activeInsight = insight;
	}
	
	public void setActiveInsight(String insightId) {
		activeInsight = this.get(insightId);
	}

	public Insight getActiveInsight() {
		return activeInsight;
	}
	
	public Set<String> getAllInsights() {
		return this.keySet();
	}
	
	public static int getIdCount(){
		return idCount++;
	}
	
}
