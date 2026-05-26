/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.om;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InsightStore extends ConcurrentHashMap<String, Insight> {

	private Map<String, Set<String>> sessionIdHash = new ConcurrentHashMap<String, Set<String>>();

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
	 * 
	 * @return
	 */
	public static InsightStore getInstance() {
		if (store == null) {
			store = new InsightStore();
		}
		return store;
	}

	/**
	 * Adds an insight to be kept in memory while returning a unique key to retrieve
	 * the insight
	 * 
	 * @param data The insight to kept in storage
	 * @return The unique id for the insight
	 */
	public String put(Insight data) {
		String uniqueID = data.getInsightId();
		super.put(uniqueID, data);
		return uniqueID;
	}

	/**
	 * Returns a boolean true/false if insight was successfully remove using the key
	 * 
	 * @param key The unique id for the data-frame
	 * @return boolean true if the key was successful at removing data, false
	 *         otherwise
	 */
	public boolean remove(String key) {
		Insight data = super.remove(key);
		if (activeInsight != null && activeInsight.getInsightId().equalsIgnoreCase(key)) {
			activeInsight = null;
		}

		if (data != null) {
			return true;
		} else {
			return false;
		}
	}

	public void addToSessionHash(String sessionID, String insightID) {
		Set<String> insightIDs = null;

		if (sessionIdHash.containsKey(sessionID)) {
			insightIDs = sessionIdHash.get(sessionID);
			if (insightIDs == null) {
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
		if (!sessionIdHash.containsKey(sessionId)) {
			return false;
		}
		Set<String> insightIDs = sessionIdHash.get(sessionId);
		if (insightIDs.contains(insightId)) {
			insightIDs.remove(insightId);
			return true;
		}

		return false;
	}

	public Set<String> getInsightIDsForSession(String sessionId) {
		return sessionIdHash.get(sessionId);
	}

	public void clearSession(String sessionId) {
		if (sessionIdHash.containsKey(sessionId)) {
			sessionIdHash.remove(sessionId);
		}
	}

	public Insight findInsightInStore(String engineName, String rdbmsId) {
		Insight retIn = null;
		INSIGHT_LOOP: for (String insightKey : this.keySet()) {
			Insight in = this.get(insightKey);
			String inEngineName = in.getProjectId();
			String inRdbmsId = in.getRdbmsId();
			if (engineName.equals(inEngineName) && rdbmsId.equals(inRdbmsId)) {
				retIn = in;
				break INSIGHT_LOOP;
			}
		}
		return retIn;
	}

	public Set<String> getAllInsights() {
		return this.keySet();
	}

	//////////////// CODE FOR THICK CLIENT///////////////////////////
	public void setActiveInsight(Insight insight) {
		activeInsight = insight;
	}

	public void setActiveInsight(String insightId) {
		activeInsight = this.get(insightId);
	}

	public Insight getActiveInsight() {
		return activeInsight;
	}

	public static int getIdCount() {
		return idCount++;
	}

}
