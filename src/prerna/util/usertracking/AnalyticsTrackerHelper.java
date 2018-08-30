package prerna.util.usertracking;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class AnalyticsTrackerHelper {

	private AnalyticsTrackerHelper() {
		
	}

	/**
	 * Flush a nounstore into a map key-value
	 * @param store
	 * @param keysToGet
	 * @return
	 */
	public static Map<String, List<String>> getHashInputs(NounStore store, String[] keysToGet) {
		Map<String, List<String>> keyValues = new HashMap<String, List<String>>();
		for(String key : keysToGet) {
			GenRowStruct grs = store.getNoun(key);
			if(grs == null) {
				continue;
			}
			int size = grs.size();
			List<String> values = new Vector<String>();
			for(int i = 0; i < size; i++) {
				values.add(grs.get(i) + "");
			}
			keyValues.put(key, values);
		}
		return keyValues;
	}
	
}
