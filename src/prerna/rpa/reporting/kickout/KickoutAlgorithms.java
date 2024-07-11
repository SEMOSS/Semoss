package prerna.rpa.reporting.kickout;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rpa.db.jedis.JedisStore;
import redis.clients.jedis.Jedis;

public class KickoutAlgorithms {

	private static final Logger LOGGER = LogManager.getLogger(KickoutAlgorithms.class.getName());

	private final String prefix;
	private final String timestampsKey;
	private final String referenceKey;
	private final String openRecordsKey;
	
	private final SimpleDateFormat timestampFormatter;
	
	public KickoutAlgorithms(String prefix) {
		this.prefix = prefix;
		timestampsKey = KickoutJedisKeys.timestampsKey(prefix);
		referenceKey = KickoutJedisKeys.referenceKey(prefix);
		timestampFormatter = KickoutJedisKeys.timestampFormatter();
		openRecordsKey = KickoutJedisKeys.openRecordsKey(prefix);
	}
	
	public String runAlgorithm(KickoutAlgorithm algorithm, int d) throws KickoutAlgorithmException {
		
		// Return the name of the Jedis hash that is updated by the algorithm
		String jedisHash = null;
		switch (algorithm) {
		case OPEN_RECORDS:
			try {
				runOpenRecords(d);
				jedisHash = KickoutJedisKeys.openRecordsKey(prefix);
			} catch (ParseException e) {
				throw new KickoutAlgorithmException("Failed to run 'open records' algorithm for " + prefix + ".", e);
			}
			break;
		}
		return jedisHash;
	}
	
	public void runOpenRecords(int d) throws ParseException {
		LOGGER.info("Running 'open records' algorithm for " + prefix + ".");

//		// This algorithm returns a map of id, csv record
//		Map<String, String> data = new HashMap<>();
//		
//		// convert d in days to ms
//		long dms = d * 24 * 60 * 60 * 1000L;
//		
//		// T is a reverse-sorted list of timestamps
//		List<String> T = getSortedTimestamps();
//		if (T.isEmpty()) return;
//		Collections.reverse(T);
//		
//		// T0 is a map of id, first observed time
//		Map<String, Long> T0 = new HashMap<>();
//		
//		// T1 is a map of id, last observed time
//		Map<String, Long> T1 = new HashMap<>();
//		
//		// X is the set of expired ids
//		Set<String> X = new HashSet<>();
//				
//		// Iterate backwards through time to determine all open records
//		int i = 0;
//		while (i < T.size()) {
//			LOGGER.info("Crunching " + T.get(i) + ".");
//			
//			// t is the time corresponding to the ith element of T
//			long t = timestampFormatter.parse(T.get(i)).getTime();
//			
//			// To avoid unnecessary computation,
//			// break when there is no longer any possibility of updating t0 for any id.
//			// This occurs when the minimum of T0 >= dms.
//			if (!T0.isEmpty()) {
//				long minT0 = Collections.min(T0.values());
//				if ((minT0 - t) >= dms) {
//					LOGGER.info("There are no open records before " + getFormattedTimestamp(minT0) + ".");
//					break;
//				}
//			}
//			
//			// ID is the set of ids for t 
//			Set<String> ID = getIdsForReportTimestamp(getFormattedTimestamp(t));
//			
//			// When beyond d, only retain ids that have already been observed
//			if (i > d) {
//				ID.retainAll(T0.keySet());
//			}
//			
//			// Only process the ids that are not expired
//			ID.removeAll(X);
//						
//			// Loop through each id in ID and update T0, T1, X accordingly
//			for (String id : ID) {
//
//				// Add the id to T0 and T1 the first time the id is observed
//				if (!T0.containsKey(id)) {
//					T0.put(id, t);
//					T1.put(id, t);
//				}
//				
//				// Update t0 else add it to the ignore list
//				if ((T0.get(id) - t) <= dms) {
//					T0.put(id, t);
//				} else {
//					X.add(id);
//				}
//			}
//			
//			// Increment the pointer
//			i++;
//		}
//		
//		// Validate that the algorithm ran properly
//		boolean check1 = T0.keySet().containsAll(T1.keySet());
//		boolean check2 = T1.keySet().containsAll(T0.keySet());
//		if (check1 && check2) {
//			LOGGER.info("'Open records' algorithm ran properly.");
//		} else {
//			LOGGER.error("'Open records' algorithm did not run properly.");
//			return;
//		}
//
//		// Want data in the format id, csv data, t0, t1
//		Set<String> openIDs = T0.keySet();
//		try (Jedis jedis = JedisStore.getInstance().getResource()) {
//			for (String id : openIDs) {
//				String rawCsvData = jedis.hget(referenceKey, id);
//				String t0 = getFormattedTimestamp(T0.get(id));
//				String t1 = getFormattedTimestamp(T1.get(id));
//				StringBuilder csvData = new StringBuilder();
//				csvData.append(rawCsvData);
//				csvData.append(",");
//				csvData.append(t0);
//				csvData.append(",");
//				csvData.append(t1);
//				data.put(id, csvData.toString());
//			}
//			
//			// Re-populate the open records hash in jedis
//			jedis.del(openRecordsKey);
//			for (Entry<String, String> entry : data.entrySet()) {
//				jedis.hset(openRecordsKey, entry.getKey(), entry.getValue());
//			}
//		}
	}
	
//	private String getFormattedTimestamp(long t) {
//		return timestampFormatter.format(new Date(t));
//	}
//	
//	private List<String> getSortedTimestamps() {
//		try (Jedis jedis = JedisStore.getInstance().getResource()) {
//			Set<String> timestamps = jedis.smembers(timestampsKey);
//			List<String> sortedTimestamps = new ArrayList<>(timestamps);
//			Collections.sort(sortedTimestamps);
//			return sortedTimestamps;
//		}
//	}
//	
//	private Set<String> getIdsForReportTimestamp(String reportTimestamp) {
//		try (Jedis jedis = JedisStore.getInstance().getResource()) {
//			return jedis.smembers(KickoutJedisKeys.reportKey(prefix, reportTimestamp));
//		}
//	}
	
}
