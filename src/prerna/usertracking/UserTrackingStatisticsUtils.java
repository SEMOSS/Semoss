package prerna.usertracking;

import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Precision;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.util.Constants;

public class UserTrackingStatisticsUtils {
	
	// words for point markdown and description
	private static double WFP_MD = 30.0;
	private static double WFP_DESC = 10.0;
	
	// max points for markdown and description and remaining keys
	private static double MP_MD = 5.0;
	private static double MP_DESC = 2.0;
	private static double MP_RK = 3.0;
	
	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static double calculateScore(String databaseId) {
		double score = calculateSecurityScore(databaseId);
		// round to 2 decimal places
		score = Precision.round(score, 2);
		return score;
	}
	
	private static double calculateSecurityScore(String databaseId) {
		double calc = 0.0;

		List<String> keys = SecurityEngineUtils.getAllMetakeys();
		Map<String, Object> keyVals = SecurityEngineUtils.getAggregateEngineMetadata(databaseId, keys, false);

		int keysAccounted = 0;
		if (keyVals.containsKey(Constants.MARKDOWN)) {
			Object md = keyVals.get(Constants.MARKDOWN);
			String databaseMarkdown = getStringFromObject(md);
			calc += calcScoreForString(databaseMarkdown, WFP_MD, MP_MD);
			keysAccounted++;
		}
		
		if (keyVals.containsKey(Constants.DESCRIPTION)) {
			String description = getStringFromObject(keyVals.get(Constants.DESCRIPTION));
			calc += calcScoreForString(description, WFP_DESC, MP_DESC);
			keysAccounted++;
		}
		
		calc += scoreRemainingKeys(keyVals, keysAccounted, keys);
		
		return calc;
	}
	
	private static double scoreRemainingKeys(Map<String, Object> keyVals, int keysAccounted, List<String> keys) {
		double sizeOfKeys = keyVals.keySet().size() - keysAccounted;
		double totalKeys = keys.size() - keysAccounted;
		double keyRatio = sizeOfKeys / totalKeys;
		keyRatio *= MP_RK;
		return Math.min(MP_RK, keyRatio);
	}

	private static double calcScoreForString(String s, double wfp, double mp) {
		if (s == null) {
			return 0.0;
		}
		double wordCount = s.split("\\s+").length;
		return Math.min(mp, wordCount / wfp);
	}

	private static String getStringFromObject(Object o) {
		String val;
		if (o instanceof List) {
			List<Object> list = (List<Object>) o;
			if (list.size() > 0) {
				val = (String) list.get(list.size() - 1);
			} else {
				val = null;
			}
		} else {
			val = (String) o;
		}
		return val;
	}

}
