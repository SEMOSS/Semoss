package prerna.usertracking.reactors;

import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Precision;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class UsabilityScoreReactor extends AbstractReactor {
	
	// words for point markdown and description
	private double WFP_MD = 30.0;
	private double WFP_DESC = 10.0;
	
	// max points for markdown and description and remaining keys
	private double MP_MD = 5.0;
	private double MP_DESC = 2.0;
	private double MP_RK = 3.0;
	
	public UsabilityScoreReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		databaseId = checkDatabaseId(databaseId);
		
		
		double score = 0.0;
		
		if (AbstractSecurityUtils.securityEnabled()) {
			score += calculateSecurityScore(databaseId);
		} else {
			score += calculateScore(databaseId);
		}

		// round to 2 decimal places
		score = Precision.round(score, 2);

		return new NounMetadata(score, PixelDataType.CONST_DECIMAL);
	}
	
	private double calculateSecurityScore(String databaseId) {
		double calc = 0.0;

		List<String> keys = SecurityEngineUtils.getAllMetakeys();
		Map<String, Object> keyVals = SecurityEngineUtils.getAggregateDatabaseMetadata(databaseId, keys, false);

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
	
	private double scoreRemainingKeys(Map<String, Object> keyVals, int keysAccounted, List<String> keys) {
		double sizeOfKeys = keyVals.keySet().size() - keysAccounted;
		double totalKeys = keys.size() - keysAccounted;
		double keyRatio = sizeOfKeys / totalKeys;
		keyRatio *= MP_RK;
		return Math.min(MP_RK, keyRatio);
	}

	private double calcScoreForString(String s, double wfp, double mp) {
		if (s == null) {
			return 0.0;
		}
		double wordCount = s.split("\\s+").length;
		return Math.min(mp, wordCount / wfp);
	}

	// TODO: figure this out
	private double calculateScore(String databaseId) {
		double calc = 0.0;
		return 0;
	}
	
	private String getStringFromObject(Object o) {
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
	

	private String checkDatabaseId(String databaseId) {
		if(AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityEngineUtils.userCanViewDatabase(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to database");
			}
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			if(!MasterDatabaseUtility.getAllDatabaseIds().contains(databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist");
			}
		}
		return databaseId;
	}

}
