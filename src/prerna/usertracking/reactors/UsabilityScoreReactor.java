package prerna.usertracking.reactors;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserTrackingStatisticsUtils;
import prerna.util.Utility;

public class UsabilityScoreReactor extends AbstractReactor {
	
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
		double score = UserTrackingStatisticsUtils.calculateScore(databaseId);
		return new NounMetadata(score, PixelDataType.CONST_DECIMAL);
	}

	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	private String checkDatabaseId(String databaseId) {
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to database");
		}
		return databaseId;
	}

}
