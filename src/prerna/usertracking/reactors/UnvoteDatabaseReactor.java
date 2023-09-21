package prerna.usertracking.reactors;

import java.util.List;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Utility;

@Deprecated
public class UnvoteDatabaseReactor extends AbstractReactor {
	
	public UnvoteDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		List<Pair<String, String>> creds = User.getUserIdAndType(this.insight.getUser());
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null) {
			throw new IllegalArgumentException("Database Id cannot be null.");
		}
		
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database cannot be viewed by user.");
		}
		
		UserCatalogVoteUtils.delete(creds, databaseId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully unvoted for catalog"));
		return noun;
	}

}