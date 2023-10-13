package prerna.usertracking.reactors;

import java.util.List;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Utility;

@Deprecated
public class VoteDatabaseReactor extends AbstractReactor {

	public VoteDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.VOTE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null) {
			throw new IllegalArgumentException("Database Id is null");
		}

		Integer vote = Integer.valueOf(this.keyValue.get(this.keysToGet[1]));
		if (vote == null) {
			throw new IllegalArgumentException("Vote is null");
		}

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database cannot be viewed by user.");
		}

		List<Pair<String, String>> creds = User.getUserIdAndType(this.insight.getUser());
		
		UserCatalogVoteUtils.vote(creds, databaseId, vote);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully voted for catalog"));
		return noun;
	}

}