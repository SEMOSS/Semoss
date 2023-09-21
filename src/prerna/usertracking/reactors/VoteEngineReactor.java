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

public class VoteEngineReactor extends AbstractReactor {

	public VoteEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.VOTE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null || (engineId=engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Engine Id cannot be empty or null");
		}

		Integer vote = Integer.valueOf(this.keyValue.get(this.keysToGet[1]));
		if (vote == null) {
			throw new IllegalArgumentException("Vote cannot be empty or null");
		}

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine does not exist or cannot be viewed by user.");
		}

		List<Pair<String, String>> creds = User.getUserIdAndType(this.insight.getUser());
		
		UserCatalogVoteUtils.vote(creds, engineId, vote);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully voted for engine " + engineId));
		return noun;
	}

}