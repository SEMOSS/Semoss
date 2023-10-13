package prerna.usertracking.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class GetUserDatabaseVotesReactor extends AbstractReactor {

	public GetUserDatabaseVotesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null) {
			throw new IllegalArgumentException("Database Id cannot be null.");
		}

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database cannot be viewed by user.");
		}

		// get the primary login in case of different upvote and downvote for different
		// authproviders.
		List<Pair<String, String>> creds = User.getPrimaryUserIdAndType(this.insight.getUser());

		if (creds.size() != 1) {
			throw new IllegalArgumentException("Could not get primary login details.");
		}
		Pair<String, String> primaryCredentials = creds.get(0);

		Map<Pair<String, String>, Integer> userVotes = UserCatalogVoteUtils.getVote(creds, databaseId);

		int userVote = 0;
		if (userVotes.containsKey(primaryCredentials)) {
			userVote = userVotes.get(primaryCredentials);
		}

		int total = UserCatalogVoteUtils.getAllVotes(databaseId);

		Map<String, Integer> votes = new HashMap<>();
		votes.put("userVote", userVote);
		votes.put("total", total);

		return new NounMetadata(votes, PixelDataType.MAP);
	}

}