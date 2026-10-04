package prerna.reactor.collaboration;

import java.util.Map;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Apply a typed, reversible change to the draft only. */
public class BrainChangeTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainChangeTopicReviewReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "operationId", "change" };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		Map<String, Object> change = getMapFromKeyOrCurRow("change");
		if (revision == null || change == null) {
			throw new IllegalArgumentException("Pass a saved review revision and a change map");
		}
		return mapResult(BrainTopicReviewUtils.change(user, getString("reviewId"), revision,
				getString("operationId"), change));
	}

	@Override
	public String getReactorDescription() {
		return "Revision-checked, idempotent topic-review draft correction. Changes: confirm/reject/move/also_link with topicKey, threadIds, preview versions and targetKey where needed; organize with reviewed groups and scopeVersion; undo with changeId. Real links and combinations are applied only by BrainApplyTopicReview";
	}
}
