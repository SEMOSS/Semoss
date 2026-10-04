package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** The examples are real imported conversations, not invented model summaries. */
public class BrainGetTopicReviewEvidenceReactor extends AbstractCollaborationReactor {
	public BrainGetTopicReviewEvidenceReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "topicKey", "query", "offset", "limit" };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		Integer offset = getIntFromKeyOrCurRow("offset");
		Integer limit = getIntFromKeyOrCurRow("limit");
		if (revision == null) {
			throw new IllegalArgumentException("Pass the saved review revision");
		}
		return mapResult(BrainTopicReviewUtils.evidence(user, getString("reviewId"), revision,
				getString("topicKey"), getString("query"), offset == null ? 0 : offset, limit == null ? 20 : limit));
	}

	@Override
	public String getReactorDescription() {
		return "Reads owner-scoped imported conversation headers behind a draft topic, plus current links and a version for corrections; no body reads or writes";
	}
}
