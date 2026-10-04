package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Apply a saved revision atomically and start/resume its own filing job. */
public class BrainApplyTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainApplyTopicReviewReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "retryFiling" };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		if (revision == null || revision < 1) {
			throw new IllegalArgumentException("Pass a saved review ID and positive revision");
		}
		return mapResult(BrainTopicReviewUtils.apply(user, getString("reviewId"), revision,
				Boolean.TRUE.equals(getBoolean("retryFiling"))));
	}

	@Override
	public String getReactorDescription() {
		return "Atomically applies a saved topic-review revision, returning saved names/descriptions/IDs and its exact filing job. Repeating the same revision does not recreate topics. Zero kept topics require no filing job";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
		case "reviewId" -> "The owner's review ID";
		case "revision" -> "Saved draft revision to apply; stale revisions are rejected";
		case "retryFiling" -> "true to retry failed or partially completed filing without recreating topics; default false";
		default -> super.getDescriptionForKey(key);
		};
	}
}
