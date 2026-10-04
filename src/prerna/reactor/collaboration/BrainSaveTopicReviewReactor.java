package prerna.reactor.collaboration;

import java.util.Map;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Save the editable draft against the last observed revision. */
public class BrainSaveTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainSaveTopicReviewReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "draft" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		Map<String, Object> draft = getMapFromKeyOrCurRow("draft");
		if (revision == null || revision < 1 || draft == null) {
			throw new IllegalArgumentException("Pass a saved review ID, positive revision and draft map");
		}
		return mapResult(BrainTopicReviewUtils.save(user, getString("reviewId"), revision, draft));
	}

	@Override
	public String getReactorDescription() {
		return "Saves an owner-scoped topic-review draft with revision checking; no topic profiles or thread assignments are applied";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
		case "reviewId" -> "The review ID returned by BrainStartTopicReview";
		case "revision" -> "Last observed saved draft revision; conflicting edits are rejected";
		case "draft" -> "topics list (maximum 100): key, id, name, description, short (empty uses name), keep and removedPeople. Source evidence is server-owned";
		default -> super.getDescriptionForKey(key);
		};
	}
}
