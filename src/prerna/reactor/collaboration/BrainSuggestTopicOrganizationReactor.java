package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Read-only grouping proposals using the owner's saved context and level of detail. */
public class BrainSuggestTopicOrganizationReactor extends AbstractCollaborationReactor {
	public BrainSuggestTopicOrganizationReactor() {
		this.keysToGet = new String[] { "reviewId", "revision" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		Integer revision = getIntFromKeyOrCurRow("revision");
		if (revision == null) throw new IllegalArgumentException("Pass the saved review revision");
		return mapResult(BrainTopicReviewUtils.suggestOrganization(getUser(), getString("reviewId"), revision));
	}

	@Override
	public String getReactorDescription() {
		return "Proposes flat topic groups using the saved owner's work context, broad/projects/detailed preference and permitted example headers. Returns complete topic-key coverage, explanations and open questions; no writes, tools or room messages";
	}
}
