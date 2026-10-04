package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Exact impact preview shared by direct combination controls and assistant proposals. */
public class BrainPreviewTopicOrganizationReactor extends AbstractCollaborationReactor {
	public BrainPreviewTopicOrganizationReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "proposal" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		Integer revision = getIntFromKeyOrCurRow("revision");
		var proposal = getMapFromKeyOrCurRow("proposal");
		if (revision == null || proposal == null) throw new IllegalArgumentException("Pass the saved review revision and a proposal with groups");
		return mapResult(BrainTopicReviewUtils.previewOrganization(getUser(), getString("reviewId"), revision, proposal.get("groups")));
	}

	@Override
	public String getReactorDescription() {
		return "Previews a topic grouping's retained profiles, contributing topics, conversation examples and counts of saved links/people/notes/rules/Work references. Accept its scopeVersion through BrainChangeTopicReview organize; final apply transfers only reviewed scopes";
	}
}
