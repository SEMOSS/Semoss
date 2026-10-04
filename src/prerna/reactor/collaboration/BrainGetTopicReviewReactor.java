package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Read the signed-in owner's saved topic review without generating or applying topics. */
public class BrainGetTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainGetTopicReviewReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		return mapResult(BrainTopicReviewUtils.get(getUser()));
	}

	@Override
	public String getReactorDescription() {
		return "Reads the owner's resumable topic-review draft, revision, apply receipt and exact filing job; review is null before setup";
	}
}
