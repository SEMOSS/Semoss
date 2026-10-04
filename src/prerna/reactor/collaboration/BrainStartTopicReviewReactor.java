package prerna.reactor.collaboration;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Initialize once, then resume the owner's topic review across navigation and sessions. */
public class BrainStartTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainStartTopicReviewReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		return mapResult(BrainTopicReviewUtils.start(getUser()));
	}

	@Override
	public String getReactorDescription() {
		return "Resumes the owner's saved topic review or initializes it once from topic suggestions. Does not regenerate an existing draft. Model failure still allows manual topics";
	}
}
