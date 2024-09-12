package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveLlmFeedbackReactor extends AbstractReactor {
	
    public RemoveLlmFeedbackReactor() {
        this.keysToGet = new String[] {"messageId"};
        this.keyRequired = new int[] {1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();

        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }

        String messageId = this.keyValue.get(this.keysToGet[0]);

        if (!ModelInferenceLogsUtils.userIsMessageAuthor(user.getPrimaryLoginToken().getId(), messageId)) {
            throw new SemossPixelException("User is not the author of this message and cannot remove feedback");
        }

        ModelInferenceLogsUtils.removeFeedback(messageId);

        return new NounMetadata(true, PixelDataType.BOOLEAN);
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if ("messageId".equals(key)) {
            return "The unique identifier for the I/O between a user and the LLM response";
        }
        return super.getDescriptionForKey(key);
    }
}
