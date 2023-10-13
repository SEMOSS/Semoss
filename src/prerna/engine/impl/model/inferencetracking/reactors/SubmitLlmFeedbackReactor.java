package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubmitLlmFeedbackReactor extends AbstractReactor {
	
    public SubmitLlmFeedbackReactor() {
        this.keysToGet = new String[] {"messageId", "feedbackText", "rating"};
        this.keyRequired = new int[] {1, 0, 1};
    }

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        String messageId = this.keyValue.get(this.keysToGet[0]);
        boolean output = ModelInferenceLogsUtils.userIsMessageAuthor(user.getPrimaryLoginToken().getId(), messageId);
        if (!output) {
        	throw new SemossPixelException("User is not the author of this message and cannot provide feedback");
        }
        String feedbackText = this.keyValue.get(this.keysToGet[1]);
        boolean rating = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]) + "");
        ModelInferenceLogsUtils.recordFeedback(messageId, feedbackText, rating);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("messageId")) {
			return "The unique identififer for the I/O betweeen a user and the LLM response";
		} else if (key.equals("feedbackText")) {
			return "Additional feedback in the form of text to decribe the issue/benefits of the response";
		} else if (key.equals("rating")) {
			return "true/false value to indicate if the reponse was helpful or not";
		} 
		return super.getDescriptionForKey(key);
	}
}
