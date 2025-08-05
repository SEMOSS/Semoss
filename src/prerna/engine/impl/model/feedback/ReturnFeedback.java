package prerna.engine.impl.model.feedback;

public class ReturnFeedback implements IFeedback {
	
	private String messageId;
	private boolean rating;
	
	public ReturnFeedback(IFeedback input) {
		this.messageId = input.getMessageId();
		this.rating = input.getRating();
	}

	@Override
	public String getMessageId() {
		return this.messageId;
	}

	@Override
	public String getFeedbackText() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFeedbackDate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getRating() {
		return this.rating;
	}

}
