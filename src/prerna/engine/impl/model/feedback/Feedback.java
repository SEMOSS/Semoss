package prerna.engine.impl.model.feedback;

public class Feedback implements IFeedback {
	
	private String messageId;
	private boolean rating;
	
	public Feedback(IFeedback input) {
		this.messageId = input.getMessageId();
		this.rating = input.getRating();
	}

	@Override
	public String getMessageId() {
		return this.messageId;
	}

	@Override
	public boolean getRating() {
		return this.rating;
	}

}
