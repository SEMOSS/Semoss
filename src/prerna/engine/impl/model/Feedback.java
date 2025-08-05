package prerna.engine.impl.model;

public class Feedback implements IFeedback {
	private String messageId;
	private String messageType;
	private String feedbackText;
	private String feedbackDate;
	private boolean rating;
	
	public Feedback(String messageId, String messageType, String feedbackText, String feedbackDate, boolean rating) {
		this.messageId = messageId;
		this.messageType = messageType;
		this.feedbackText = feedbackText;
		this.feedbackDate = feedbackDate;
		this.rating = rating;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getMessageType() {
		return messageType;
	}

	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}

	public String getFeedbackText() {
		return feedbackText;
	}

	public void setFeedbackText(String feedbackText) {
		this.feedbackText = feedbackText;
	}

	public String getFeedbackDate() {
		return feedbackDate;
	}

	public void setFeedbackDate(String feedbackDate) {
		this.feedbackDate = feedbackDate;
	}

	public boolean getRating() {
		return rating;
	}

	public void setRating(boolean rating) {
		this.rating = rating;
	}
}
