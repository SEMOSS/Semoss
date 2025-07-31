package prerna.engine.impl.model;

public class Feedback {
	private String transactionid;
	private String messageType;
	private String feedbackText;
	private String feedbackDate;
	private String rating;
	
	public Feedback(String messageId, String messageType, String feedbackText, String feedbackDate, String rating) {
		this.transactionid = messageId;
		this.messageType = messageType;
		this.feedbackText = feedbackText;
		this.feedbackDate = feedbackDate;
		this.rating = rating;
	}

	public String getTransactionId() {
		return transactionid;
	}

	public void setMessageId(String transactionId) {
		this.transactionid = transactionId;
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

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}
}
