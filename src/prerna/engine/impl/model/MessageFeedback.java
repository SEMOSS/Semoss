package prerna.engine.impl.model;

import prerna.engine.impl.model.message.MessageType;
import prerna.util.Utility;

public class MessageFeedback {
	private String messageId;
	private MessageType messageType;
	private String feedbackText;
	private java.sql.Timestamp feedbackDate;
	private boolean rating;
	
	public MessageFeedback(String messageId, MessageType messageType, String feedbackText, java.sql.Timestamp feedbackDate, boolean rating) {
		this.messageId = messageId;
		this.messageType = messageType;
		this.feedbackText = feedbackText;
		this.feedbackDate = feedbackDate;
		this.rating = rating;
	}
	
	public MessageFeedback(String messageId, MessageType messageType, String feedbackText, boolean rating) {
		this.messageId = messageId;
		this.messageType = messageType;
		this.feedbackText = feedbackText;
		this.feedbackDate = Utility.getCurrentSqlTimestampUTC();
		this.rating = rating;
	}
	
	public MessageFeedback() {
		
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}

	public String getFeedbackText() {
		return feedbackText;
	}

	public void setFeedbackText(String feedbackText) {
		this.feedbackText = feedbackText;
	}

	public java.sql.Timestamp getFeedbackDate() {
		return feedbackDate;
	}

	public void setFeedbackDate(java.sql.Timestamp feedbackDate) {
		this.feedbackDate = feedbackDate;
	}

	public boolean getRating() {
		return rating;
	}

	public void setRating(boolean rating) {
		this.rating = rating;
	}
}
