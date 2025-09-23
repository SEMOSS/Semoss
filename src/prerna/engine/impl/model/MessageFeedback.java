package prerna.engine.impl.model;

import prerna.date.SemossDate;
import prerna.engine.impl.model.message.MessageType;
import prerna.util.Utility;

public class MessageFeedback {
	private String messageId;
	private MessageType messageType;
	private String feedbackText;
	private SemossDate feedbackDate;
	private boolean rating;
	
	public MessageFeedback(String messageId, MessageType messageType, String feedbackText, SemossDate feedbackDate, Boolean rating) {
		this.messageId = messageId;
		this.messageType = messageType;
		this.feedbackText = feedbackText;
		this.feedbackDate = feedbackDate;
		this.rating = rating.booleanValue();
	}
	
	public MessageFeedback(String messageId, MessageType messageType, String feedbackText, Boolean rating) {
		this.messageId = messageId;
		this.messageType = messageType;
		this.feedbackText = feedbackText;
		this.feedbackDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
		this.rating = rating.booleanValue();
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

	public SemossDate getFeedbackDate() {
		return feedbackDate;
	}

	public void setFeedbackDate(SemossDate feedbackDate) {
		this.feedbackDate = feedbackDate;
	}

	public boolean getRating() {
		return rating;
	}

	public void setRating(boolean rating) {
		this.rating = rating;
	}
}
