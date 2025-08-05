package prerna.engine.impl.model;

public interface IFeedback {
	String getMessageId();
    String getFeedbackText();
    String getFeedbackDate();
    boolean getRating();
}