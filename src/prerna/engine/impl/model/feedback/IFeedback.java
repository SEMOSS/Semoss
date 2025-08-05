package prerna.engine.impl.model.feedback;

public interface IFeedback {
	String getMessageId();
    String getFeedbackText();
    String getFeedbackDate();
    boolean getRating();
}