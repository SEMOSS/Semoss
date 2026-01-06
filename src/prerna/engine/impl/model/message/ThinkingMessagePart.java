package prerna.engine.impl.model.message;

import com.google.gson.annotations.SerializedName;

public class ThinkingMessagePart extends MessagePart {

	@SerializedName("thinking")
	private String thinking;

	public ThinkingMessagePart() {
		super(MessagePartType.THINKING);
	}

	public ThinkingMessagePart(String thinking) {
		this();
		this.thinking = thinking;
	}

	public String getThinking() {
		return thinking;
	}

	public void setThinking(String thinking) {
		this.thinking = thinking;
	}
}

