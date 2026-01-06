package prerna.engine.impl.model.message;

import com.google.gson.annotations.SerializedName;

public class SystemMessagePart extends MessagePart {

	@SerializedName("prompt")
	private String prompt;

	public SystemMessagePart() {
		super(MessagePartType.SYSTEM);
	}

	public SystemMessagePart(String prompt) {
		this();
		this.prompt = prompt;
	}

	public String getPrompt() {
		return prompt;
	}

	public void setPrompt(String prompt) {
		this.prompt = prompt;
	}
}

