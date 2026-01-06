package prerna.engine.impl.model.message;

import com.google.gson.annotations.SerializedName;

public class TextMessagePart extends MessagePart {

	@SerializedName("text")
	private String text;

	/**
	 * Optional UI-only text (legacy inputUIPrompt) when {@code text} differs.
	 */
	@SerializedName("uiText")
	private String uiText;

	public TextMessagePart() {
		super(MessagePartType.TEXT);
	}

	public TextMessagePart(String text) {
		this();
		this.text = text;
		this.uiText = text;
	}

	public TextMessagePart(String text, String uiText) {
		this();
		this.text = text;
		this.uiText = (uiText == null || uiText.isEmpty()) ? text : uiText;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
		if (uiText == null || uiText.isEmpty()) {
			uiText = text;
		}
	}

	public String getUiText() {
		return (uiText == null || uiText.isEmpty()) ? text : uiText;
	}

	public void setUiText(String uiText) {
		this.uiText = (uiText == null || uiText.isEmpty()) ? text : uiText;
	}
}
