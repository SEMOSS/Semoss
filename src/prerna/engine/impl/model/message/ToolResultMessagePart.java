package prerna.engine.impl.model.message;

import com.google.gson.annotations.SerializedName;

public class ToolResultMessagePart extends MessagePart {

	@SerializedName("toolResult")
	private ToolResultPart toolResult;

	public ToolResultMessagePart() {
		super(MessagePartType.TOOL_RESULT);
	}

	public ToolResultMessagePart(ToolResultPart toolResult) {
		this();
		this.toolResult = toolResult;
	}

	public ToolResultPart getToolResult() {
		return toolResult;
	}

	public void setToolResult(ToolResultPart toolResult) {
		this.toolResult = toolResult;
	}
}

