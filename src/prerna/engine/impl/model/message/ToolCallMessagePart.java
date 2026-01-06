package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class ToolCallMessagePart extends MessagePart {


	@SerializedName("toolCall")
	private Map<String, Object> toolCall;



	public ToolCallMessagePart() {
		super(MessagePartType.TOOL_CALL);
	}



	public ToolCallMessagePart(Map<String, Object> toolCall) {
		this();
		this.toolCall = toolCall;
	}



	public Map<String, Object> getToolCall() {
		return toolCall;
	}


}
