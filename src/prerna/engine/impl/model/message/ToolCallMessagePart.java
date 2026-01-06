package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class ToolCallMessagePart extends MessagePart {

	@SerializedName("toolCalls")
	private List<Map<String, Object>> toolCalls = new ArrayList<>();

	public ToolCallMessagePart() {
		super(MessagePartType.TOOL_CALL);
	}

	public ToolCallMessagePart(List<Map<String, Object>> toolCalls) {
		this();
		if (toolCalls != null) {
			this.toolCalls = new ArrayList<>(toolCalls);
		}
	}

	public List<Map<String, Object>> getToolCalls() {
		return new ArrayList<>(toolCalls);
	}

	public void setToolCalls(List<Map<String, Object>> toolCalls) {
		this.toolCalls = (toolCalls == null) ? new ArrayList<>() : new ArrayList<>(toolCalls);
	}
}

