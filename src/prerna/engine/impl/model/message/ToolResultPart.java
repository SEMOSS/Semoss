package prerna.engine.impl.model.message;

import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class ToolResultPart {

	@SerializedName("toolCallId")
	private String toolCallId;

	@SerializedName("toolName")
	private String toolName;

	@SerializedName("output")
	private String output;

	@SerializedName("toolParameterValues")
	private Map<String, Object> toolParameterValues;

	@SerializedName("toolStatus")
	private String toolStatus;

	public ToolResultPart() {
	}

	public ToolResultPart(String toolCallId, String toolName, String output, Map<String, Object> toolParameterValues,
			String toolStatus) {
		this.toolCallId = toolCallId;
		this.toolName = toolName;
		this.output = output;
		this.toolParameterValues = toolParameterValues;
		this.toolStatus = toolStatus;
	}

	public String getToolCallId() {
		return toolCallId;
	}

	public void setToolCallId(String toolCallId) {
		this.toolCallId = toolCallId;
	}

	public String getToolName() {
		return toolName;
	}

	public void setToolName(String toolName) {
		this.toolName = toolName;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public Map<String, Object> getToolParameterValues() {
		return toolParameterValues;
	}

	public void setToolParameterValues(Map<String, Object> toolParameterValues) {
		this.toolParameterValues = toolParameterValues;
	}

	public String getToolStatus() {
		return toolStatus;
	}

	public void setToolStatus(String toolStatus) {
		this.toolStatus = toolStatus;
	}
}

