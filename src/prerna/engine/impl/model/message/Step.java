package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class Step {

	//Fields
	
	//Text?
	//Tools?
	
	@SerializedName("tools")
	private List<String> tools = new ArrayList<>();
    
    @SerializedName("content")
	private String content;

	public List<String> getTools() {
		return tools;
	}

	public void setTools(List<String> tools) {
		this.tools = tools;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
    
    
}
