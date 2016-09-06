package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class DataframeMetadata extends AbstractPkqlMetadata {

	private String frameType;
	private final String FRAME_TEMPLATE_KEY = "dataFrame";
	
	public DataframeMetadata() {
		
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(FRAME_TEMPLATE_KEY, this.frameType);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Created new frame of type {{" + FRAME_TEMPLATE_KEY + "}}.";
		return generateExplaination(template, getMetadata());
	}

	public void setFrameType(String frameType) {
		this.frameType = frameType;
	}
}
