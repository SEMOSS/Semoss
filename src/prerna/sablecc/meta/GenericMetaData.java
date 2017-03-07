package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class GenericMetaData extends AbstractPkqlMetadata {

	private String staticExplanation;
	public GenericMetaData(String explanation) {
		this.staticExplanation = explanation;
	}
	
	public void setExplanation(String explanation) {
		this.staticExplanation = explanation;
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		return metadata;
	}

	@Override
	public String getExplanation() {
		return staticExplanation;
	}
}