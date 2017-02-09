package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class DataInsightMetaData extends AbstractPkqlMetadata {

	
	public DataInsightMetaData() {
		
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Returned Insight Id";
		return template;
//		return generateExplaination(template, getMetadata());
	}
}
