package prerna.sablecc.meta;

import java.util.Map;

public class DataframeHeaderMetadata extends AbstractPkqlMetadata{

	@Override
	public Map<String, Object> getMetadata() {
		//No data needed.
		return null;
	}

	@Override
	public String getExplanation() {
		String template = "Retrieved headers";
		return generateExplaination(template, getMetadata());
	}

}
