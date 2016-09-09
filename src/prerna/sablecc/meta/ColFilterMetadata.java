package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class ColFilterMetadata extends AbstractPkqlMetadata {

	private String filCol;
	private final String FILTER_COL_TEMPLATE_KEY = "filCol";
	
	public ColFilterMetadata(String filCol) {
		this.filCol = filCol;
	}

	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(FILTER_COL_TEMPLATE_KEY, this.filCol);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Filtered column {{" + FILTER_COL_TEMPLATE_KEY + "}}.";
		return generateExplaination(template, getMetadata());
	}
}
