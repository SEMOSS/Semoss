package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class ColUnfilterMetadata extends AbstractPkqlMetadata {
	private String unfilterCol;
	private final String UNFILTER_COL_TEMPLATE_KEY = "filCol";
	
	public ColUnfilterMetadata(String unfilterCol) {
		this.unfilterCol = unfilterCol;
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(UNFILTER_COL_TEMPLATE_KEY, this.unfilterCol);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Unfiltered column {{" + UNFILTER_COL_TEMPLATE_KEY + "}}";
		return generateExplaination(template, getMetadata());
	}
}
