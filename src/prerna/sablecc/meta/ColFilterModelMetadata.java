package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class ColFilterModelMetadata extends AbstractPkqlMetadata {
	private String col;
	private String filterWord;
	private final String COL_TEMPLATE_KEY = "col";
	
	public ColFilterModelMetadata(String col) {
		this.col = col;
	}

	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(COL_TEMPLATE_KEY, col);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Retrieved filter model data for {{" + COL_TEMPLATE_KEY + "}}";
		return generateExplaination(template, getMetadata());
	}
}
