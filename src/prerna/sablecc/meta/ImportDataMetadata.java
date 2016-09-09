package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class ImportDataMetadata extends AbstractPkqlMetadata {

	// TODO: this is an incomplete explanation!!!
	// need to finish this at some point
	// should use the explanation in the api reactor
	
	private List<String> selectors;
	private final String SELECTORS_TEMPLATE_KEY = "selectors";
	
	public ImportDataMetadata() {
		
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(SELECTORS_TEMPLATE_KEY, this.selectors);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Importing {{" + SELECTORS_TEMPLATE_KEY + "}}";
		return generateExplaination(template, getMetadata());
	}

	public void setColumns(List<String> selectors) {
		this.selectors = selectors;
	}

}
