package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
		List<String> retSelectors = this.selectors;
		// this is for when a user types in a import data
		// without specifying the selectors
		// it uses the engines full QueryStruct based on the owl
		// and loads everything in
		// shorthand designed for loading in a full flat db quickly
		if(retSelectors == null) {
			retSelectors = new Vector<String>();
			retSelectors.add("Loading All Engine Data");
		}
		metadata.put(SELECTORS_TEMPLATE_KEY, retSelectors);
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
