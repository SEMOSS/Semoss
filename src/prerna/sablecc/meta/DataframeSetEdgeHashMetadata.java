package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class DataframeSetEdgeHashMetadata extends AbstractPkqlMetadata {

	private Map<String, Set<String>> newEdgeHash;
	private final String NEW_EDGE_HASH_KEY = "filEdgeHash";

	public DataframeSetEdgeHashMetadata(Map<String, Set<String>> newEdgeHash) {
		this.newEdgeHash = newEdgeHash;
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(NEW_EDGE_HASH_KEY, this.newEdgeHash);

		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Changed edge hash to {{" + NEW_EDGE_HASH_KEY + "}}";
		return generateExplaination(template, getMetadata());
	}
	
}
