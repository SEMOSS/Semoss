package prerna.sablecc.meta;

import java.util.HashMap;
import java.util.Map;

public class ColRenameMetadata extends AbstractPkqlMetadata {
	
	private String oldNameCol;
	private String renameCol;
	private String exprTerm;
	private final String OLD_COL_TEMPLATE_KEY = "oldColumnName";
	private final String NEW_COL_TEMPLATE_KEY = "renamedColumnName";

	
	public ColRenameMetadata(String oldName, String newName, String exprTerm) {
		this.oldNameCol = oldName;
		this.renameCol = newName;
	}

	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new HashMap<String, Object>();
//		metadata.put(OLD_COL_TEMPLATE_KEY, this.oldNameCol);
		metadata.put(NEW_COL_TEMPLATE_KEY, this.renameCol);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Renamed column {{" + NEW_COL_TEMPLATE_KEY + "}}";
		return generateExplaination(template, getMetadata());
	}
}