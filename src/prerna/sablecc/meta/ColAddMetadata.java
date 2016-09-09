package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class ColAddMetadata extends AbstractPkqlMetadata {
	
	private String newCol;
	private String exprTerm;
	private final String NEW_COL_TEMPLATE_KEY = "newColumn";
	private final String EXPR_TERM_TEMPLATE_KEY = "exprTerm";

	
	public ColAddMetadata(String newCol, String exprTerm) {
		this.newCol = newCol;
		this.exprTerm = exprTerm;
	}

	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(NEW_COL_TEMPLATE_KEY, this.newCol);
		metadata.put(EXPR_TERM_TEMPLATE_KEY, exprTerm);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Added column {{" + NEW_COL_TEMPLATE_KEY + "}} {{"+ EXPR_TERM_TEMPLATE_KEY +"}}";
		return generateExplaination(template, getMetadata());
	}
}
