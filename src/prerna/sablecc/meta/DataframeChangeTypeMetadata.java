package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class DataframeChangeTypeMetadata extends AbstractPkqlMetadata {

	private String column;
	private String newType;
	private String oldType;
	private final String COL_TEMPLATE_KEY = "filCol";
	private final String NEW_TYPE_TEMPLATE_KEY = "newType";
	private final String OLD_TYPE_TEMPLATE_KEY = "oldType";

	public DataframeChangeTypeMetadata(String column, String newType, String oldType) {
		this.column = column;
		this.newType = newType.toUpperCase();
		this.oldType = oldType.toUpperCase();
	}
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(COL_TEMPLATE_KEY, this.column);
		metadata.put(NEW_TYPE_TEMPLATE_KEY, this.newType);
		metadata.put(OLD_TYPE_TEMPLATE_KEY, this.oldType);

		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Changed column {{" + COL_TEMPLATE_KEY + "}} from type {{" + OLD_TYPE_TEMPLATE_KEY + "}} to "
				+ "type {{" + NEW_TYPE_TEMPLATE_KEY + "}}";
		return generateExplaination(template, getMetadata());
	}
	
}
