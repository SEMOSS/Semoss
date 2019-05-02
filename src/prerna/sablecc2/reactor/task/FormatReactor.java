package prerna.sablecc2.reactor.task;

import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class FormatReactor extends TaskBuilderReactor {
	
	public FormatReactor() {
		this.keysToGet = new String[]{"type", ReactorKeysEnum.OPTIONS.getKey()};
	}
	
	@Override
	protected void buildTask() {
		GenRowStruct type = this.store.getNoun("type");
		if(type != null && !type.isEmpty()) {
			String formatType = type.get(0).toString();
			task.setFormat(formatType);
		}
		GenRowStruct options = this.store.getNoun(keysToGet[1]);
		if(options != null && !options.isEmpty()) {
			Map<String, Object> optionValues = (Map<String, Object>) options.get(0);
			task.setFormatOptions(optionValues);
		}
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("type")) {
			return "The format type (e.g., a visualization can be the \"table\" or \"graph\" type)";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
