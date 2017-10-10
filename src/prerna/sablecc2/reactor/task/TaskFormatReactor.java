package prerna.sablecc2.reactor.task;

import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;

public class TaskFormatReactor extends TaskBuilderReactor {
	
	@Override
	protected void buildTask() {
		GenRowStruct type = this.store.getNoun("type");
		if(type != null && !type.isEmpty()) {
			String formatType = type.get(0).toString();
			task.setFormat(formatType);
		}
		GenRowStruct options = this.store.getNoun("options");
		if(options != null && !options.isEmpty()) {
			Map<String, Object> optionValues = (Map<String, Object>) options.get(0);
			task.setFormatOptions(optionValues);
		}
	}
}
