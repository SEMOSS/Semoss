package prerna.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;

import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.util.Utility;

public abstract class AbstractLambdaTaskReactor extends TaskBuilderReactor {
	
	static final String IMPORTS_KEY = "imports";

	/**
	 * Return the code block for the lambda function
	 * @return
	 */
	protected String getCode() {
		String encodedCode = (String) this.propStore.get("CODE");
		String code = Utility.decodeURIComponent(encodedCode);
		return code;
	}
	
	/**
	 * Get imports to add as part of the class
	 * @return
	 */
	protected List<String> getImports() {
		List<String> imports = new ArrayList<String>();
		GenRowStruct importGrs = this.store.getNoun(IMPORTS_KEY);
		if(importGrs != null && !importGrs.isEmpty()) {
			int size = importGrs.size();
			for(int i = 0; i < size; i++) {
				imports.add(importGrs.get(i).toString());
			}
		}
		return imports;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("CODE")) {
			return "The code block for the lambda function";
		} else if (key.equals(IMPORTS_KEY)) {
			return "The imports to add as part of the class";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
