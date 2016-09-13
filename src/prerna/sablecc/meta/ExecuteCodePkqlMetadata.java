package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.Map;

public class ExecuteCodePkqlMetadata extends AbstractPkqlMetadata {

	public static final String CODE_TEMPLATE_KEY = "code";
	public static final String EXECUTOR_TEMPLATE_KEY = "executor";
	
	private String code;
	private String executor;
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		metadata.put(CODE_TEMPLATE_KEY, this.code);
		metadata.put(EXECUTOR_TEMPLATE_KEY, this.executor);
		return metadata;
	}

	@Override
	public String getExplanation() {
		String template = "Executing {{" + EXECUTOR_TEMPLATE_KEY + "}} code.  Ran : {{" + CODE_TEMPLATE_KEY+ "}}";
		return generateExplaination(template, getMetadata());
	}
	
	public void setExecutedCode(String code) {
		this.code = code;
	}
	
	public void setExecutorType(String executor) {
		this.executor = executor;
	}

}
