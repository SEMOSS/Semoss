package prerna.query.querystruct.modelinference;

import java.util.Map;

import prerna.query.querystruct.AbstractQueryStruct;

public class ModelInferenceQueryStruct extends AbstractQueryStruct{
	
	protected String context = null;
	protected Map<String, Object> hyperParameters;
	public QUERY_STRUCT_TYPE qsType = QUERY_STRUCT_TYPE.ENGINE;
	
	public void setHyperParameters(Map<String, Object> hyperParameters) {
		this.hyperParameters = hyperParameters;
	}
	
	public Map<String, Object> getHyperParameters() {
		return this.hyperParameters;
	}
	
	public void setContext(String context) {
		this.context = context;
	}
	
	public String getContext() {
		return this.context;
	}
	
	// TODO create methods for function on input
	// TODO create methods for function on input
	// TODO create methods for function on input
	// TODO create methods for function on input
	
	// TODO create methods for function on output
	// TODO create methods for function on output
	// TODO create methods for function on output
	// TODO create methods for function on output
	
	// TODO create methods for vector jsons?
	// TODO create methods for vector jsons?
	// TODO create methods for vector jsons?	
}
