package prerna.sablecc2.pipeline;

import java.util.Map;

public class ConstantPipelineOperation extends PipelineOperation {

	Map<String, Object> scalarMap = null;
	
	/**
	 * Constructor 
	 * @param opName		The name of the reactor for the operation
	 * @param opString		Primarily need this for debugging
	 */
	public ConstantPipelineOperation(String opName, String opString) {
		super(opName, opString);
	}
	
	public void setScalarMap(Map<String, Object> scalarMap) {
		this.scalarMap = scalarMap;
	}
}
