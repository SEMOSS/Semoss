package prerna.sablecc2;

import java.util.Map;

public class NotebookExecution {

	private PixelRunner runner;
	private Map<String, Object> variableOutput;
	
	public PixelRunner getRunner() {
		return runner;
	}
	
	public void setRunner(PixelRunner runner) {
		this.runner = runner;
	}
	
	public Map<String, Object> getVariableOutput() {
		return variableOutput;
	}
	
	public void setVariableOutput(Map<String, Object> variableOutput) {
		this.variableOutput = variableOutput;
	}
	
}
