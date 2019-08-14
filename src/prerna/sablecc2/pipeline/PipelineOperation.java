package prerna.sablecc2.pipeline;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class PipelineOperation {

	List<Object> rowInputs = new Vector<Object>();
	Map<String, Object> nounInputs = new Hashtable<String, Object>();
	
	// the name of the reactor
	String opName = null;
	String opString = null;
	String widgetId = null;
	/**
	 * Constructor 
	 * @param opName		The name of the reactor for the operation
	 * @param opString		Primarily need this for debugging
	 */
	public PipelineOperation(String opName, String opString) {
		this.opName = opName;
		this.opString = opString;
	}
	
	public String getOpName() {
		return this.opName;
	}
	
	public String getOpString() {
		return this.opString;
	}
	
	public void setWidgetId(String widgetId) {
		this.widgetId = widgetId;
	}
	
	public String getWidgetId() {
		return this.widgetId;
	}
	
	public void addRowInput(Object o) {
		this.rowInputs.add(o);
	}
	
	public void addRowInputs(List<Object> o) {
		this.rowInputs.addAll(o);
	}
	
	public void addNounInputs(String key, Object o) {
		this.nounInputs.put(key, o);
	}
	
	public List<Object> getRowInputs() {
		return this.rowInputs;
	}
	
	public Map<String, Object> getNounInputs() {
		return this.nounInputs;
	}
}
