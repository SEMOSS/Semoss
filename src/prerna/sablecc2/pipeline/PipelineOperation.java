package prerna.sablecc2.pipeline;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class PipelineOperation {

	List<Map> rowInputs = new Vector<Map>();
	Map<String, List<Map>> nounInputs = new Hashtable<String, List<Map>>();
	
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
	
	public void addRowInput(Map o) {
		this.rowInputs.add(o);
	}
	
	public void addNounInputs(String key, Map o) {
		if(this.nounInputs.containsKey(key)) {
			this.nounInputs.get(key).add(o);
		} else {
			List<Map> oList = new Vector<Map>();
			oList.add(o);
			this.nounInputs.put(key, oList);
		}
	}
	
	public List<Map> getRowInputs() {
		return this.rowInputs;
	}
	
	public Map<String, List<Map>> getNounInputs() {
		return this.nounInputs;
	}
}
