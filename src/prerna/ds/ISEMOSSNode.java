package prerna.ds;

public interface ISEMOSSNode extends ITreeKeyEvaluatable {
	
	public enum VALUE_TYPE {ORDINAL, NOMINAL};
	
	// gets the type of this node
	public String getType();
	
	// gets the type of value
	public VALUE_TYPE getValueType();

}
