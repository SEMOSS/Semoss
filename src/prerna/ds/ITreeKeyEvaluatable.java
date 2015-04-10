package prerna.ds;

public interface ITreeKeyEvaluatable {
	
	public String getKey();
	
	public boolean isLeft(ITreeKeyEvaluatable object); // evaluates for less than or equal to
	
	public boolean isEqual(ITreeKeyEvaluatable object); // evaluates if it is equal
	
	public String getValue();
}
