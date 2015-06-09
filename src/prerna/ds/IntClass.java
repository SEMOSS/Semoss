package prerna.ds;

// simple class to test the binary tree
public class IntClass implements ISEMOSSNode{

	int value;
	String type = null;
	Object rawValue;
	
	public IntClass(int value)
	{
		this.value = value;
	}
	
	public IntClass(int value, String type)
	{
		this.value = value;
		this.type = type;
	}
	
	public IntClass(int value, Object rawValue, String type) {
		this.value = value;
		this.type = type;
		this.rawValue = rawValue;
	}
	
	@Override
	public String getKey() {
		return value+"";
	}

	@Override
	public boolean isLeft(ITreeKeyEvaluatable object) {
		return this.value > ((IntClass)object).value;
	}
	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		return this.value == ((IntClass)object).value;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public Object getRawValue() {
		return this.rawValue;
	}

	@Override
	public Integer getValue() {
		return this.value;
	}

	@Override
	public VALUE_TYPE getValueType() {
		return ISEMOSSNode.VALUE_TYPE.ORDINAL;
	}

}
