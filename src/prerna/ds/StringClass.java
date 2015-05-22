package prerna.ds;

public class StringClass implements ISEMOSSNode {
	
	String innerString = null;
	String type = null;
	String rawValue = null;
	
	public StringClass(String innerString)
	{
		this.innerString = innerString;
	}
	
	public StringClass(String innerString, String type)
	{
		this.innerString = innerString;
		this.type = type;
	}
	
	public StringClass(String innerString, String rawValue, String type) {
		this.innerString = innerString;
		this.type = type;
		this.rawValue = rawValue;
	}
	
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return innerString;
	}

	@Override
	public boolean isLeft(ITreeKeyEvaluatable object) {
		// TODO Auto-generated method stub
		return ((((StringClass)object).innerString.compareTo(innerString)) > 0);
	}

	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		// TODO Auto-generated method stub
		return ((StringClass)object).innerString.compareTo(innerString) == 0;
	}


	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return this.innerString;
	}


	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return this.type;
	}


	@Override
	public VALUE_TYPE getValueType() {
		// TODO Auto-generated method stub
		return ISEMOSSNode.VALUE_TYPE.NOMINAL;
	}

	@Override
	public String getRawValue() {
		// TODO Auto-generated method stub
		return this.rawValue;
	}
}
