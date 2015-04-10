package prerna.ds;

// simple class to test the binary tree

public class IntClass implements ITreeKeyEvaluatable{

	int value;
	
	public IntClass(int value)
	{
		this.value = value;
	}
	
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return value+"";
	}

	@Override
	public boolean isLeft(ITreeKeyEvaluatable object) {
		// TODO Auto-generated method stub
		return this.value > ((IntClass)object).value;
	}
	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		// TODO Auto-generated method stub
		return this.value == ((IntClass)object).value;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return null;
	}

}
