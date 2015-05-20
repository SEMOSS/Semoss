package prerna.ds;

public class DoubleClass implements ISEMOSSNode {

	double value;
	String type = null;
	Object rawValue = null;
	
	public DoubleClass(double d, String t, Object r) {
		value = d;
		type = t;
		rawValue = r;
	}
	
	public DoubleClass(double d, String t) {
		value = d;
		type = t;
	}
	
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return value+"";
	}

	@Override
	public boolean isLeft(ITreeKeyEvaluatable object) {
		// TODO Auto-generated method stub
		return this.value > ((DoubleClass)object).value;
	}

	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		// TODO Auto-generated method stub
		return this.value == ((DoubleClass)object).value;
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return type;
	}

	@Override
	public String getRawValue() {
		// TODO Auto-generated method stub
		return this.rawValue.toString();
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VALUE_TYPE getValueType() {
		// TODO Auto-generated method stub
		return ISEMOSSNode.VALUE_TYPE.ORDINAL;
	}


}
