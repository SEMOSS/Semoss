package prerna.ds;

public class DoubleClass implements ISEMOSSNode {

	double value;
	String type = null;
	Object rawValue;
	
	public DoubleClass(double d, Object r, String t) {
		value = d;
		rawValue = r;
		type = t;
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
		return this.value > ((DoubleClass)object).value;
	}

	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		return this.value == ((DoubleClass)object).value;
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
	public Double getValue() {
		return this.value;
	}

	@Override
	public VALUE_TYPE getValueType() {
		return ISEMOSSNode.VALUE_TYPE.ORDINAL;
	}


}
