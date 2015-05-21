package prerna.ds;

public class DoubleClass implements ISEMOSSNode {

	double value;
	String type = null;
	double rawValue;
	
	public DoubleClass(double d, String t, double r) {
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
	public Double getRawValue() {
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
