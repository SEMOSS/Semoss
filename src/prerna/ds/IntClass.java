package prerna.ds;

// simple class to test the binary tree
public class IntClass implements ISEMOSSNode{

	int value;
	String type = null;
	int rawValue;
	
	public IntClass(int value)
	{
		this.value = value;
	}
	
	public IntClass(String string, Boolean serialized){
		if(!serialized){
			this.value = Integer.parseInt(string);
		}
		else{
			fromString(string);
		}
	}
	
	public IntClass(int value, String type)
	{
		this.value = value;
		this.type = type;
	}
	
	public IntClass(int value, int rawValue, String type) {
		this.value = value;
		this.type = type;
		this.rawValue = rawValue;
	}
	
	@Override
	public String toString(){
		String className = "";
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		String ret = className + "###" + 
				"innerString==="+this.value +
				"&&&type==="+this.type +
				"&&&rawValue==="+this.rawValue;
		return ret;
	}
	
	private void fromString(String serializedString){
//		System.err.println(serializedString);
		String[] mainParts = serializedString.split("&{3}");
		for(int idx = 0; idx < mainParts.length; idx++){
			String element = mainParts[idx];
			String[] parts = element.split("={3}");
			String name = parts[0];
			String value = parts[1];
			if(!value.equals("null")){
				if(name.equals("innerString")){
					this.value = Integer.parseInt(value);
				}
				else if(name.equals("type")){
					this.type = value;
				}
				else if(name.equals("rawValue")){
					this.rawValue = Integer.parseInt(value);
				}
			}
		}
	}
	
	@Override
	public String getKey() {
		return value+"";
	}

	@Override
	public boolean isLeft(ITreeKeyEvaluatable object) {
		boolean val = false;
		try {
			val = this.value > ((IntClass)object).value;
		} catch (ClassCastException e) {
			try {
				val = this.value > ((DoubleClass)object).value;
			} catch (ClassCastException e2) {
				return false;
			}
		}
		return val;
	}
	
	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		boolean val = false;
		try {
			val = this.value == ((IntClass)object).value;
		} catch (ClassCastException e) {
			return false;
		}
		return val;
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
