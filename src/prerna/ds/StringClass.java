package prerna.ds;

public class StringClass implements ISEMOSSNode {
	
	String innerString = null;
	String type = null;
	String rawValue = null;
	
	public StringClass(String string, Boolean serialized){
		if(!serialized){
			this.innerString = innerString;
		}
		else{
			fromString(string);
		}
	}
	
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
	public String toString(){
		String className = "";
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		String ret = className + "###" + 
				"innerString==="+this.innerString +
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
					this.innerString = value;
				}
				else if(name.equals("type")){
					this.type = value;
				}
				else if(name.equals("rawValue")){
					this.rawValue = value;
				}
			}
		}
	}
	
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return innerString;
	}

	@Override
	public boolean isLeft(ITreeKeyEvaluatable object) {
		boolean val = false;
		try {
			val = ((((StringClass)object).innerString.compareTo(innerString)) > 0);
		} catch (ClassCastException e) {
			return true;
		}
		return val;
	}

	@Override
	public boolean isEqual(ITreeKeyEvaluatable object) {
		boolean val = false;
		try {
			val = ((StringClass)object).innerString.compareTo(innerString) == 0;
		} catch (ClassCastException e) {
			return false;
		}
		return val;
	}


	@Override
	public String getValue() {
		return this.innerString;
	}


	@Override
	public String getType() {
		return this.type;
	}


	@Override
	public VALUE_TYPE getValueType() {
		return ISEMOSSNode.VALUE_TYPE.NOMINAL;
	}

	@Override
	public String getRawValue() {
		return this.rawValue;
	}
}
