package prerna.sablecc2.om;

public class NounMetadata 
{
	private String explanation;
	private boolean required;
	private final PkslDataTypes noun;
	private final Object value;

	public NounMetadata(Object value, PkslDataTypes noun) {
		setDefaults();
		this.noun = noun;
		this.value = value;
	}
	
	private void setDefaults() {
		explanation = "";
		required = true;
	}
	
	public PkslDataTypes getNounName() {
		return noun;
	}
	
	public void setIsRequired(boolean isRequired) {
		this.required = isRequired;
	}
	
	public boolean isRequired() {
		return this.required;
	}
	
	public void setExplanation(String explanation) {
		this.explanation = explanation;
	}
	
	public String explanation() {
		return this.explanation;
	}
	
	//should make this immutable
	public Object getValue() {
		return this.value;
	}
}
