package prerna.sablecc2.om;

import java.util.Map;

public class NounMetadata 
{
	//Single = 1 and only 1
	//Multiple = 1 or more
	public enum QUANTITY {SINGLE, MULTIPLE};
//	public enum REQUIRED{ TRUE, FALSE};
	
	private String explanation;
	private boolean required;
	private final String noun;
	private String source;
	private QUANTITY quantity;
	private final Object value;

	private Map<String, Object> nounMetaData;
	
	public NounMetadata(Object value, String noun) {
		setDefaults();
		this.noun = noun;
		this.value = value;
	}
	
	private void setDefaults() {
		explanation = "";
		required = true;
		source = "";
		quantity = QUANTITY.SINGLE;
	}
	
//	public void setNounName(String noun) {
//		this.noun = noun;
//		nounMetaData.put("NOUN", noun);
//	}
	
	public String getNounName() {
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
