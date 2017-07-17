package prerna.sablecc2.om;

public class NounMetadata 
{
	private String explanation = "";
	private final PkslDataTypes noun;
	private final PkslOperationTypes opType;
	private final Object value;

	public NounMetadata(Object value, PkslDataTypes noun) {
		this(value, noun, PkslOperationTypes.OPERATION);
	}
	
	public NounMetadata(Object value, PkslDataTypes noun, PkslOperationTypes opType) {
		this.noun = noun;
		this.value = value;
		this.opType = opType;
	}
	
	public PkslDataTypes getNounType() {
		return this.noun;
	}
	
	public PkslOperationTypes getOpType() {
		return this.opType;
	}
	
	public void setExplanation(String explanation) {
		this.explanation = explanation;
	}
	
	public String explanation() {
		return this.explanation;
	}
	
	public Object getValue() {
		return this.value;
	}
	
	/**
	 * To help w/ debugging
	 */
	public String toString() {
		return "NOUN META DATA ::: " + this.value.toString();
	}
}
