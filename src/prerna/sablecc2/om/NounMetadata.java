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
	
	//should make this immutable
	public Object getValue() {
		return this.value;
	}
	
	public boolean isScalar() {
		// if it is a number or word
		// it is a scalar
		if(this.noun == PkslDataTypes.CONST_DECIMAL || 
			this.noun == PkslDataTypes.CONST_INT ||
			this.noun == PkslDataTypes.CONST_STRING ||
			this.noun == PkslDataTypes.BOOLEAN) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * To help w/ debugging
	 */
	public String toString() {
		return "NOUN META DATA ::: " + this.value.toString();
	}
}
