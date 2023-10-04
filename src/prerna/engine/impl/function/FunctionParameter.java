package prerna.engine.impl.function;

public class FunctionParameter {

	String parameterName;
	String parameterType;
	String parameterDescription;
	
	/**
	 * 
	 */
	public FunctionParameter() {
		
	}
	
	/**
	 * 
	 * @param parameterName
	 * @param parameterType
	 * @param parameterDescription
	 */
	public FunctionParameter(String parameterName, String parameterType, String parameterDescription) {
		this.parameterName = parameterName;
		this.parameterType = parameterType;
		this.parameterDescription = parameterDescription;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getParameterName() {
		return parameterName;
	}
	
	/**
	 * 
	 * @param parameterName
	 */
	public void setParameterName(String parameterName) {
		this.parameterName = parameterName;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getParameterType() {
		return parameterType;
	}
	
	/**
	 * 
	 * @param parameterType
	 */
	public void setParameterType(String parameterType) {
		this.parameterType = parameterType;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getParameterDescription() {
		return parameterDescription;
	}
	
	/**
	 * 
	 * @param parameterDescription
	 */
	public void setParameterDescription(String parameterDescription) {
		this.parameterDescription = parameterDescription;
	}
	
}
