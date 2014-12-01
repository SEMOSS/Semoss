package prerna.rdf.query.util;

public class SEMOSSParameter {
	private String paramType;
	private String param;
	private TriplePart paramPart;
	
	String parameterString;
	
	SEMOSSParameter(String param, String paramType, TriplePart paramPart){
		this.param = param;
		this.paramType = paramType;
		this.paramPart = paramPart;
	}
	
	public void createString() {
		String objectString = SPARQLQueryHelper.createComponentString(paramPart);
		parameterString = "BIND(<@" + param + "-" + paramType + "@> AS " + objectString + ")";
	}
	
	public String getParamString() {
		createString();
		return parameterString;
	}
	
	public String getParamName() {
		return param;
	}
	
	public String getParamType() {
		return paramType;
	}
	
	public TriplePart getParamPart() {
		return paramPart;
	}
}
