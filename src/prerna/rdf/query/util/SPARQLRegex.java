package prerna.rdf.query.util;

public class SPARQLRegex {
	
	TriplePart var, value;
	Boolean isValueString;
	String regexString;
	
	//TODO: incorporate having clauses inside of a regex

	public SPARQLRegex(TriplePart var, TriplePart value, boolean isValueString)
	{
		if(!var.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
		if(!value.getType().equals(TriplePart.LITERAL))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql literal");
		}
		this.var = var;
		this.value = value;
		this.isValueString = isValueString;
	}
	
	public void createString()
	{
		String subjectString = SPARQLQueryHelper.createComponentString(var);
		String objectString = SPARQLQueryHelper.createComponentString(value);
		if(!isValueString)
		{
			subjectString = "STR(" + subjectString + ")";
		}
		regexString = "REGEX(" + subjectString + ", " + objectString + " )";
	}
	
	public String getRegexString()
	{
		createString();
		return regexString;
	}
	
	public Object getVar()
	{
		return var;
	}
	
	public Object getValue()
	{
		return value;
	}

}
