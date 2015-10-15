package prerna.rdf.query.util;

public class SPARQLFilterURIParam implements ISPARQLFilterInput{

	FILTER_TYPE type = FILTER_TYPE.URI_MATCH;
	TriplePart var, value;
	String paramString;
	
	// this class is to add filters as URIs
	public SPARQLFilterURIParam(TriplePart var, TriplePart value)
	{
		if(!var.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
		if(!value.getType().equals(TriplePart.URI))
		{
			throw new IllegalArgumentException("Bind object has to be a uri");
		}
		this.var = var;
		this.value = value;
	}
	
	public void createString()
	{
		// example is ?Studio = <http://semoss.org/ontologies/Concept/Studio/Buena_Vista>
		String subjectString = SPARQLQueryHelper.createComponentString(var);
		String objectString = SPARQLQueryHelper.createComponentString(value);
		paramString = subjectString + " = " + objectString;
	}
	
	@Override
	public Object getVar()
	{
		return var;
	}
	
	@Override
	public Object getValue()
	{
		return value;
	}

	@Override
	public String getFilterInput() {
		createString();
		return paramString;
	}
	
}
