package prerna.rdf.query.util;

public class SPARQLFilterURIParam implements ISPARQLFilterInput{

	FILTER_TYPE type = FILTER_TYPE.URI_MATCH;
	TriplePart var, value;
	String paramString;
	String comparator;
	
	// this class is to add filters as URIs
	public SPARQLFilterURIParam(TriplePart var, TriplePart value, String comparator)
	{
		if(!var.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
//		if(!value.getType().equals(TriplePart.URI))
//		{
//			throw new IllegalArgumentException("Bind object has to be a uri");
//		}
		this.var = var;
		this.value = value;
		this.comparator = comparator;
	}
	
	public void createString()
	{
		// example is ?Studio = <http://semoss.org/ontologies/Concept/Studio/Buena_Vista>
		String subjectString = SPARQLQueryHelper.createComponentString(var);
		String objectString = SPARQLQueryHelper.createComponentString(value);
		paramString = subjectString + " " + comparator + " " + objectString;
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
