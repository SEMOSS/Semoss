package prerna.rdf.query.util;

public class SPARQLBind {
	TriplePart bindSubject, bindObject;
	String bindString;
	
	public SPARQLBind(TriplePart bindSubject, TriplePart bindObject)
	{
		if(!bindObject.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
		this.bindSubject = bindSubject;
		this.bindObject = bindObject;
	}
	
	public void createString()
	{
		String subjectString = SPARQLQueryHelper.createComponentString(bindSubject);
		String objectString = SPARQLQueryHelper.createComponentString(bindObject);
		bindString = "BIND(" + subjectString + " AS " + objectString + ")";
	}
	
	public String getBindString()
	{
		createString();
		return bindString;
	}
	
	public Object getBindSubject()
	{
		return bindSubject;
	}
	
	public Object getBindObject()
	{
		return bindObject;
	}

}
