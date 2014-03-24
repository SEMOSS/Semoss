package prerna.rdf.query.util;

public class SPARQLTriple {
	String tripleString = "";
	TriplePart subject, predicate, object;
	boolean sbjURIBoo, predURIBoo, objURIBoo;
	
	public SPARQLTriple(TriplePart subject, TriplePart predicate, TriplePart object)
	{
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}
	
	public void createString()
	{
		String subjectString = SPARQLQueryHelper.createComponentString(subject);
		String predicateString = SPARQLQueryHelper.createComponentString(predicate);
		String objectString = SPARQLQueryHelper.createComponentString(object);
		tripleString = "{" + subjectString + " " + predicateString + " " + objectString + "}";
	}
	
	public String getTripleString()
	{
		createString();
		return tripleString;
	}
	
	public Object getSubject()
	{
		return subject;
	}
	
	public Object getPredicate()
	{
		return predicate;
	}
	
	public Object getObject()
	{
		return object;
	}
	
}
	


