package prerna.rdf.engine.api;

public interface IConstructStatement {

	public String getPredicate();
	
	public Object getObject();
	
	public String getSubject();
	
	public void setPredicate(String predicate);
	
	public void setSubject(String subject);
	
	public void setObject(Object object);
	
	
}
