package prerna.rdf.engine.wrappers;

import prerna.rdf.engine.api.IConstructStatement;

public class ConstructStatement implements IConstructStatement {

	String subject, predicate = null;
	Object object = null;
	
	@Override
	public String getPredicate() {
		// TODO Auto-generated method stub
		return predicate;
	}

	@Override
	public Object getObject() {
		// TODO Auto-generated method stub
		return object;
	}

	@Override
	public String getSubject() {
		// TODO Auto-generated method stub
		return subject;
	}

	@Override
	public void setPredicate(String predicate) {
		// TODO Auto-generated method stub
		this.predicate = predicate;
	}

	@Override
	public void setSubject(String subject) {
		// TODO Auto-generated method stub
		this.subject = subject;
	}

	@Override
	public void setObject(Object object) {
		// TODO Auto-generated method stub
		this.object = object;
	}

}
