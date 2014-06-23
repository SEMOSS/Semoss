package prerna.rdf.query.util;

import java.util.ArrayList;

public class SPARQLBindings {
	ArrayList<TriplePart> bindSubject;
	TriplePart bindObject;
	String bindingsString;
	
	public SPARQLBindings(ArrayList<TriplePart> bindSubject, TriplePart bindObject)
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
		String subjectString = "";
		for(TriplePart subjectPart : bindSubject)
		{
			subjectString = subjectString + "(" + SPARQLQueryHelper.createComponentString(subjectPart) + ")";
		}
		String objectString = SPARQLQueryHelper.createComponentString(bindObject);
		bindingsString = "BINDINGS " + objectString + " {" + subjectString + "}";
	}
	
	public String getBindingString()
	{
		createString();
		return bindingsString;
	}
	
	public Object getBindingSubject()
	{
		return bindSubject;
	}
	
	public Object getBindingObject()
	{
		return bindObject;
	}

}
