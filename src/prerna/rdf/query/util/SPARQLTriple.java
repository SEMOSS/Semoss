/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
	


