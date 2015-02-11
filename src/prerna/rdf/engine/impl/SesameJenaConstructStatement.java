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
package prerna.rdf.engine.impl;

/**
 * A way of storing each triple in RDF for CONSTRUCT based SPARQL queries.
 */
@Deprecated
public class SesameJenaConstructStatement{
	
	String subject = null;
	String predicate = null;
	Object object = null;
	
	String serialRep = null;
	
	
	
	public String getSerialRep() {
		return serialRep;
	}

	public void setSerialRep(String serialRep) {
		this.serialRep = serialRep;
	}

	/**
	 * Method getSubject. Gets the subject of the SPARQL query.	
	 * @return String - the subject of the query.*/
	public String getSubject() {
		return subject;
	}
	
	/**
	 * Method setSubject.  Sets the subject of the SPARQL query.
	 * @param subject String - the subject of the query.
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}
	
	/**
	 * Method getPredicate. Gets the predicate of the SPARQL query.		
	 * @return String - the predicate of the query.*/
	public String getPredicate() {
		return predicate;
	}
	
	/**
	 * Method setPredicate. - Sets the predicate of the SPARQL query.	
	 * @param predicate String - the predicate of the query.
	 */
	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}
	
	/**
	 * Method getObject. Gets the object of the SPARQL query.	
	 * @return Object - the object of the query.*/
	public Object getObject() {
		return object;
	}
	
	/**
	 * Method setObject. Sets the object of the SPARQL query.	
	 * @param object Object - the object of the query.
	 */
	public void setObject(Object object) {
		this.object = object;
	}
}
