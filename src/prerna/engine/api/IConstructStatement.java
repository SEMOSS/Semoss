/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.api;

/**
 * Interface for representing semantic triple statements in RDF-style data models.
 * 
 * <p>This interface defines the structure for semantic statements that follow the
 * Resource Description Framework (RDF) triple pattern of Subject-Predicate-Object.
 * These triples are fundamental building blocks for representing relationships
 * and facts in semantic data models and knowledge graphs.</p>
 * 
 * <p>A typical RDF triple might look like:</p>
 * <ul>
 *   <li><strong>Subject:</strong> "John Smith" (who or what the statement is about)</li>
 *   <li><strong>Predicate:</strong> "works_for" (the relationship or property)</li>
 *   <li><strong>Object:</strong> "ACME Corporation" (the value or target of the relationship)</li>
 * </ul>
 * 
 * <p>This interface is commonly used in RDF database engines, semantic reasoning
 * systems, and knowledge graph construction within the SEMOSS platform.</p>
 * 
 * @see {@link IRDFDatabase} for RDF database operations
 * @see {@link IConstructWrapper} for construct query handling
 * @author SEMOSS
 */
public interface IConstructStatement {

	/**
	 * Gets the predicate (property or relationship) of the RDF triple.
	 * 
	 * <p>The predicate defines the relationship or property that connects
	 * the subject to the object in the semantic statement.</p>
	 * 
	 * @return The predicate string representing the relationship or property
	 */
	public String getPredicate();
	
	/**
	 * Gets the object (value or target) of the RDF triple.
	 * 
	 * <p>The object is the value or resource that the predicate relates
	 * the subject to. It can be a literal value, URI, or other resource.</p>
	 * 
	 * @return The object value, which may be a string, URI, or other data type
	 */
	public Object getObject();
	
	/**
	 * Gets the subject (resource or entity) of the RDF triple.
	 * 
	 * <p>The subject is the resource or entity that the statement is about.
	 * It's typically a URI or identifier for the thing being described.</p>
	 * 
	 * @return The subject string representing the resource or entity
	 */
	public String getSubject();
	
	/**
	 * Sets the predicate (property or relationship) of the RDF triple.
	 * 
	 * @param predicate The predicate string representing the relationship or property
	 */
	public void setPredicate(String predicate);
	
	/**
	 * Sets the subject (resource or entity) of the RDF triple.
	 * 
	 * @param subject The subject string representing the resource or entity
	 */
	public void setSubject(String subject);
	
	/**
	 * Sets the object (value or target) of the RDF triple.
	 * 
	 * @param object The object value, which may be a string, URI, or other data type
	 */
	public void setObject(Object object);
	
	
}
