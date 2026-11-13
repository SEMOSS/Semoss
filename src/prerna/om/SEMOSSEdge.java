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
package prerna.om;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.parsers.RdfLiteralParser;
import prerna.util.Constants;
import prerna.util.Utility;

public class SEMOSSEdge {

	transient public SEMOSSVertex inVertex = null;
	transient public SEMOSSVertex outVertex = null;
	String uri = null;
	String source = null;
	String target = null;

	public Map<String, Object> propHash = new HashMap<>();
	static final Logger classLogger = LogManager.getLogger(SEMOSSEdge.class.getName());

	/**
	 * 
	 * @param inVertex
	 * @param outVertex
	 * @param uri       Vertex1 (OutVertex) -------> Vertex2 (InVertex) (OutEdge)
	 *                  (InEdge)
	 */
	public SEMOSSEdge(SEMOSSVertex outVertex, SEMOSSVertex inVertex, String uri) {
		this.uri = uri;
		this.source = outVertex.getURI();
		this.target = inVertex.getURI();
		putProperty(Constants.URI, uri);
		String className = Utility.getClassName(uri);
		String edgeName = Utility.getInstanceName(uri);
		putProperty(Constants.EDGE_TYPE, className);
		putProperty(Constants.EDGE_NAME, edgeName);
		this.inVertex = inVertex;
		this.outVertex = outVertex;

		// in vertex
		inVertex.addInEdge(this);

		// out vertex
		outVertex.addOutEdge(this);
	}

	/**
	 * Method getURI.
	 * 
	 * @return String
	 */
	public String getURI() {
		return uri;
	}

	/**
	 * Method getProperty.
	 * 
	 * @param arg0 String
	 * 
	 * @return Object
	 */
	public Object getProperty(String arg0) {
		return propHash.get(arg0);
	}

	/**
	 * Returns the properties of the edge.
	 * 
	 * @return the properties by name
	 */
	public Map<String, Object> getProperty() {
		return propHash;
	}

	/**
	 * Method getPropertyKeys.
	 * 
	 * @return Set<String>
	 */
	public Set<String> getPropertyKeys() {
		return propHash.keySet();
	}

	/**
	 * Method removeProperty.
	 * 
	 * @param arg0 String
	 * 
	 * @return Object
	 */
	public Object removeProperty(String arg0) {
		return propHash.remove(arg0);
	}

	/**
	 * Method setProperty.
	 * 
	 * @param propNameURI String
	 * @param propValue   Object
	 */
	public void setProperty(String propNameURI, Object propValue) {
		String instanceName = Utility.getInstanceName(propNameURI);

		boolean converted = false;
		if (propValue instanceof org.openrdf.model.Literal literal
				&& RdfLiteralParser.readsAsDouble(literal.getLabel())) {
			try {
				propHash.put(instanceName, literal.doubleValue());
				converted = true;
			} catch (RuntimeException e) {
				classLogger.error("Could not read property {} on edge {} as a double", instanceName, uri, e);
			}
		}
		if (propValue instanceof org.apache.jena.rdf.model.Literal literal) {
			try {
				if (literal.getValue() instanceof Number number) {
					propHash.put(instanceName, number.doubleValue());
					converted = true;
				}
			} catch (RuntimeException e) {
				classLogger.error("Could not read the value of property {} on edge {}", instanceName, uri, e);
			}
		}
		if (!converted) {
			propHash.put(instanceName, propValue);
		}
		classLogger.debug(uri + "<>" + instanceName + "<>" + propValue);
	}

	/**
	 * Method putProperty.
	 * 
	 * @param propName  String
	 * @param propValue String
	 */
	public void putProperty(String propName, String propValue) {
		propHash.put(propName, propValue);
	}

}
