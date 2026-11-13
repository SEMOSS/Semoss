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

import java.awt.Color;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.parsers.RdfLiteralParser;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.Utility;

public class SEMOSSVertex {

	public String uri = null;
	public Map<String, Object> propHash = new HashMap<>();
	transient public Map<String, SEMOSSVertex> edgeHash = new HashMap<>();

	transient Vector<SEMOSSEdge> inEdge = new Vector<SEMOSSEdge>();
	transient Vector<SEMOSSEdge> outEdge = new Vector<SEMOSSEdge>();

	transient static final Logger classLogger = LogManager.getLogger(SEMOSSVertex.class.getName());

	/**
	 * Creates a vertex for the URI, with its type, name and color read from the
	 * URI.
	 * 
	 * @param uri the vertex URI
	 */
	public SEMOSSVertex(String uri) {
		this.uri = uri;
		putProperty(Constants.URI, uri);

		String className = Utility.getClassName(uri);
		String instanceName = Utility.getInstanceName(uri);

		classLogger.debug("Class Name " + className + " Instance Name " + instanceName);
		if (instanceName == null) {
			instanceName = uri;
		}
		if (className == null) {
			className = instanceName;
		}

		putProperty(Constants.VERTEX_TYPE, className);
		classLogger.debug("Type is " + className);

		putProperty(Constants.VERTEX_NAME, instanceName);
		classLogger.debug("Name is " + instanceName);

		Color color = TypeColorShapeTable.getInstance().getColor(className, instanceName);
		setColor(color);
		classLogger.debug("Color is " + color);
	}

	/**
	 * Creates a vertex for a value of the given type, such as a literal.
	 * 
	 * @param type the vertex type
	 * @param vert the value the vertex holds
	 */
	public SEMOSSVertex(String type, Object vert) {
		this.uri = type + "/" + vert;
		putProperty(Constants.URI, this.uri);

		String value = vert + "";
		if (vert instanceof org.openrdf.model.Literal) {
			propHash.put(type, value);
		}

		// parse out all the other properties
		classLogger.debug("URI " + uri);
		String className = Utility.getInstanceName(uri);

		putProperty(Constants.VERTEX_TYPE, className);
		classLogger.debug("Type is " + className);

		putProperty(Constants.VERTEX_NAME, value);
		classLogger.debug("Name is " + value);

		Color color = TypeColorShapeTable.getInstance().getColor(className, value);
		setColor(color);
		classLogger.debug("Color is " + color);
	}

	/**
	 * Returns the properties of the vertex.
	 * 
	 * @return the properties by name
	 */
	public Map<String, Object> getProperty() {
		return this.propHash;
	}

	// refresh the color with what is in TypeColorShapeTable
	public void resetColor() {
		this.setColor(TypeColorShapeTable.getInstance().getColor(this.getProperty(Constants.VERTEX_TYPE) + "",
				this.getProperty(Constants.VERTEX_NAME) + ""));
	}

	public void setColor(Color c) {
		String rgb = "";
		if (c != null) {
			rgb = c.getRed() + "," + c.getGreen() + "," + c.getBlue();
		}
		this.putProperty(Constants.VERTEX_COLOR, rgb);
	}

	public Color getColor() {
		if (!this.propHash.containsKey(Constants.VERTEX_COLOR)) {
			resetColor();
		}
		String color = this.getProperty(Constants.VERTEX_COLOR) + "";
		if (!color.isEmpty()) {
			StringTokenizer tokenizer = new StringTokenizer(color, ",");
			return new Color(Integer.parseInt(tokenizer.nextToken()), Integer.parseInt(tokenizer.nextToken()),
					Integer.parseInt(tokenizer.nextToken()));
		} else {
			return null;
		}
	}

	// this is the out vertex
	public void addInEdge(SEMOSSEdge edge) {
		inEdge.add(edge);
		Integer edgeCount = Integer.valueOf(0);
		if (propHash.containsKey(Constants.INEDGE_COUNT)) {
			edgeCount = (Integer) propHash.get(Constants.INEDGE_COUNT);
		}
		edgeCount++;
		propHash.put(Constants.INEDGE_COUNT, edgeCount);

		// adding the edge
		edgeHash.put(edge.inVertex.getProperty(Constants.VERTEX_NAME) + "", edge.inVertex);

		addVertexCounter(edge.outVertex);
	}

	/**
	 * Method addVertexCounter.
	 * 
	 * @param outVert the vertex on the other side of the edge
	 */
	public void addVertexCounter(SEMOSSVertex outVert) {
		// also create specific
		// find the type
		// get the node on other side
		String vertType = (String) outVert.getProperty(Constants.VERTEX_TYPE);
		Object vertTypeCount = propHash.getOrDefault(vertType, 0);
		if (vertTypeCount instanceof Integer count) {
			propHash.put(vertType, count + 1);
		} else {
			classLogger.debug("Property {} on vertex {} is not a count, so the {} neighbor count is skipped", vertType,
					uri, vertType);
		}
	}

	// this is the invertex
	public void addOutEdge(SEMOSSEdge edge) {
		outEdge.add(edge);
		Integer edgeCount = Integer.valueOf(0);
		if (propHash.containsKey(Constants.OUTEDGE_COUNT)) {
			edgeCount = (Integer) propHash.get(Constants.OUTEDGE_COUNT);
		}
		edgeCount++;
		propHash.put(Constants.OUTEDGE_COUNT, edgeCount);

		// add the out vertex
		edgeHash.put(edge.outVertex.getProperty(Constants.VERTEX_NAME) + "", edge.outVertex);

		addVertexCounter(edge.inVertex);
	}

	/**
	 * Returns the edges that point into this vertex.
	 * 
	 * @return the in edges
	 */
	public Vector<SEMOSSEdge> getInEdges() {
		return this.inEdge;
	}

	/**
	 * Returns the edges that point out of this vertex.
	 * 
	 * @return the out edges
	 */
	public Vector<SEMOSSEdge> getOutEdges() {
		return this.outEdge;
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
	 * Method putProperty.
	 * 
	 * @param propName  String
	 * @param propValue String
	 */
	public void putProperty(String propName, String propValue) {
		propHash.put(propName, propValue);
	}

	/**
	 * Method setProperty.
	 * 
	 * @param propNameURI String
	 * @param propValue   Object
	 */
	public void setProperty(String propNameURI, Object propValue) {
		String instanceName = Utility.getInstanceName(propNameURI);
		classLogger.debug(instanceName + "<>" + propValue);

		boolean converted = false;
		if (propValue instanceof org.openrdf.model.Literal literal
				&& RdfLiteralParser.readsAsDouble(literal.getLabel())) {
			try {
				propHash.put(instanceName, literal.doubleValue());
				converted = true;
			} catch (RuntimeException e) {
				classLogger.error("Could not read property {} on vertex {} as a double", instanceName, uri, e);
			}
		}
		if (propValue instanceof org.apache.jena.rdf.model.Literal literal) {
			try {
				propHash.put(instanceName, RdfLiteralParser.getJenaValue(literal));
				converted = true;
			} catch (RuntimeException e) {
				classLogger.error("Could not read the value of property {} on vertex {}", instanceName, uri, e);
				propHash.put(instanceName, literal.getString());
				converted = true;
			}
		}
		if (!converted) {
			propHash.put(instanceName, propValue);
		}
		classLogger.debug(uri + "<>" + instanceName + "<>" + propValue);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getURI() == null) {
			return false;
		}

		if (obj instanceof SEMOSSVertex) {
			return this.getURI().equals(((SEMOSSVertex) obj).getURI());
		}

		return false;
	}
}
