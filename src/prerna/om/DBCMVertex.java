/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import prerna.util.Constants;
import prerna.util.Utility;


/**
 */
public class DBCMVertex{
	
	
	public String uri = null;
	public Hashtable <String, Object> propHash = new Hashtable<String,Object>();
	transient Hashtable <String, String>uriHash = new Hashtable<String,String>();
	Vector <DBCMEdge> inEdge = new Vector<DBCMEdge>();
	Vector <DBCMEdge> outEdge = new Vector<DBCMEdge>();
	
	transient static final Logger logger = LogManager.getLogger(DBCMVertex.class.getName());
	
	// TODO need to find a way to identify the source i.e. put that as a property
	
	/**
	 * Constructor for DBCMVertex.
	 * @param uri String
	 */
	public DBCMVertex(String uri)
	{
		this.uri = uri;
		putProperty(Constants.URI, uri);
		
		// parse out all the oth er properties
		logger.debug("URI " + uri);
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
//		int totalTok = tokens.countTokens();
		String className = Utility.getClassName(uri);
		String instanceName = Utility.getInstanceName(uri);
		
		logger.debug("Class Name " + className + " Instance Name " + instanceName);

		if(instanceName == null)
			instanceName = uri;
		if(className == null)
			className = instanceName;
		
		putProperty(Constants.VERTEX_TYPE, className);
		logger.debug("Type is " + className);
		
		putProperty(Constants.VERTEX_NAME, instanceName);
		logger.debug("Name is " + instanceName);

	}
	
	/**
	 * Constructor for DBCMVertex.
	 * @param type String
	 * @param vert Object
	 */
	public DBCMVertex(String type, Object vert)
	{
		this.uri = type + "/" + vert;
		putProperty(Constants.URI, this.uri);
		
		String value = vert +"";
		if(vert instanceof Literal)
		{
			//logger.info("This is a literal impl >>>>>> "  + ((Literal)propValue).doubleValue());
			try {
				propHash.put(type, ((Literal)vert).doubleValue());				
			}catch(Exception ex)
			{logger.debug(ex);}
			try{
				propHash.put(type, vert + "");
			}catch (Exception ex)
			{logger.debug(ex);}
		}

		// parse out all the oth er properties
		logger.debug("URI " + uri);
		String className = Utility.getInstanceName(uri);
				
		putProperty(Constants.VERTEX_TYPE, className);
		logger.debug("Type is " + className);
		
		putProperty(Constants.VERTEX_NAME, value);
		logger.debug("Name is " + value);

	}

	
	/**
	 * Method getProperty.
	
	 * @return Hashtable */
	public Hashtable getProperty()
	{
		return this.propHash;
	}
	
	/**
	 * Method addInEdge.
	 * @param edge DBCMEdge
	 */
	public void addInEdge(DBCMEdge edge)
	{
		inEdge.add(edge);
		double edgeCount = 0.0;
		if(propHash.containsKey(Constants.INEDGE_COUNT))
			edgeCount = (Double)propHash.get(Constants.INEDGE_COUNT);
		edgeCount++;
		propHash.put(Constants.INEDGE_COUNT, edgeCount);
		
		addVertexCounter(edge.inVertex);
	}
	
	/**
	 * Method addVertexCounter.
	 * @param outVert DBCMVertex
	 */
	public void addVertexCounter(DBCMVertex outVert)
	{
		// also create specific 
		// find the type
		// get the node on other side
		String vertType = (String)outVert.getProperty(Constants.VERTEX_TYPE);
		//logger.info("Vertex Type is >>>>>>>>>>>>>>>>>" + vertType);
		Integer vertTypeCount = new Integer(0);
		if(propHash.containsKey(vertType))
			vertTypeCount = (Integer)propHash.get(vertType);	
		vertTypeCount++;
		propHash.put(vertType, vertTypeCount);
	}
	
	
	/**
	 * Method addOutEdge.
	 * @param edge DBCMEdge
	 */
	public void addOutEdge(DBCMEdge edge)
	{
		outEdge.add(edge);
		double edgeCount = 0.0;
		if(propHash.containsKey(Constants.OUTEDGE_COUNT))
			edgeCount = (Double)propHash.get(Constants.OUTEDGE_COUNT);
		edgeCount++;
		propHash.put(Constants.OUTEDGE_COUNT, edgeCount);

		addVertexCounter(edge.outVertex);
	}
	
	/**
	 * Method getInEdges.
	
	 * @return Vector<DBCMEdge> */
	public Vector<DBCMEdge> getInEdges()
	{
		return this.inEdge;
	}

	/**
	 * Method getOutEdges.
	
	 * @return Vector<DBCMEdge> */
	public Vector<DBCMEdge> getOutEdges()
	{
		return this.outEdge;
	}

	/**
	 * Method getURI.
	
	 * @return String */
	public String getURI()
	{
		return uri;
	}

	/**
	 * Method getProperty.
	 * @param arg0 String
	
	 * @return Object */
	public Object getProperty(String arg0) {
		return propHash.get(arg0);
	}

	/**
	 * Method getPropertyKeys.
	
	 * @return Set<String> */
	public Set<String> getPropertyKeys() {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Method removeProperty.
	 * @param arg0 String
	
	 * @return Object */
	public Object removeProperty(String arg0) {
		return propHash.remove(arg0);
	}

	/**
	 * Method putProperty.
	 * @param propName String
	 * @param propValue String
	 */
	public void putProperty(String propName, String propValue)
	{
		propHash.put(propName, propValue);
	}
	
	/**
	 * Method setProperty.
	 * @param propNameURI String
	 * @param propValue Object
	 */
	public void setProperty(String propNameURI, Object propValue) {
		// one is a p
		StringTokenizer tokens = new StringTokenizer(propNameURI + "", "/");
		int totalTok = tokens.countTokens();
		String className = null;
		String instanceName = null;

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok)
				className = tokens.nextToken();
			else if (tokIndex + 1 == totalTok)
				instanceName = tokens.nextToken();
			else
				tokens.nextToken();
		}
		uriHash.put(instanceName, propNameURI);
		// I need to convert these decimals and other BS into a proper value
		// awesome !!
		// will come to this in a bit
		logger.debug(instanceName + "<>" + propValue);

		// need to write the routine for conversion here
		
		boolean converted = false;
		try
		{
			if(propValue instanceof Literal)
			{
				//logger.info("This is a literal impl >>>>>> "  + ((Literal)propValue).doubleValue());
				propHash.put(instanceName, ((Literal)propValue).doubleValue());
				converted = true;
			}
		}catch(RuntimeException ex)
		{
			logger.debug(ex);
		}
		try
		{
			if(propValue instanceof com.hp.hpl.jena.rdf.model.Literal)
			{
				logger.info("Class is " + propValue.getClass());
				// try double
				try
				{
					Double value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getDouble();
					converted = true;
					propHash.put(instanceName, value);
				}catch (RuntimeException ignored) {
					logger.debug(ignored);
					converted = false;
				}
				
				// try integer
				if(!converted)
				{
					try
					{
						Integer value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getInt();
						converted = true;
						propHash.put(instanceName, value);
					}catch (RuntimeException ignored) {
						logger.debug(ignored);
						converted = false;
					}
				}
				
				// try boolean
				if(!converted)
				{
					try
					{
						Boolean value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getBoolean();
						converted = true;
						propHash.put(instanceName, value);
					}catch (RuntimeException ignored) {
						logger.debug(ignored);
					}
				}
				// try string
				if(!converted)
				{
					try
					{
						String value = ((com.hp.hpl.jena.rdf.model.Literal)propValue).getString();
						converted = true;
						propHash.put(instanceName, value);

					}catch (RuntimeException ignored) {
						logger.debug(ignored);
						converted = false;
					}
				}
				
				//propHash.put(instanceName, ((com.hp.hpl.jena.rdf.model.Literal)propValue).getDouble());
				//converted = true;
			}
		}catch(RuntimeException ex)
		{
			logger.debug(ex);
		}
		if(!converted)
		{
			propHash.put(instanceName, propValue);
		}
		logger.debug(uri + "<>" + instanceName + "<>" + propValue);
	}
}
