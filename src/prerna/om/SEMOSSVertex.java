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
package prerna.om;

import java.awt.Color;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.Utility;


/**
 */
public class SEMOSSVertex{
	
	
	public String uri = null;
	public Hashtable <String, Object> propHash = new Hashtable<String,Object>();
	transient public Hashtable <String, SEMOSSVertex> edgeHash = new Hashtable<String,SEMOSSVertex>();
	
	// hash of URIs
	transient Hashtable <String, String>uriHash = new Hashtable<String,String>();
	
	// Tree structure in memory as a hashtable
	// the hashtable is typically of the following format
	// all - has all the nodes currently recorded - Fuzzy on this one
	// <Edgetype - VertexHash>
	// where the vertex Hash has
	// <Vertex Name - Vertex>
	// also has All - which is all the vertices within it
	transient Hashtable <String, Hashtable> navHash = null;
	
	Vector <SEMOSSEdge> inEdge = new Vector<SEMOSSEdge>();
	Vector <SEMOSSEdge> outEdge = new Vector<SEMOSSEdge>();
	
	transient static final Logger logger = LogManager.getLogger(SEMOSSVertex.class.getName());
	
	// TODO need to find a way to identify the source i.e. put that as a property
	
	/**
	 * Constructor for DBCMVertex.
	 * @param uri String
	 */
	public SEMOSSVertex(String uri)
	{
		this.uri = uri;
		putProperty(Constants.URI, uri);
		
		// parse out all the oth er properties
		logger.debug("URI " + uri);
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
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

		Color color = TypeColorShapeTable.getInstance().getColor(className, instanceName);
		setColor(color);
		logger.debug("Color is " + color);
	}
	
	/**
	 * Constructor for DBCMVertex.
	 * @param type String
	 * @param vert Object
	 */
	public SEMOSSVertex(String type, Object vert)
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

		Color color = TypeColorShapeTable.getInstance().getColor(className, value);
		setColor(color);
		logger.debug("Color is " + color);
	}

	
	/**
	 * Method getProperty.
	
	 * @return Hashtable */
	public Hashtable getProperty()
	{
		return this.propHash;
	}
	
	// refresh the color with what is in TypeColorShapeTable
	public void resetColor()
	{
		this.setColor(TypeColorShapeTable.getInstance().getColor(this.getProperty(Constants.VERTEX_TYPE)+"", this.getProperty(Constants.VERTEX_NAME)+""));
	}
	
	public void setColor(Color c){
		String rgb = "";
		if(c != null){
			rgb = c.getRed() + "," + c.getGreen() + "," +c.getBlue();
		}
		this.putProperty(Constants.VERTEX_COLOR, rgb);
	}
	
	public Color getColor(){
		String color = this.getProperty(Constants.VERTEX_COLOR) + "";
		if(!color.isEmpty()){
			StringTokenizer tokenizer = new StringTokenizer(color, ",");
			return new Color(Integer.parseInt(tokenizer.nextToken()), Integer.parseInt(tokenizer.nextToken()), Integer.parseInt(tokenizer.nextToken()));
		}
		else return null;
	}
	
	// this is the out vertex
	public void addInEdge(SEMOSSEdge edge)
	{
		inEdge.add(edge);
		Integer edgeCount = new Integer(0);
		if(propHash.containsKey(Constants.INEDGE_COUNT))
			edgeCount = (Integer)propHash.get(Constants.INEDGE_COUNT);
		edgeCount++;
		propHash.put(Constants.INEDGE_COUNT, edgeCount);
		
		// adding the edge
		edgeHash.put(edge.inVertex.getProperty(Constants.VERTEX_NAME) + "", edge.inVertex);
		
		addVertexCounter(edge.outVertex);
		//loadEdge(edge);
	}
	
	/**
	 * Method addVertexCounter.
	 * @param outVert DBCMVertex
	 */
	public void addVertexCounter(SEMOSSVertex outVert)
	{
		// also create specific 
		// find the type
		// get the node on other side
		String vertType = (String)outVert.getProperty(Constants.VERTEX_TYPE);
		//logger.info("Vertex Type is >>>>>>>>>>>>>>>>>" + vertType);
		Integer vertTypeCount = new Integer(0);
		try
		{
			if(propHash.containsKey(vertType))
				vertTypeCount = (Integer)propHash.get(vertType);	
			vertTypeCount++;
			propHash.put(vertType, vertTypeCount);
		}catch (Exception ignored)
		{logger.debug(ignored);
		}
	}
	
	
	// this is the invertex
	public void addOutEdge(SEMOSSEdge edge)
	{
		outEdge.add(edge);
		Integer edgeCount = new Integer(0);
		if(propHash.containsKey(Constants.OUTEDGE_COUNT))
			edgeCount = (Integer)propHash.get(Constants.OUTEDGE_COUNT);
		edgeCount++;
		propHash.put(Constants.OUTEDGE_COUNT, edgeCount);

		// add the out vertex
		edgeHash.put(edge.outVertex.getProperty(Constants.VERTEX_NAME) + "", edge.outVertex);
		
		addVertexCounter(edge.inVertex);
	}
	
	/**
	 * Method getInEdges.
	
	 * @return Vector<DBCMEdge> */
	public Vector<SEMOSSEdge> getInEdges()
	{
		return this.inEdge;
	}

	/**
	 * Method getOutEdges.
	
	 * @return Vector<DBCMEdge> */
	public Vector<SEMOSSEdge> getOutEdges()
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
		// TODO Auto-generated method stub
		return propHash.get(arg0);
	}

	/**
	 * Method getPropertyKeys.
	
	 * @return Set<String> */
	public Set<String> getPropertyKeys() {
		return propHash.keySet();
	}

	/**
	 * Method removeProperty.
	 * @param arg0 String
	
	 * @return Object */
	public Object removeProperty(String arg0) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
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
		}catch(Exception ex)
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
					}
				}
				
				//propHash.put(instanceName, ((com.hp.hpl.jena.rdf.model.Literal)propValue).getDouble());
				//converted = true;
			}
		}catch (RuntimeException ignored) {
			logger.debug(ignored);
		}
		if(!converted)
		{
			propHash.put(instanceName, propValue);
		}
		logger.debug(uri + "<>" + instanceName + "<>" + propValue);
	}
	
	// the call that will create the tree
	public void createTree()
	{
		// get all the inedges
		// create the structure
		if(navHash == null)
			navHash = new Hashtable<String, Hashtable>();
		
		for(int edgeIndex = 0;edgeIndex < inEdge.size();edgeIndex++)
		{
			loadEdge(inEdge.elementAt(edgeIndex));
		}	
	}
	public void loadEdge(SEMOSSEdge edge)
	{
		
		// need to expose the edge properties as well		
		if(navHash == null)
			navHash = new Hashtable<String, Hashtable>();

		String edgeType = (String)edge.getProperty(Constants.EDGE_TYPE);
		SEMOSSVertex otherVertex = edge.inVertex;
		
		// get the vertex type hash
		Hashtable <String, Object> vertHash = new Hashtable<String, Object>();
		if(navHash.containsKey(edgeType))
			vertHash = navHash.get(edgeType);
		
		// now put the other hashtable within it
		String vertexName = (String)otherVertex.getProperty(Constants.VERTEX_NAME);

		// put the vertex and props
		vertHash.put(vertexName, otherVertex);
		// put the properties
		vertHash.put(vertexName + Constants.PROPS, edge.propHash);
		//System.err.println("From " + getProperty(Constants.VERTEX_NAME) + " Adding Vertex " + vertexName + "To Hash " + edgeType);
		navHash.put(edgeType, vertHash);
	}
	
	public boolean equals(SEMOSSVertex vert) {
		return this.getURI().equals(vert.getURI());
	}
}
