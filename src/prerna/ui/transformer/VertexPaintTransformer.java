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
package prerna.ui.transformer;

import java.awt.Paint;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.util.Constants;

/**
 * Transforms the color of vertices/nodes on the graph.
 */
public class VertexPaintTransformer implements Transformer <SEMOSSVertex, Paint> {

	Hashtable<String, String> verticeURI2Show = null;
	static final Logger logger = LogManager.getLogger(VertexPaintTransformer.class.getName());
	
	/**
	 * Constructor for VertexPaintTransformer.
	 */
	public VertexPaintTransformer()
	{
		
	}
	
	/**
	 * Method setVertHash.  Sets the Hashtable of vertices.
	 * @param verticeURI2Show Hashtable
	 */
	public void setVertHash(Hashtable<String, String> verticeURI2Show)
	{
		this.verticeURI2Show = verticeURI2Show;
	}

	/**
	 * Method getVertHash. Retreives the hashtable of vertices 
	
	 * @return Hashtable */
	public Hashtable getVertHash()
	{
		return verticeURI2Show;
	}
	
	/**
	 * Method transform.  Get the DI Helper to find what is needed to get for vertex
	 * @param arg0 DBCMVertex - The edge of which this returns the properties.
	
	 * @return Paint - The type of Paint. */
	@Override
	public Paint transform(SEMOSSVertex arg0) {
		Paint type = null;
		
		if(verticeURI2Show == null){
			type = arg0.getColor();
		}
		else if(verticeURI2Show != null)
		{
			String URI = (String)arg0.getProperty(Constants.URI);
			logger.debug("URI " + URI);
			if(verticeURI2Show.containsKey(URI))
			{
				type = arg0.getColor();
//				String propType = (String)arg0.getProperty(Constants.VERTEX_TYPE);
//				String vertName = (String)arg0.getProperty(Constants.VERTEX_NAME);
//				logger.debug("Found the URI");
//				Object hashRet = verticeURI2Show.get(URI);
//				if (hashRet instanceof Color)
//				{
//					type = (Color)verticeURI2Show.get(URI);
//				}
//				else
//				{
//					type = TypeColorShapeTable.getInstance().getColor(propType, vertName);
//				}
			}
		}
		return type;
	}
}
