/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.transformer;

import java.awt.Color;
import java.awt.Paint;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

/**
 * Transforms the color of vertices/nodes on the graph.
 */
public class VertexPaintTransformer implements Transformer <SEMOSSVertex, Paint> {

	Hashtable<String, String> verticeURI2Show = null;
	Logger logger = Logger.getLogger(getClass());
	
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
