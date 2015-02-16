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

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Paint;
import java.awt.Stroke;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSEdge;
import prerna.util.Constants;

/**
 * Transforms the color of edges on the graph.
 */
public class EdgePaintTransformer implements Transformer <SEMOSSEdge, Paint> {
	
	Hashtable<String, Paint> edgeHash = null;
	static final Logger logger = LogManager.getLogger(EdgePaintTransformer.class.getName());
	public static VertexPaintTransformer tx = null;
	
	/**
	 * Constructor for EdgePaintTransformer.
	 */
	public EdgePaintTransformer()
	{
		
	}
	/**
	 * Method setEdgeHash.  Sets the local edge hashtable.
	 * @param edgesHash Hashtable<String,Paint>
	 */
	public void setEdgeHash(Hashtable<String, Paint> edgesHash)
	{
		this.edgeHash = edgesHash;
	}
	
	
	/**
	 * Method getInstance.  Retrieves an instance of the vertex paint transformer.	
	 * @return VertexPaintTransformer */
	public static VertexPaintTransformer getInstance()
	{
		if(tx == null)
			tx = new VertexPaintTransformer();
		return tx;
	}

	/**
	 * Method transform.  Get the DI Helper to find what is needed to get for vertex
	 * @param edge DBCMEdge - The edge of which this returns the properties.
	
	 * @return Paint - The type of Paint. */
	@Override
	public Paint transform(SEMOSSEdge edge)
	{
		
		Paint p = Color.red;
		float dash[] = {10.0f};
		
		Stroke retStroke = new BasicStroke(1.0f);
		try
		{	
                if (edgeHash != null) {
                    if (edgeHash.containsKey(edge.getProperty(Constants.URI))) {
                          p=edgeHash.get(edge.getProperty(Constants.URI));
                    } else{
                          p=Color.black;
                          // logger.info(count);
                    }
                }
                else
                {
                	p = Color.black;
                }
                

		}
		catch(RuntimeException ex) {
			//TODO: Specify exception(s) and action
			//ignore
			System.out.println("ignored");
		}
		return p;
	}
}
