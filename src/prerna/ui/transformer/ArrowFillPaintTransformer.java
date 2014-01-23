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

import org.apache.commons.collections15.Transformer;

import prerna.om.SEMOSSEdge;

/**
 * Highlights the edges of a graph so they can be highlighted.
 */
public class ArrowFillPaintTransformer implements Transformer <SEMOSSEdge, Paint> {
	
	
	Hashtable <String, SEMOSSEdge> edges = null;
	
	/**
	 * Constructor for ArrowFillPaintTransformer.
	 */
	public ArrowFillPaintTransformer()	{
		
	}
	
	/**
	 * Method setEdges. Sets the edges of the graph.
	 * @param edges Hashtable<String,DBCMEdge>
	 */
	public void setEdges(Hashtable <String, SEMOSSEdge> edges)
	{
		this.edges = edges;
	}
	

	/**
	 * Method transform.  Transforms the edge parameter to its original color.
	 * @param edge DBCMEdge
	
	 * @return Paint */
	@Override
	public Paint transform(SEMOSSEdge edge)
	{
		Paint p = Color.gray;
		return p;
	}
}
