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

import java.awt.BasicStroke;
import java.awt.Stroke;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;

import prerna.om.SEMOSSVertex;

/**
 */
public class VertexStrokeTransformer implements Transformer <SEMOSSVertex, Stroke> {
	
	
	Hashtable <String, SEMOSSVertex> vertices = null;
	
	/**
	 * Constructor for VertexStrokeTransformer.
	 */
	public VertexStrokeTransformer()
	{
		
	}
	
	/**
	 * Method setEdges.
	 * @param edges Hashtable<String,DBCMVertex>
	 */
	public void setEdges(Hashtable <String, SEMOSSVertex> edges)
	{
		this.vertices = vertices;
	}
	

	/**
	 * Method transform.
	 * @param vertex DBCMVertex
	
	 * @return Stroke */
	@Override
	public Stroke transform(SEMOSSVertex vertex)
	{
		
		
		Stroke retStroke = new BasicStroke(1.0f);
		try
		{	
			retStroke = new BasicStroke(0f);
                

		}
		catch(Exception ex)
		{
			//ignore
		}
		return retStroke;
	}
}
