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

import prerna.om.SEMOSSEdge;
import prerna.util.Constants;

/**
 */
public class EdgeArrowStrokeTransformer implements Transformer <SEMOSSEdge, Stroke> {
	
	
	Hashtable <String, SEMOSSEdge> edges = null;
	
	/**
	 * Constructor for EdgeArrowStrokeTransformer.
	 */
	public EdgeArrowStrokeTransformer()
	{
		
	}
	
	/**
	 * Method setEdges.
	 * @param edges Hashtable
	 */
	public void setEdges(Hashtable edges)
	{
		this.edges = edges;
	}
	
	/**
	 * Method transform.
	 * @param edge DBCMEdge
	
	 * @return Stroke */
	@Override
	public Stroke transform(SEMOSSEdge edge)
	{
		
		float selectedFontFloat =2.5f;
		float unselectedFontFloat =0.1f;
		
		float standardFontFloat = 0.3f;

		Stroke retStroke = new BasicStroke(1.0f);
		try
		{	
			if (edges != null) {
				if (edges.containsKey(edge.getProperty(Constants.URI))) {
					Object val = edges.get(edge.getProperty(Constants.URI));
					try {
						double valDouble = (Double) val;
						float valFloat = (float) valDouble;
						float newFontFloat = selectedFontFloat * valFloat;
						retStroke = new BasicStroke(newFontFloat, BasicStroke.CAP_BUTT,
								BasicStroke.JOIN_MITER, 10.0f);
					} catch(Exception e) {
						//TODO: Specify exception(s)
						retStroke = new BasicStroke(selectedFontFloat, BasicStroke.CAP_BUTT,
								BasicStroke.JOIN_MITER, 10.0f);
					}
				} else {
					retStroke = new BasicStroke(unselectedFontFloat);
				}
			}
			else {
				retStroke = new BasicStroke(standardFontFloat, BasicStroke.CAP_BUTT,
						BasicStroke.JOIN_ROUND);
			}
		}
		catch(Exception ex) {
			//TODO: Specify exception(s) and action
			//ignore
		}
		return retStroke;
	}
}
