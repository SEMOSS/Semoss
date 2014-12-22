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
