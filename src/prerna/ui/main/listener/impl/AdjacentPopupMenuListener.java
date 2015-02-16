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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JMenuItem;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.DistanceDownstreamProcessor;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.Constants;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 * Controls what to do when the pop up menu is selected on a graph.
 */
public class AdjacentPopupMenuListener implements ActionListener {

	IPlaySheet ps = null;
	SEMOSSVertex [] vertices = null;
	
	static final Logger logger = LogManager.getLogger(AdjacentPopupMenuListener.class.getName());
	
	/**
	 * Method setPlaysheet.  Sets the instance of the playsheet.
	 * @param ps IPlaySheet  The playsheet accessed by the listener.
	 */
	public void setPlaysheet(IPlaySheet ps)
	{
		this.ps = ps;
	}
	
	/**
	 * Method setDBCMVertex.  Sets the instance of the DBCMVertex.
	 * @param vertices DBCMVertex[]  The vertex array accessed by the listener.
	 */
	public void setDBCMVertex(SEMOSSVertex [] vertices)
	{
		logger.debug("Set the vertices " + vertices.length);
		this.vertices = vertices;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		GraphPlaySheet ps2 = (GraphPlaySheet)ps;
		//Get the button name to understand whether to add upstream or downstream or both
		JMenuItem button = (JMenuItem) e.getSource();
		String buttonName = button.getName();
		// Get the DBCM edges from vertices and then add the edge
		
		Collection<Vector> allPlaySheetEdges = ps2.filterData.edgeTypeHash.values();
		Vector allEdgesVect = new Vector();
		for(Vector v: allPlaySheetEdges) allEdgesVect.addAll(v);
		logger.debug("Getting the base graph");

		//Get what edges are already highlighted so that we can just add to it
		//Get what vertices are already painted so we can just add to it
		EdgeStrokeTransformer tx = (EdgeStrokeTransformer)ps2.getView().getRenderContext().getEdgeStrokeTransformer();
		Hashtable <String, SEMOSSEdge> edgeHash= tx.getEdges();
		if(edgeHash==null) edgeHash = new Hashtable<String, SEMOSSEdge>();
			
		VertexPaintTransformer vtx = (VertexPaintTransformer)ps2.getView().getRenderContext().getVertexFillPaintTransformer();
		Hashtable <String, String> vertHash = vtx.getVertHash();
		if(vertHash == null) vertHash = new Hashtable<String, String>();
		
		PickedState state = ps2.getView().getPickedVertexState();
		state.clear();

		//if it is All, must use distance downstream processor to get all of the edges
		if(!buttonName.equals("All")){
			for(int vertIndex = 0;vertIndex < vertices.length;vertIndex++)
			{
				SEMOSSVertex vert = vertices[vertIndex];
				logger.debug("In Edges count is " + vert.getInEdges().size());
				logger.debug("Out Edges count is " + vert.getOutEdges().size());
				vertHash.put(vert.getURI(), vert.getURI());
				
				//if the button name contains upstream, get the upstream edges and vertices
				if(buttonName.contains("Downstream")){
					edgeHash = putEdgesInHash(vert.getOutEdges(), edgeHash);
					for (SEMOSSEdge edge : vert.getOutEdges()){
						if (allEdgesVect.contains(edge)){
							vertHash.put(edge.inVertex.getURI(), edge.inVertex.getURI());
							state.pick(edge.inVertex, true);
						}
					}
				}
				
				//if the button name contains downstream, get the downstream edges and vertices
				if(buttonName.contains("Upstream")){
					edgeHash = putEdgesInHash(vert.getInEdges(), edgeHash);
					for (SEMOSSEdge edge : vert.getInEdges()){
						if (allEdgesVect.contains(edge)){
							vertHash.put(edge.outVertex.getURI(), edge.outVertex.getURI());
							state.pick(edge.outVertex, true);
						}
					}
				}
				
			}
		}
		
		else if(buttonName.equals("All")){
			DistanceDownstreamProcessor ddp = new DistanceDownstreamProcessor();
			ddp.setSelectedNodes(vertices);
			ddp.setForest(ps2.forest);
			ddp.execute();
			//use the master hash to set the nodes and edges
			Hashtable masterHash = ddp.masterHash;
			Iterator masterIt = masterHash.keySet().iterator();
			while(masterIt.hasNext()){
				SEMOSSVertex vert = (SEMOSSVertex) masterIt.next();
				Hashtable vHash = (Hashtable) masterHash.get(vert);
				ArrayList<SEMOSSVertex> parentPath = (ArrayList<SEMOSSVertex>) vHash.get(ddp.pathString);
				ArrayList<SEMOSSEdge> parentEdgePath = (ArrayList<SEMOSSEdge>) vHash.get(ddp.edgePathString);
				edgeHash = putEdgesInHash(new Vector(parentEdgePath), edgeHash);
				for (SEMOSSEdge edge : parentEdgePath){
					if (allEdgesVect.contains(edge)){
						vertHash.put(edge.outVertex.getURI(), edge.outVertex.getURI());
						vertHash.put(edge.inVertex.getURI(), edge.inVertex.getURI());
						state.pick(edge.outVertex, true);
						state.pick(edge.inVertex, true);
					}
				}
			}
			
		}
		ps2.getView().setPickedVertexState(state);
	
		tx.setEdges(edgeHash);
		vtx.setVertHash(vertHash);
		VertexLabelFontTransformer vlft = (VertexLabelFontTransformer)ps2.getView().getRenderContext().getVertexFontTransformer();
		vlft.setVertHash(vertHash);
		ArrowDrawPaintTransformer atx = (ArrowDrawPaintTransformer)ps2.getView().getRenderContext().getArrowDrawPaintTransformer();
		atx.setEdges(edgeHash);
		EdgeArrowStrokeTransformer stx = (EdgeArrowStrokeTransformer)ps2.getView().getRenderContext().getEdgeArrowStrokeTransformer();
		stx.setEdges(edgeHash);
			
		// repaint it
		ps2.getView().repaint();
	}
	
	/**
	 * Method putEdgesInHash.  Puts the new relationships in the in-memory graph hashtable.
	 * @param edges Vector<DBCMEdge>  The Vector of new edges.
	 * @param hash Hashtable<String,DBCMEdge>  The hashtable to be updated.
	
	 * @return Hashtable<String,DBCMEdge> The updated hashtable.*/
	private Hashtable <String, SEMOSSEdge> putEdgesInHash(Vector <SEMOSSEdge> edges, Hashtable <String, SEMOSSEdge> hash)
	{
		for(int edgeIndex = 0;edgeIndex < edges.size();edgeIndex++)
		{
			SEMOSSEdge edge = edges.elementAt(edgeIndex);
			hash.put((String)edge.getProperty(Constants.URI), edge);
		}
		return hash;
	}
		
}
