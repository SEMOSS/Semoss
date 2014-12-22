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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.alg.KruskalMinimumSpanningTree;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.util.Constants;
import prerna.util.Utility;


// implements the minimum spanning tree

/**
 */
public class MSTPopupMenuListener implements ActionListener {

	IPlaySheet ps = null;
	SEMOSSVertex [] vertices = null;
	
	static final Logger logger = LogManager.getLogger(MSTPopupMenuListener.class.getName());
	
	/**
	 * Method setPlaysheet.
	 * @param ps IPlaySheet
	 */
	public void setPlaysheet(IPlaySheet ps)
	{
		this.ps = ps;
	}
		
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		
		// gets the view from the playsheet
		// gets the jGraphT graph
		// runs the kruskal on it
		// Creates the edges and sets it on the edge painter
		// repaints it
		// I cannot add this to the interface because not all of them will be forced to have it
		// yes, which means the menu cannot be generic too - I understand
		GraphPlaySheet ps2 = (GraphPlaySheet)ps;
		logger.debug("Getting the base graph");
		Graph graph = ps2.getGraph();
		KruskalMinimumSpanningTree<SEMOSSVertex, SEMOSSEdge> kmst = new KruskalMinimumSpanningTree<SEMOSSVertex, SEMOSSEdge>(graph);
		
		// get all the edges
		Iterator <SEMOSSEdge> csIterator = kmst.getEdgeSet().iterator();
		Hashtable <String, SEMOSSEdge> edgeHash = new Hashtable<String, SEMOSSEdge>();
		while(csIterator.hasNext())
		{
				SEMOSSEdge edge = csIterator.next();
				String edgeName = (String)edge.getProperty(Constants.URI);
				edgeHash.put(edgeName, edge);
		}

		EdgeStrokeTransformer tx = (EdgeStrokeTransformer)ps2.getView().getRenderContext().getEdgeStrokeTransformer();
		tx.setEdges(edgeHash);
		
		// repaint it
		ps2.getView().repaint();
		int originalSize = ps2.forest.getEdgeCount();
		int shortestPathSize = kmst.getEdgeSet().size();
		Utility.showMessage("Minimum Spanning Tree uses " + shortestPathSize + " edges out of " + originalSize+ " original edges");
	}
	
}
