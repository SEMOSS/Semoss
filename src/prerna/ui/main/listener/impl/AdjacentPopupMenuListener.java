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
		GraphPlaySheet ps2 = (GraphPlaySheet) ps;
		//Get the button name to understand whether to add upstream or downstream or both
		JMenuItem button = (JMenuItem) e.getSource();
		String buttonName = button.getName();
		Hashtable highlighAdjHash = new Hashtable();
		highlighAdjHash.put("type", buttonName);
		highlighAdjHash.put("vertices", this.vertices);
		highlighAdjHash.put("web", false);
		ps2.runHighlightAdjacent(highlighAdjHash);
	}		
}
