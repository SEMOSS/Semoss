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
import java.awt.event.MouseEvent;
import java.awt.geom.Point2D;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JComponent;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.SimpleGraphNodePopup;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.SQLGraphPlaysheet;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import edu.uci.ics.jung.algorithms.layout.GraphElementAccessor;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.ModalLensGraphMouse;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 * Controls what happens when a user clicks on a node in a graph.
 */
public class SimpleGraphListener extends ModalLensGraphMouse implements IChakraListener
{
	static final Logger logger = LogManager.getLogger(SimpleGraphListener.class.getName());
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.debug(" Came to action performed here");
	}
	
	/**
	 * Method mousePressed.  Controls what happens when the mouse is pressed.
	 * @param e MouseEvent
	 */
	@Override
	public void mousePressed(MouseEvent e)
	{
		super.mousePressed(e);
		logger.debug(e.getModifiers());
		logger.debug(" Clicked");
		logger.debug(e.getSource());
		VisualizationViewer viewer = (VisualizationViewer)e.getSource();
		
		SEMOSSVertex clickedVert = null;
		//Check to see if an edge or a vertex was selected so we know which to show in the property table model
		boolean nodeClicked = false;
        GraphElementAccessor pickSupport = viewer.getPickSupport();
		Point2D p = e.getPoint();
        Point2D ivp = p;
    	Object clickedObject = pickSupport.getVertex(viewer.getGraphLayout(), ivp.getX(), ivp.getY());
    	if(clickedObject instanceof SEMOSSVertex){
    		nodeClicked = true;
    		clickedVert = (SEMOSSVertex) clickedObject;
    	}
		
    	SQLGraphPlaysheet ps3 = (SQLGraphPlaysheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.PROP_TABLE);
		TableModel tm = new DefaultTableModel();
		
		Hashtable vertHash = new Hashtable();

		// handle the edges 
		/*
		if(!nodeClicked){ //only need to work with the edges if an edge was clicked directly
			PickedState <SEMOSSEdge> ps2 = viewer.getPickedEdgeState();
			Iterator <SEMOSSEdge> it2 = ps2.getPicked().iterator();
			while(it2.hasNext())
			{
				SEMOSSEdge v = it2.next();
				logger.info("Edge Name  >>> " + v.getProperty(Constants.EDGE_NAME));
				// this needs to invoke the property table model stuff
				EdgePropertyTableModel pm = new EdgePropertyTableModel(ps3.getFilterData(), v);
				table.setModel(pm);
				//table.repaint();
				pm.fireTableDataChanged();
				logger.debug("Add this in - Edge Table");	
			}
		}*/


		//if it is a right click
		//if there are no vertices already selected
		//see if the right click was on a vertex
		//show popup menu
		if(e.getButton() == MouseEvent.BUTTON3)
		{
			logger.debug("Button 3 is pressed ");

			PickedState <SEMOSSVertex> ps = viewer.getPickedVertexState();
			Iterator <SEMOSSVertex> it = ps.getPicked().iterator();
			SEMOSSVertex [] vertices = new SEMOSSVertex[ps.getPicked().size()];
			
			// all we need to do here is get the array of selected vertices so that we can pass them to the popup
			if(!nodeClicked){
				for(int vertIndex = 0;it.hasNext();vertIndex++)
				{
					SEMOSSVertex v = it.next();
					vertices[vertIndex] = v;
					//add selected vertices
					vertHash.put(v.getProperty(Constants.URI), v.getProperty(Constants.URI));
					
					logger.info("Vert Name  >>> " + v.getProperty(Constants.VERTEX_NAME));
	
					logger.debug("Add this in - Prop Table");
				}
			}
			
			else{
		        if(clickedVert != null) {
	            	logger.info("Got vertex on right click");
	            	ps.clear();
	            	ps.pick(clickedVert, true);
	            	vertices = new SEMOSSVertex[1];
	            	vertices[0] = clickedVert;
		        }
			}

			SimpleGraphNodePopup popup = new SimpleGraphNodePopup(ps3, vertices, e.getComponent(), e.getX(), e.getY());
			
			popup.show(e.getComponent(), e.getX(), e.getY());
			// need to show a menu here
		}
		
		//Need vertex to highlight when click in skeleton mode... Here we need to get the already selected vertices
		//so that we can add to them
		VertexLabelFontTransformer vlft = null;
		/*
		if(ps3.searchPanel.btnHighlight.isSelected()) {
			vlft = (VertexLabelFontTransformer) viewer.getRenderContext().getVertexFontTransformer();
			vertHash.putAll(vlft.getVertHash());
			vlft.setVertHash(vertHash);
			VertexPaintTransformer ptx = (VertexPaintTransformer)viewer.getRenderContext().getVertexFillPaintTransformer();
			ptx.setVertHash(vertHash);
			viewer.repaint();
		}*/
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}

}
