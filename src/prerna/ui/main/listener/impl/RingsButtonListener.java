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

import javax.swing.JToggleButton;

import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.BalloonLayoutRings;
import prerna.ui.transformer.RadialTreeLayoutRings;
import edu.uci.ics.jung.algorithms.layout.BalloonLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.graph.Forest;
import edu.uci.ics.jung.visualization.VisualizationViewer;

/**
 * Controls the rendering of rings on a graph if the layout is a balloon or a radial tree.
 */
public class RingsButtonListener implements ActionListener {
	
	VisualizationViewer view;
	GraphPlaySheet ps;
	BalloonLayoutRings rings=new BalloonLayoutRings();
	RadialTreeLayoutRings treeRings = new RadialTreeLayoutRings();
	Layout lay;
	
	/**
	 * Constructor for RingsButtonListener.
	 */
	public RingsButtonListener(){
		this.view = view;
	}

	/**
	 * Method setViewer.  Sets the view that the listener will access.
	 * @param view VisualizationViewer
	 */
	public void setViewer(VisualizationViewer view){
		this.view = view;
		this.rings.setViewer(view);
		this.treeRings.setViewer(view);
	}
	
	/**
	 * Method setGraph.  Sets the graph that the listener will access.
	 * @param forest Forest
	 */
	public void setGraph(Forest forest){
		treeRings.setForest(forest);
	}
	
	/**
	 * Method setLayout.  Sets the layout that the listener will access.
	 * @param lay Layout
	 */
	public void setLayout(Layout lay){
		this.lay = lay;
		if(lay instanceof BalloonLayout)
			this.rings.setLayout(lay);
		else if (lay instanceof RadialTreeLayout)
			this.treeRings.setLayout(lay);
	}
	

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// Get if the button is selected
		JToggleButton button = (JToggleButton) e.getSource();
		
		if(!button.isSelected()){
			button.setSelected(false);
			if(lay instanceof BalloonLayout)
				view.removePreRenderPaintable(rings);
			else if (lay instanceof RadialTreeLayout)
				view.removePreRenderPaintable(treeRings);
		}
		else{
			button.setSelected(true);
			if(lay instanceof BalloonLayout)
				view.addPreRenderPaintable(rings);
			else if (lay instanceof RadialTreeLayout){
				view.addPreRenderPaintable(treeRings);
			}
		}
		view.repaint();
		
	}
	
	

}
