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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;

import javax.swing.AbstractAction;
import javax.swing.JButton;

import prerna.om.SEMOSSVertex;
import prerna.ui.transformer.VertexShapeTransformer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 * Controls the re-sizing of nodes in the graph play sheet.
 */
public class GraphVertexSizeListener extends AbstractAction implements ActionListener{

	VertexShapeTransformer transformerV;
	VisualizationViewer viewer;
	
	/**
	 * Constructor for GraphVertexSizeListener.
	 */
	public GraphVertexSizeListener(){
		
	}
	
	/**
	 * Method setTransformers.  Sets the transformers that the listener will access.
	 * @param trans VertexShapeTransformer
	 */
	public void setTransformers(VertexShapeTransformer trans){
		transformerV = trans;
	}
	
	/**
	 * Method setViewer.  Sets the viewer that the listener will access.
	 * @param v VisualizationViewer
	 */
	public void setViewer(VisualizationViewer v){
		viewer = v;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		//get selected vertices
		PickedState <SEMOSSVertex> psV = viewer.getPickedVertexState();
		
		JButton button = (JButton) e.getSource();
		
		//if no vertices are selected, perform action on all vertices
		if(psV.getPicked().size()==0){
			//just have to check if the button is to increase font size or decrease.
			//need to check bounds on how high/low the font size can get
			if(button.getName().contains("Increase")){
				transformerV.increaseSize();
			}
			else if(button.getName().contains("Decrease")){
				transformerV.decreaseSize();
			}
		}
		//else if vertices have been selected, apply action only to those vertices
		else if (psV.getPicked().size()> 0){
			Iterator <SEMOSSVertex> it = psV.getPicked().iterator();
			while(it.hasNext()){
				SEMOSSVertex vert = it.next();
				String URI = vert.getURI();
				if(button.getName().contains("Increase")){
					transformerV.increaseSize(URI);
				}
				else if(button.getName().contains("Decrease")){
					transformerV.decreaseSize(URI);
				}
			}
		}
		
		viewer.repaint();
	}

}
