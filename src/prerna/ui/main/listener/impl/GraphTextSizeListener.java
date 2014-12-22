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
import java.util.Iterator;

import javax.swing.AbstractAction;
import javax.swing.JButton;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.transformer.EdgeLabelFontTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 * Controls the re-sizing of text on the graph play sheet.
 */
public class GraphTextSizeListener extends AbstractAction implements ActionListener{

	VertexLabelFontTransformer transformerV;
	EdgeLabelFontTransformer transformerE;
	VisualizationViewer viewer;
	
	/**
	 * Constructor for GraphTextSizeListener.
	 */
	public GraphTextSizeListener(){
		
	}
	
	/**
	 * Method setTransformers. Sets the transformers that the listener will access.
	 * @param trans VertexLabelFontTransformer
	 * @param transE EdgeLabelFontTransformer
	 */
	public void setTransformers(VertexLabelFontTransformer trans, EdgeLabelFontTransformer transE){
		transformerV = trans;
		transformerE = transE;
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
		PickedState <SEMOSSEdge> psE = viewer.getPickedEdgeState();
		
		JButton button = (JButton) e.getSource();
		
		//if no vertices are selected, perform action on all vertices
		if(psV.getPicked().size()==0 && psE.getPicked().size() == 0){
			//just have to check if the button is to increase font size or decrease.
			//need to check bounds on how high/low the font size can get
			if(button.getName().contains("Increase")){
				transformerV.increaseFontSize();
				transformerE.increaseFontSize();
			}
			else if(button.getName().contains("Decrease")){
				transformerV.decreaseFontSize();
				transformerE.decreaseFontSize();
			}
		}
		//else if vertices have been selected, apply action only to those vertices
		else if (psV.getPicked().size()> 0){
			Iterator <SEMOSSVertex> it = psV.getPicked().iterator();
			while(it.hasNext()){
				SEMOSSVertex vert = it.next();
				String URI = vert.getURI();
				if(button.getName().contains("Increase")){
					transformerV.increaseFontSize(URI);
				}
				else if(button.getName().contains("Decrease")){
					transformerV.decreaseFontSize(URI);
				}
			}
		}
		//else if edges have been selected, apply action only to those vertices
		else if (psE.getPicked().size()> 0){
			Iterator <SEMOSSEdge> it = psE.getPicked().iterator();
			while(it.hasNext()){
				SEMOSSEdge vert = it.next();
				String URI = vert.getURI();
				if(button.getName().contains("Increase")){
					transformerE.increaseFontSize(URI);
				}
				else if(button.getName().contains("Decrease")){
					transformerE.decreaseFontSize(URI);
				}
			}
		}
		
		viewer.repaint();
	}

}
