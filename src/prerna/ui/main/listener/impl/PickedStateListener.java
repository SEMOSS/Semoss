/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.PickedState;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.VertexPropertyTableModel;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.ui.transformer.VertexShapeTransformer;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls what happens when a picked state occurs.
 */
public class PickedStateListener implements ItemListener {

	private static final Logger logger = LogManager.getLogger(PickedStateListener.class);
	VisualizationViewer viewer;
	
	/**
	 * Constructor for PickedStateListener.
	 * @param v VisualizationViewer
	 */
	public PickedStateListener(VisualizationViewer v){
		viewer = v;
	}
	
	/**
	 * Method itemStateChanged.  Changes the state of the graph play sheet.
	 * @param e ItemEvent
	 */
	@Override
	public void itemStateChanged(ItemEvent e) {
		logger.debug(" Clicked");
		logger.info(e.getSource());

		GraphPlaySheet ps3 = (GraphPlaySheet) ((OldInsight) InsightStore.getInstance().getActiveInsight()).getPlaySheet();

		JTable table = (JTable) Utility.getDIHelperLocalProperty(Constants.PROP_TABLE);
		TableModel tm = new DefaultTableModel();
		table.setModel(tm);

		//need to check if there are any size resets that need to be done
		VertexShapeTransformer vst = (VertexShapeTransformer) viewer.getRenderContext().getVertexShapeTransformer();
		vst.emptySelected();

		// handle the vertices
		PickedState <SEMOSSVertex> ps = viewer.getPickedVertexState();
		Iterator <SEMOSSVertex> it = ps.getPicked().iterator();
		
		SEMOSSVertex [] vertices = new SEMOSSVertex[ps.getPicked().size()];
		
		//Need vertex to highlight when click in skeleton mode... Here we need to get the already selected vertices
		//so that we can add to them
		Hashtable vertHash = new Hashtable();
		VertexLabelFontTransformer vlft = null;
		if((ps3).searchPanel.btnHighlight.isSelected()) {
			vlft = (VertexLabelFontTransformer) viewer.getRenderContext().getVertexFontTransformer();
			vertHash=vlft.getVertHash();
		}

		SEMOSSVertex v;
		for(int vertIndex = 0;it.hasNext() && ( v = it.next()) != null;vertIndex++)
		{
			vertices[vertIndex] = v;
			//add selected vertices
			vertHash.put(v.getProperty(Constants.URI), v.getProperty(Constants.URI));
			
			logger.info(" Name  >>> " + v.getProperty(Constants.VERTEX_NAME));
			vst.setSelected(v.getURI());
			// this needs to invoke the property table model stuff

			VertexPropertyTableModel pm = new VertexPropertyTableModel(ps3.getFilterData(),v);
			table.setModel(pm);
			pm.fireTableDataChanged();
			logger.debug("Add this in - Prop Table");
		}
		if(ps3.searchPanel.btnHighlight.isSelected()) {
			if (vlft != null) {
				vlft.setVertHash(vertHash);
			}
			VertexPaintTransformer ptx = (VertexPaintTransformer)viewer.getRenderContext().getVertexFillPaintTransformer();
			ptx.setVertHash(vertHash);
		}
		
	}

}
