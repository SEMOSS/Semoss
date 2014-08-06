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

import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.VertexPropertyTableModel;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.ui.transformer.VertexShapeTransformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 * Controls what happens when a picked state occurs.
 */
public class PickedStateListener implements ItemListener {

	static final Logger logger = LogManager.getLogger(PickedStateListener.class.getName());
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
		PickedState pickedState = (PickedState) e.getSource();	
		
		GraphPlaySheet ps3 = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.PROP_TABLE);
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
		if(((GraphPlaySheet)ps3).searchPanel.btnHighlight.isSelected()) {
			vlft = (VertexLabelFontTransformer) viewer.getRenderContext().getVertexFontTransformer();
			vertHash=vlft.getVertHash();
		}

		for(int vertIndex = 0;it.hasNext();vertIndex++)
		{
			SEMOSSVertex v = it.next();
			vertices[vertIndex] = v;
			//add selected vertices
			vertHash.put(v.getProperty(Constants.URI), v.getProperty(Constants.URI));
			
			logger.info(" Name  >>> " + v.getProperty(Constants.VERTEX_NAME));
			vst.setSelected(v.getURI());
			// this needs to invoke the property table model stuff
			
			VertexPropertyTableModel pm = new VertexPropertyTableModel(ps3.getFilterData(),v);
			table.setModel(pm);
			//table.repaint();
			pm.fireTableDataChanged();
			logger.debug("Add this in - Prop Table");
		}
		if(ps3.searchPanel.btnHighlight.isSelected()) {
			vlft.setVertHash(vertHash);
			VertexPaintTransformer ptx = (VertexPaintTransformer)viewer.getRenderContext().getVertexFillPaintTransformer();
			ptx.setVertHash(vertHash);
		}
		
	}

}
