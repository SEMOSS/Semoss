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
