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

import javax.swing.JInternalFrame;
import javax.swing.JTable;
import javax.swing.JToggleButton;
import javax.swing.event.InternalFrameEvent;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import prerna.ui.components.EdgeFilterTableModel;
import prerna.ui.components.VertexFilterData;
import prerna.ui.components.VertexFilterTableModel;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 */
public class GraphPlaySheetListener extends PlaySheetListener {

	/**
	 * TODO unused method
	 * Method internalFrameActivated.  Gets the playsheet that is being activated
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {
		// get the playsheet that is being activated
		super.internalFrameActivated(e);
		logger.info("Internal Frame Activated >>>> ");
		
		//now graphPlaySheet Specific Items
		JInternalFrame jf = e.getInternalFrame();
		GraphPlaySheet ps = (GraphPlaySheet)jf;
		// get the filter data
		VertexFilterData vfd = ps.getFilterData();
		VertexFilterTableModel model = new VertexFilterTableModel(vfd);	
		// get the table
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.FILTER_TABLE);
		table.setModel(model);
		//table.repaint();
		model.fireTableDataChanged();
		logger.debug("Added the Node filter table ");

		EdgeFilterTableModel model2 = new EdgeFilterTableModel(vfd);	
		// get the table
		JTable table2 = (JTable)DIHelper.getInstance().getLocalProp(Constants.EDGE_TABLE);
		// need to figure a way to put the renderer here
		table2.setModel(model2);
		model2.fireTableDataChanged();
		logger.debug("Added the Edge filter table ");
		
		// this should also enable the extend and overlay buttons
		JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
		append.setEnabled(true);
		CSSApplication css = new CSSApplication(append,".toggleButton");
		//append.repaint();
		logger.info("Internal Frame Activated >>>> Complete ");
		
	}

	/**
	 * TODO unused method
	 * Method internalFrameClosed.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameClosed(InternalFrameEvent e) {
		logger.info("Begin");
		super.internalFrameClosed(e);
		
		//now graphplaysheet specific things
		JInternalFrame jf = e.getInternalFrame();
		GraphPlaySheet ps = (GraphPlaySheet)jf;
		// get the table
		TableModel model = new DefaultTableModel();
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.FILTER_TABLE);
		table.setModel(model);
		JTable table3 = (JTable)DIHelper.getInstance().getLocalProp(Constants.EDGE_TABLE);
		table3.setModel(model);
		logger.debug("Cleaned up the filter tables ");
		// also clear the properties table
		JTable table2 = (JTable)DIHelper.getInstance().getLocalProp(Constants.PROP_TABLE);
		table2.setModel(model);
		
		//if the playsheet has a data latency popup associated with it, close it
		if(ps.dataLatencyPopUp!=null && !ps.dataLatencyPopUp.isClosed()){
			ps.dataLatencyPopUp.dispose();
		}
		//if the playsheet has a data latency play popup associated with it, close it
		if(ps.dataLatencyPlayPopUp!=null && !ps.dataLatencyPlayPopUp.isClosed()){
			ps.dataLatencyPlayPopUp.dispose();
		}
		
		logger.debug("Disabled the extend and append ");
		logger.info("Complete ");
	}


	/**TODO unused method
	 * Method internalFrameOpened.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameOpened(InternalFrameEvent arg0) {
		logger.info("Internal Frame opened");
		// this should also enable the extend and overlay buttons
		JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
		CSSApplication css = new CSSApplication(append,".toggleButton");
		append.setEnabled(true);

	}

}
