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

import javax.swing.JInternalFrame;
import javax.swing.JTable;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ui.components.ControlData;
import prerna.ui.components.LabelTableModel;
import prerna.ui.components.TooltipTableModel;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls the play sheet.
 */
public class PlaySheetControlListener implements InternalFrameListener {

	static final Logger logger = LogManager.getLogger(PlaySheetControlListener.class.getName());
	
	/**
	 * TODO unused method
	 * Method internalFrameActivated.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {
		// get the playsheet that is being activated
		logger.info("Internal Frame Activated >>>> ");
		JInternalFrame jf = e.getInternalFrame();
		// get the filter data
		ControlData vfd = null;
		GraphPlaySheet ps = (GraphPlaySheet)jf;
		vfd = ps.getControlData();
		
		LabelTableModel model = new LabelTableModel(vfd);	
	
		logger.info("Label count is " + model.getRowCount());
		// get the table
		JTable table = (JTable) Utility.getDIHelperLocalProperty(Constants.LABEL_TABLE);
		table.setModel(model);
		//table.repaint();
		model.fireTableDataChanged();
		logger.debug("Added the Node filter table ");

		TooltipTableModel model2 = new TooltipTableModel(vfd);	
		// get the table
		JTable table2 = (JTable) Utility.getDIHelperLocalProperty(Constants.TOOLTIP_TABLE);
		// need to figure a way to put the renderer here
		//TableColumn col = table2.getColumnModel().getColumn(3);
		//table2.setDefaultRenderer(Double.class, new EdgeFilterRenderer());
		//table2.setDefaultEditor(Double.class, new EdgeFilterRenderer());
		table2.setModel(model2);
		//table.repaint();
		model2.fireTableDataChanged();
		logger.debug("Added the Edge filter table ");
		logger.info("Internal Frame Activated >>>> Complete ");

	}

	/**
	 * TODO unused method
	 * Method internalFrameClosed.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameClosed(InternalFrameEvent e) {
		// when closed
		// need to empty the tables
		// remove from the question playsheet store
		logger.info("Begin");
		JInternalFrame jf = e.getInternalFrame();
		GraphPlaySheet ps = (GraphPlaySheet)jf;
		
		// get the table
		TableModel model = new DefaultTableModel();
		JTable table = (JTable) Utility.getDIHelperLocalProperty(Constants.TOOLTIP_TABLE);
		table.setModel(model);
		JTable table3 = (JTable) Utility.getDIHelperLocalProperty(Constants.LABEL_TABLE);
		table3.setModel(model);
		
		logger.debug("Cleaned up the filter tables ");
		
		logger.debug("Disabled the extend and append ");
		logger.info("Complete ");
	}

	/**
	 * TODO unused method
	 * Method internalFrameClosing.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameClosing(InternalFrameEvent arg0) {

	}

	/**
	 * TODO unused method
	 * Method internalFrameDeactivated.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeactivated(InternalFrameEvent arg0) {

	}

	/**
	 * TODO unused method
	 * Method internalFrameDeiconified.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeiconified(InternalFrameEvent arg0) {

	}

	/**
	 * TODO unused method
	 * Method internalFrameIconified.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameIconified(InternalFrameEvent arg0) {

	}

	/**
	 * TODO unused method
	 * Method internalFrameOpened.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameOpened(InternalFrameEvent arg0) {

	}

}
