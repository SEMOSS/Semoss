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

import javax.swing.JInternalFrame;
import javax.swing.JTable;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.log4j.Logger;

import prerna.ui.components.OPropertyTableModel;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.PropertyTableModel;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the OWL for a play sheet.  Allows setting up of properties and relationships.
 */
public class PlaySheetOWLListener implements InternalFrameListener {

	Logger logger = Logger.getLogger(getClass());
	/**
	 * TODO unused method
	 * Method internalFrameActivated.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {
		// get the playsheet that is being activated
	
		logger.info("Internal Frame Activated OWL Manipulator>>>> ");
		JInternalFrame jf = e.getInternalFrame();
		GraphPlaySheet ps = (GraphPlaySheet)jf;
		if(!ps.getSudowl())
		{
			return;
		}
		// get the filter data
		PropertySpecData psd = ps.getPredicateData();
		OPropertyTableModel model = new OPropertyTableModel(psd);
	
		logger.info("Lable count is " + model.getRowCount());
		PropertyTableModel model2 = new PropertyTableModel(psd);
		// get the table
		JTable table2 = (JTable)DIHelper.getInstance().getLocalProp(Constants.DATA_PROP_TABLE);
		table2.setModel(model2);
		//table.repaint();
		model2.fireTableDataChanged();
		logger.debug("Added the Edge filter table ");
		logger.info("Internal Frame Activated OWL >>>> Complete ");
	}

	/**TODO unused method
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
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.OBJECT_PROP_TABLE);
		table.setModel(model);
		JTable table3 = (JTable)DIHelper.getInstance().getLocalProp(Constants.DATA_PROP_TABLE);
		table3.setModel(model);
		logger.debug("Cleaned up the filter tables ");
		
		logger.debug("Disabled the extend and append ");
		logger.info("Complete ");
	}

	/**TODO unused method
	 * Method internalFrameClosing.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameClosing(InternalFrameEvent arg0) {

	}

	/**TODO unused method
	 * Method internalFrameDeactivated.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeactivated(InternalFrameEvent arg0) {

	}

	/**TODO unused method
	 * Method internalFrameDeiconified.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeiconified(InternalFrameEvent arg0) {

	}

	/**TODO unused method
	 * Method internalFrameIconified.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameIconified(InternalFrameEvent arg0) {

	}

	/**TODO unused method
	 * Method internalFrameOpened.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameOpened(InternalFrameEvent arg0) {

	}

}
