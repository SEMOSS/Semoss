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

import javax.swing.JButton;
import javax.swing.JInternalFrame;
import javax.swing.JTable;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.apache.log4j.Logger;

import prerna.ui.components.ControlData;
import prerna.ui.components.LabelTableModel;
import prerna.ui.components.TooltipTableModel;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * Controls the play sheet.
 */
public class PlaySheetControlListener implements InternalFrameListener {

	public static PlaySheetControlListener listener = null;
	Logger logger = Logger.getLogger(getClass());
	
	/**
	 * Constructor for PlaySheetControlListener.
	 */
	protected PlaySheetControlListener()
	{
		
	}
	
	/**
	 * Method getInstance. Gets the instance of the play sheet control listener.	
	 * @return PlaySheetControlListener */
	public static PlaySheetControlListener getInstance()
	{
		if(listener == null)
			listener = new PlaySheetControlListener();
		return listener;
	}
	
	
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
		GraphPlaySheet ps = (GraphPlaySheet)jf;
		// get the filter data
		ControlData vfd = ps.getControlData();
		
		LabelTableModel model = new LabelTableModel(vfd);	
	
		logger.info("Label count is " + model.getRowCount());
		// get the table
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.LABEL_TABLE);
		table.setModel(model);
		//table.repaint();
		model.fireTableDataChanged();
		logger.debug("Added the Node filter table ");

		TooltipTableModel model2 = new TooltipTableModel(vfd);	
		// get the table
		JTable table2 = (JTable)DIHelper.getInstance().getLocalProp(Constants.TOOLTIP_TABLE);
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
		String questionID = ps.getQuestionID();
		
		QuestionPlaySheetStore.getInstance().remove(questionID);
		if(QuestionPlaySheetStore.getInstance().isEmpty())
		{
			JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(
					Constants.SHOW_PLAYSHEETS_LIST);
			btnShowPlaySheetsList.setEnabled(false);
		}
		
		// get the table
		TableModel model = new DefaultTableModel();
		JTable table = (JTable)DIHelper.getInstance().getLocalProp(Constants.TOOLTIP_TABLE);
		table.setModel(model);
		JTable table3 = (JTable)DIHelper.getInstance().getLocalProp(Constants.LABEL_TABLE);
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
