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
import javax.swing.JToggleButton;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;

import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * A class that controls the opening of a playsheet in the internal frame.
 */
public class BasicPlaySheetListener implements InternalFrameListener {
	public static BasicPlaySheetListener listener = null;
	Logger logger = Logger.getLogger(getClass());
	
	/**
	 * Constructor for BasicPlaySheetListener.
	 */
	protected BasicPlaySheetListener()
	{
		
	}
	/**
	 * Method getInstance.  Checks to retrieve an instance of the Basic Play sheet listener.
	
	 * @return BasicPlaySheetListener */
	public static BasicPlaySheetListener getInstance()
	{
		if(listener == null)
			listener = new BasicPlaySheetListener();
		return listener;
	}
	
	/**
	 * Method internalFrameActivated.  Gets the playsheet being activated and sets up the active playsheet.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {
		logger.info("Internal Frame Activated >>>> ");
		JInternalFrame jf = e.getInternalFrame();
		IPlaySheet ps = (IPlaySheet)jf;

		QuestionPlaySheetStore.getInstance().setActiveSheet(ps);
		
		logger.info("Internal Frame Activated >>>> Complete ");
	}
	
	/**
	 * Method internalFrameClosed.  Closes the open internal frame.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameClosed(InternalFrameEvent e) {
		// TODO Auto-generated method stub
		// when closed
		// need to empty the tables
		// remove from the question playsheet store
		logger.info("Begin");
		JInternalFrame jf = e.getInternalFrame();
		IPlaySheet ps = (IPlaySheet)jf;
		String questionID = ps.getQuestionID();
		
		// fill the nodetype list so that they can choose from
		// remove from store
		// this will also clear out active sheet
		QuestionPlaySheetStore.getInstance().remove(questionID);

		if(QuestionPlaySheetStore.getInstance().isEmpty())
		{
			JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(
					Constants.SHOW_PLAYSHEETS_LIST);
			btnShowPlaySheetsList.setEnabled(false);
		}
		logger.info("Complete ");
	}
	
	/**
	 * TODO unused method.
	 * Method internalFrameClosing.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameClosing(InternalFrameEvent arg0) {
		// TODO Auto-generated method stub
	}

	/**
	 * TODO unused method.
	 * Method internalFrameDeactivated.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeactivated(InternalFrameEvent arg0) {
		// TODO Auto-generated method stub
	}

	/**
	 * TODO unused method
	 * Method internalFrameDeiconified.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeiconified(InternalFrameEvent arg0) {
		// TODO Auto-generated method stub

	}

	/**
	 * TODO unused method
	 * Method internalFrameIconified.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameIconified(InternalFrameEvent arg0) {
		// TODO Auto-generated method stub

	}

	/**
	 * TODO unused method
	 * Method internalFrameOpened.
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameOpened(InternalFrameEvent arg0) {
		// TODO Auto-generated method stub
		logger.info("Internal Frame opened");
		// this should also enable the extend and overlay buttons

	}

}
