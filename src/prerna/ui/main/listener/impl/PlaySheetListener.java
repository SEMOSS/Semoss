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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 */
public class PlaySheetListener implements InternalFrameListener {

	static final Logger logger = LogManager.getLogger(PlaySheetListener.class.getName());
	
	/**
	 * TODO unused method
	 * Method internalFrameActivated.  Gets the playsheet that is being activated
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {
		logger.info("Internal Frame Activated >>>> ");
		JInternalFrame jf = e.getInternalFrame();
		IPlaySheet ps = (IPlaySheet)jf;

		QuestionPlaySheetStore.getInstance().setActiveSheet(ps);
		//always disable append unless specific playsheets are activated
		JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
		append.setEnabled(false);
		CSSApplication css = new CSSApplication(append,".toggleButtonDisabled");
		logger.info("Internal Frame Activated >>>> Complete ");
	}

	/**
	 * TODO unused method
	 * Method internalFrameClosed.
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
		//disable if lastsheet is closed
		if(QuestionPlaySheetStore.getInstance().size()==1)
		{
			JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
			append.setEnabled(false);
			CSSApplication css = new CSSApplication(append,".toggleButtonDisabled");
		}
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
