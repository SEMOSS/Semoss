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

import javax.swing.JToggleButton;
import javax.swing.event.InternalFrameEvent;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 */
public class GridPlaySheetListener extends PlaySheetListener {

	
	/**
	 * TODO unused method
	 * Method internalFrameActivated.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {

		super.internalFrameActivated(e);
		
		// this should also enable the extend and overlay buttons
		JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
		append.setEnabled(true);
		logger.info("Internal Frame Activated >>>> Complete ");
	}

	/**
	 * TODO unused method
	 * Method internalFrameClosed.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameClosed(InternalFrameEvent e) {
		super.internalFrameClosed(e);
		
	}

	@Override
	public void internalFrameOpened(InternalFrameEvent arg0) {
		logger.info("Internal Frame opened");
		// this should also enable the extend and overlay buttons
		JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
		append.setEnabled(true);
	}
}
