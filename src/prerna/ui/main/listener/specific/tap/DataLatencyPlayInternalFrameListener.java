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
package prerna.ui.main.listener.specific.tap;

import javax.swing.JInternalFrame;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.specific.tap.DataLatencyPlayPopup;

/**
 */
public class DataLatencyPlayInternalFrameListener implements InternalFrameListener {

	Thread thread;
	static final Logger logger = LogManager.getLogger(DataLatencyPlayInternalFrameListener.class.getName());
	
	/**
	 * Method internalFrameClosed.
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameClosed(InternalFrameEvent e) {
		//Kill the thread
		//close the window
		logger.info("Begin");
		JInternalFrame jf = e.getInternalFrame();
		DataLatencyPlayPopup pop = (DataLatencyPlayPopup)jf;
		pop.ps.resetTransformers();
		pop.thread.stop();
		//Thread.currentThread().stop();
		logger.info("Complete ");
	}

	/**
	 * Override method from InternalFrameListener
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameActivated(InternalFrameEvent e) {
		
	}
	
	/**
	 * Override method from InternalFrameListener
	 * @param e InternalFrameEvent
	 */
	@Override
	public void internalFrameClosing(InternalFrameEvent e) {
		
	}

	/**
	 * Override method from InternalFrameListener
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeactivated(InternalFrameEvent arg0) {

	}

	/**
	 * Override method from InternalFrameListener
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameDeiconified(InternalFrameEvent arg0) {

	}

	/**
	 * Override method from InternalFrameListener
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameIconified(InternalFrameEvent arg0) {

	}

	/**
	 * Override method from InternalFrameListener
	 * @param arg0 InternalFrameEvent
	 */
	@Override
	public void internalFrameOpened(InternalFrameEvent arg0) {

	}
}
