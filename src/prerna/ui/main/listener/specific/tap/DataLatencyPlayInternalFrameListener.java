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
