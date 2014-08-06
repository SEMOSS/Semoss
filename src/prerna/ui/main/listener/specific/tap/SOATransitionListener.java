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

import java.awt.event.ActionEvent;

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.SOAAnalysisPerformer;

/**
 */
public class SOATransitionListener  implements IChakraListener{

	public static SOATransitionListener listener = null;
	static final Logger logger = LogManager.getLogger(SOATransitionListener.class.getName());
	GraphPlaySheet playSheet;

	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		Runnable playRunner = null;
		playRunner = new SOAAnalysisPerformer(playSheet);
		// thread
		Thread playThread = new Thread(playRunner);
		playThread.start();		
	}
	
	/**
	 * Method setPlaySheet.
	 * @param playSheet GraphPlaySheet
	 */
	public void setPlaySheet (GraphPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}


}
