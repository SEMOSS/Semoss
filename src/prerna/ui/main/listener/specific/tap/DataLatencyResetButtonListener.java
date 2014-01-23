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
import java.awt.event.ActionListener;

import javax.swing.JButton;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.DataLatencyPlayPopup;

/**
 */
public class DataLatencyResetButtonListener implements ActionListener {

	GraphPlaySheet ps;
	DataLatencyPlayPopup pop;
	JButton pausePlayButton;
	
	/**
	 * Constructor for DataLatencyResetButtonListener.
	 * @param p IPlaySheet
	 * @param popup DataLatencyPlayPopup
	 * @param pausePlay JButton
	 */
	public DataLatencyResetButtonListener(IPlaySheet p, DataLatencyPlayPopup popup, JButton pausePlay){
		ps = (GraphPlaySheet) p;
		pop = popup;
		pausePlayButton = pausePlay;
	}

	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if (pausePlayButton.getText().equalsIgnoreCase("Pause"))
			pausePlayButton.setText("Play");
		if(pop.thread!=null){
			if(pop.thread.isAlive()){
				pop.setHoursValue(0.0);
				pop.thread.suspend();
			}
			else {
				pop.setHoursValue(0.0);
				//pop.repaint();
			}
		}
		else {
			pop.setHoursValue(0.0);
			//pop.repaint();
		}
		
	}

}
