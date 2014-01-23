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
import prerna.ui.components.specific.tap.DataLatencyPlayPopup;

/**
 */
public class DataLatencyPausePlayButtonListener implements ActionListener{

	IPlaySheet ps;
	DataLatencyPlayPopup pop;
	
	/**
	 * Constructor for DataLatencyPausePlayButtonListener.
	 * @param p 		IPlaySheet
	 * @param popup 	DataLatencyPlayPopup
	 */
	public DataLatencyPausePlayButtonListener(IPlaySheet p, DataLatencyPlayPopup popup){
		ps = p;
		pop = popup;
	}

	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JButton button = (JButton) e.getSource();
		String buttonText = button.getText();
		if(buttonText.equalsIgnoreCase("Pause")){
			//pause the thread and set the text to play
			pop.thread.suspend();
			button.setText("Play");
		}
		else if (buttonText.equalsIgnoreCase("Play")){
			//play the thread and set text to pause
			if(pop.thread!=null){
				if(pop.thread.isAlive())
					pop.thread.resume();
				else {
					Thread popup = new Thread(pop);
					pop.setThread(popup);
					popup.start();
				}
			}
			else {
				Thread popup = new Thread(pop);
				pop.setThread(popup);
				popup.start();
			}
			button.setText("Pause");
		}
	}
}
