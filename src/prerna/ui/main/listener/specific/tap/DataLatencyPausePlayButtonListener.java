/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
