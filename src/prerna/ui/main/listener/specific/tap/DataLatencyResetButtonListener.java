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
