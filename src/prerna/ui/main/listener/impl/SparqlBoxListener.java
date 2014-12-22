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
package prerna.ui.main.listener.impl;

import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;

import javax.swing.JComponent;
import javax.swing.JTextArea;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 */
public class SparqlBoxListener extends AbstractListener implements FocusListener {
	// when focused in on sparql area, set all text to plain font and black
	/**
	 * Method focusGained.  When focused
	 * @param e FocusEvent
	 */
	@Override 
	public void focusGained(FocusEvent e) {
		JTextArea area = (JTextArea) DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
		// if playsheet hint is in the sparql area, delete it when focus is gained
		if(area.getText().contains("Hint: ")){
			area.setText("");
		}
		area.setFont(new Font("Tahoma", Font.PLAIN, 11));
		area.setForeground(Color.BLACK);
	}
	
	// when not focused on sparql area and the area is filled with a play sheet hint
	// set all text to italics and gray
	/**
	 * Method focusLost.
	 * @param e FocusEvent
	 */
	@Override
	public void focusLost(FocusEvent e) {
		JTextArea area = (JTextArea) DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
		if(area.getText().contains("Hint: ")){
			area.setFont(new Font("Tahoma", Font.ITALIC, 11));
			area.setForeground(Color.GRAY);
		}
	}

	/**
	 * Method setView.
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}

	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		
	}
	
}
