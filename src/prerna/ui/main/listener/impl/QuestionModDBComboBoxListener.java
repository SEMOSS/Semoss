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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JRadioButton;

import prerna.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.helpers.EntityFillerForSubClass;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;

public class QuestionModDBComboBoxListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> questionModDBComboBox = (JComboBox<String>)e.getSource();
		String engineName = (String) questionModDBComboBox.getSelectedItem();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		JRadioButton addQuestionModType = (JRadioButton) DIHelper.getInstance().getLocalProp(Constants.ADD_QUESTION_BUTTON);

		//populate layout combobox
		JComboBox playSheetComboBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_PLAYSHEET_COMBOBOXLIST);
		playSheetComboBox.removeAllItems();
		
		playSheetComboBox.insertItemAt("*Custom_PlaySheet", 0);
		
		ArrayList playsheetArray = PlaySheetEnum.getAllSheetNames();
		for(int i = 0; i < playsheetArray.size(); i++){
			playSheetComboBox.addItem(playsheetArray.get(i));
		}
		
		//entity filler; this will populate the parameter combobox for users to select a parameter they want to bind
		ArrayList<JComboBox> boxes = new ArrayList<JComboBox>();
		ParamComboBox addParameterComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ADD_PARAMETER_COMBO_BOX);
		boxes.add(addParameterComboBox);
		
		EntityFillerForSubClass entityFillerSC = new EntityFillerForSubClass();
		entityFillerSC.boxes = boxes;
		entityFillerSC.engine = engine;
		entityFillerSC.parent = "Concept";
		Thread aThread = new Thread(entityFillerSC);
		aThread.start();

		//get the perspectives and store it
		Vector<String> perspectives = engine.getPerspectives();
		Collections.sort(perspectives);
		
		JComboBox<String> box = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_SELECTOR);
		box.removeAllItems();
		
		//populates perspectives based on selected db
		for(int itemIndex = 0;itemIndex < perspectives.size(); itemIndex++) {
			box.addItem(perspectives.get(itemIndex).toString());
		}
		if (addQuestionModType.isSelected()){
			box.insertItemAt("*NEW Perspective", 0);
			playSheetComboBox.setSelectedIndex(0);
		}
		box.setSelectedIndex(0);
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
