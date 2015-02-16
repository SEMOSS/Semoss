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

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JTextField;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;

public class QuestionPlaySheetListListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> questionLayoutListComboBox = (JComboBox<String>)e.getSource();
		String selectedLayoutName = (String) questionLayoutListComboBox.getSelectedItem();
		JButton questionModButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_BUTTON);

		if(selectedLayoutName!=null){
			String selectedLayoutClass = PlaySheetEnum.getClassFromName(selectedLayoutName);
			JTextField questionLayoutField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_LAYOUT_FIELD);
			JLabel questionLayoutFieldLabel = (JLabel) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_PLAYSHEET_COMBO_LABEL);
			
			if(!selectedLayoutClass.equals("")){
				questionLayoutField.setText(selectedLayoutClass);
			}
			
			if(questionLayoutListComboBox.getSelectedItem().equals("*Custom_PlaySheet")){
				questionLayoutField.setVisible(true);
				questionLayoutFieldLabel.setVisible(true);
				
				if(questionModButton.getText().equals("Add Question")){
					questionLayoutField.setText("");
				}
			} else {
				questionLayoutField.setVisible(false);
				questionLayoutFieldLabel.setVisible(false);
				System.err.println(selectedLayoutClass);
			}
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
