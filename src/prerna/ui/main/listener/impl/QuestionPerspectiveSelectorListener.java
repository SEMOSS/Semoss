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

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ComboboxToolTipRenderer;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.StringNumericComparator;

/**
 * Controls selection of the perspective from the left hand pane.
 */
public class QuestionPerspectiveSelectorListener extends AbstractListener {
	static final Logger logger = LogManager.getLogger(PerspectiveSelectionListener.class.getName());	
	
	// needs to find what is being selected from event
	// based on that refresh the view of questions for that given perspective
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> questionDBSelector = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.QUESTION_DB_SELECTOR);
		String engineName = (String)questionDBSelector.getSelectedItem();
		JTextField perspectiveField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_FIELD);
		JComboBox<String> questionOrderComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ORDER_COMBO_BOX);
		JRadioButton addQuestionButton = (JRadioButton) DIHelper.getInstance().getLocalProp(Constants.ADD_QUESTION_BUTTON);
		JLabel warning = (JLabel) DIHelper.getInstance().getLocalProp(Constants.QUESTION_XML_WARNING);
		JButton questionModButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_BUTTON);

		JComboBox bx = (JComboBox)e.getSource();
		String perspective = "";
		if(bx.getSelectedItem() != null) {
			perspective = bx.getSelectedItem().toString();
		}
		perspectiveField.setText(perspective);
		
		StringNumericComparator comparator = new StringNumericComparator();
		
		if(!perspective.isEmpty() && !perspective.equals("*NEW Perspective")) {
			JComboBox qp = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_SELECTOR);

			ArrayList tTip = new ArrayList();
			qp.removeAllItems();
			questionOrderComboBox.removeAllItems();
			
			String selectedVal = engineName;
			Vector<String> questionsV = new Vector<String>();

			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(selectedVal);
			try
			{
				questionsV = engine.getInsights(perspective);
				
				if(questionsV != null){
					int newQuestionOrder = 0;
					//recreate questionsV with appended order number
					Vector<String> questionsVCopy = new Vector<String>(questionsV);
					
					questionsV.clear();
					Vector<String> orderList = new Vector<String>();
					
					for(int itemIndex = 0;itemIndex < questionsVCopy.size();itemIndex++){
						//if the same question is used multiple times in different perspectives, vectorInsight will contain all those insights.
						//we need to loop through the insights and find the question that belongs to the perspective selected to get the correct order #
						Vector<Insight> vectorInsight = ((AbstractEngine)engine).getInsight2((String)questionsVCopy.get(itemIndex));
						Insight in = null;
						if(vectorInsight.size() > 1){
							for(Insight insight: vectorInsight){
								if(insight.getId().contains(perspective)){
									in = insight;
								}
							}
						} else {
							in = vectorInsight.get(0);
						}
						
						String order = in.getOrder();
						
						CSSApplication css = new CSSApplication(questionModButton, ".toggleButton");
						
						if(order!=null) {
							questionsV.add(order + ". " + questionsVCopy.get(itemIndex));
							orderList.add(order);
							warning.setVisible(false);
							questionModButton.setEnabled(true);
						}
						else {
							warning.setVisible(true);
							questionModButton.setEnabled(false);
							css = new CSSApplication(questionModButton, ".toggleButtonDisabled");

							String question = (String) questionsVCopy.get(itemIndex);
							questionsV.add(question);
							
							String[] questionsArray = question.split("\\. ", 2);
							orderList.add(questionsArray[0]);
						}
					}
					newQuestionOrder = orderList.size() + 1;
					orderList.add(newQuestionOrder+"");
					Collections.sort(orderList, comparator);
					
					DefaultComboBoxModel<String> model = new DefaultComboBoxModel<String>(orderList);
					questionOrderComboBox.setModel(model);
					if(addQuestionButton.isSelected()){
						questionOrderComboBox.setSelectedItem(newQuestionOrder);
					}
					else{
						//removes the extra number (new question indicator) in the combobox used in the Add functionality; users don't need this number when editing questions
						if(questionOrderComboBox.getItemCount()-(newQuestionOrder-1) == 1){
							model.removeElement(questionOrderComboBox.getItemCount()-1);
						}
					}
				}
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
			
			
			if(questionsV != null)
			{
				Collections.sort(questionsV, comparator);
				for(int itemIndex = 0;itemIndex < questionsV.size();itemIndex++)
				{
					String question = (String) questionsV.get(itemIndex);

					tTip.add(question);
					ComboboxToolTipRenderer renderer = new ComboboxToolTipRenderer();
					qp.setRenderer(renderer);
					renderer.setTooltips(tTip);
					renderer.setBackground(Color.WHITE);
					qp.addItem(question);
				}
			}
		}
		else{
			questionOrderComboBox.removeAllItems();
			questionOrderComboBox.addItem("1");
		}		
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}


}
