/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.ui.components.MapComboBoxRenderer;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.StringNumericComparator;
import prerna.util.Utility;

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
			Vector<String> questionIds = new Vector<String>();
			Vector<String> questionNames = null;

			IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(selectedVal);
			IProject project = Utility.getProject(engine.getEngineId());
			try
			{
				questionIds = project.getInsights(perspective);
				
				if(questionIds != null){
					int newQuestionOrder = 0;
					//recreate questionsV with appended order number
					questionNames = new Vector<String>();
					
					Vector<String> orderList = new Vector<String>();
					
					Vector<Insight> vectorInsight = project.getInsight(questionIds.toArray(new String[questionIds.size()]));
					for(int itemIndex = 0;itemIndex < vectorInsight.size();itemIndex++){
						Insight in = vectorInsight.get(itemIndex);
						// modify values to include the number ordering for optimal user experience <- cause we care about that
//						newQuestionV.add(itemIndex + 1 + ". " + in.getInsightName());
//						newQuestionID.add(in.getInsightID());
//					}
//					
//					for(int itemIndex = 0;itemIndex < questionsVCopy.size();itemIndex++){
//						//if the same question is used multiple times in different perspectives, vectorInsight will contain all those insights.
//						//we need to loop through the insights and find the question that belongs to the perspective selected to get the correct order #
//						Vector<Insight> vectorInsight = ((AbstractEngine)engine).getInsight((String)questionsVCopy.get(itemIndex));
//						Insight in = null;
//						if(vectorInsight.size() > 1){
//							for(Insight insight: vectorInsight){
////								if(insight.getId().contains(perspective)){
//									in = insight;
////								}
//							}
//						} else {
//							in = vectorInsight.get(0);
//						}
						
						String order = in.getOrder();
						
						CSSApplication css = new CSSApplication(questionModButton, ".toggleButton");
						
						if(order!=null) {
							questionNames.add(order + ". " + in.getInsightName());
							orderList.add(order);
							warning.setVisible(false);
							questionModButton.setEnabled(true);
						}
						else {
							warning.setVisible(true);
							questionModButton.setEnabled(false);
							css = new CSSApplication(questionModButton, ".toggleButtonDisabled");

							String question = in.getInsightName();
							questionNames.add(question);
							
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
				logger.error(Constants.STACKTRACE, ex);
			}
			
			
			if(questionNames != null)
			{
				Collections.sort(questionNames, comparator);
				for(int itemIndex = 0;itemIndex < questionNames.size();itemIndex++)
				{
					String question = (String) questionNames.get(itemIndex);
					String id = (String) questionIds.get(itemIndex);

					tTip.add(question);
					MapComboBoxRenderer renderer = new MapComboBoxRenderer();
					qp.setRenderer(renderer);
					renderer.setTooltips(tTip);
					renderer.setBackground(Color.WHITE);
					Map<String, String> comboMap = new HashMap<String, String>();
					comboMap.put(MapComboBoxRenderer.KEY, id);
					comboMap.put(MapComboBoxRenderer.VALUE, question);
					qp.addItem(comboMap);
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
