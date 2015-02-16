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
import java.util.List;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.ListModel;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class QuestionModButtonListener implements IChakraListener {
	JRadioButton addQuestionRadioButton = new JRadioButton();
	JRadioButton editQuestionRadioButton = new JRadioButton();
	JRadioButton deleteQuestionRadioButton = new JRadioButton();
	JTextField questionPerspectiveField = new JTextField();
	JTextField questionField = new JTextField();
	JTextField questionLayoutField = new JTextField();
	JTextPane questionSparql = new JTextPane();
	JTextPane parameterDependTextPane = new JTextPane();
	JTextPane parameterQueryTextPane = new JTextPane();
	JList<String> parameterDependList = new JList<String>();
	JList<String> parameterQueryList = new JList<String>();
	JList<String> parameterOptionList = new JList<String>();
	JComboBox<String> questionDBSelector = new JComboBox<String>();
	JComboBox<String> questionSelector = new JComboBox<String>();
	JComboBox<String> questionPerspectiveSelector = new JComboBox<String>();
	JComboBox<String> questionOrderComboBox = new JComboBox<String>();
	ArrayList<String> questionList = new ArrayList<String>();
	String selectedPerspective = null;
	String questionModType = null;

	String engineName = null;
	Vector<String> parameterDependListVector = new Vector<String>();
	Vector<String> parameterQueryListVector = new Vector<String>();
	Vector<String> parameterOptionListVector = new Vector<String>();

	ListModel<String> dependModel = null;
	ListModel<String> queryModel = null;
	ListModel<String> optionModel = null;

	String perspective, question, layout, sparql, questionKey,
			questionDescription, order;

	IEngine engine = null;
	QuestionAdministrator questionAdmin = null;
	Insight in = null;

	String xmlFile = null;
	String baseFolder = null;

	boolean existingPerspective = false;

	private void reloadDB(){
		//selects the db in repolist so the questions refresh with the changes
		//selects the db in repolist so the questions refresh with the changes
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		List selectedList = list.getSelectedValuesList();
		String selectedValue = selectedList.get(selectedList.size()-1).toString();
		
		//don't need to refresh if selected db is not the db you're modifying. when you click to it it will refresh anyway.
		if(engineName.equals(selectedValue)){
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(selectedValue);
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);
			
			JComboBox<String> box = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();
			
			for(int itemIndex = 0;itemIndex < perspectives.size(); itemIndex++) {
				box.addItem(perspectives.get(itemIndex).toString());
			}
		}
	}
	
	public void getFieldData() {
		addQuestionRadioButton = (JRadioButton) DIHelper.getInstance()
				.getLocalProp(Constants.ADD_QUESTION_BUTTON);

		editQuestionRadioButton = (JRadioButton) DIHelper.getInstance()
				.getLocalProp(Constants.EDIT_QUESTION_BUTTON);

		deleteQuestionRadioButton = (JRadioButton) DIHelper.getInstance()
				.getLocalProp(Constants.DELETE_QUESTION_BUTTON);

		questionPerspectiveField = (JTextField) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_PERSPECTIVE_FIELD);
		// JTextField questionKeyField = (JTextField) DIHelper.getInstance()
		// .getLocalProp(Constants.QUESTION_KEY_FIELD);
		questionField = (JTextField) DIHelper.getInstance().getLocalProp(
				Constants.QUESTION_FIELD);

		questionLayoutField = (JTextField) DIHelper.getInstance().getLocalProp(
				Constants.QUESTION_LAYOUT_FIELD);

		questionSparql = (JTextPane) DIHelper.getInstance().getLocalProp(
				Constants.QUESTION_SPARQL_TEXT_PANE);

		parameterDependTextPane = (JTextPane) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_DEPEND_TEXT_PANE);

		parameterQueryTextPane = (JTextPane) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_QUERY_TEXT_PANE);
		parameterDependList = (JList<String>) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);
		parameterQueryList = (JList<String>) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_QUERIES_JLIST);
		parameterOptionList = (JList<String>) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_OPTIONS_JLIST);
		questionDBSelector = (JComboBox<String>) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_DB_SELECTOR);
		questionSelector = (JComboBox<String>) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_MOD_SELECTOR);
		questionPerspectiveSelector = (JComboBox<String>) DIHelper
				.getInstance().getLocalProp(
						Constants.QUESTION_PERSPECTIVE_SELECTOR);
		questionOrderComboBox = (JComboBox<String>) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_ORDER_COMBO_BOX);

		engineName = (String) questionDBSelector.getSelectedItem();

		dependModel = parameterDependList.getModel();
		queryModel = parameterQueryList.getModel();
		optionModel = parameterOptionList.getModel();

		// if dependencies are added, store all of them in an arraylist and add
		// them to the question later by going through the arraylist
		if (!(dependModel.getSize() == 0)) {
			parameterDependListVector.clear();
			for (int i = 0; i < dependModel.getSize(); i++) {
				parameterDependListVector.add(dependModel.getElementAt(i));
			}
		} else {
			parameterDependListVector.clear();
		}

		// if parameter queries are added store all of them in an arraylist and
		// add them to the question later by going through the arraylist
		if (!(queryModel.getSize() == 0)) {
			parameterQueryListVector.clear();

			for (int i = 0; i < queryModel.getSize(); i++) {
				parameterQueryListVector.add(queryModel.getElementAt(i));
			}
		} else {
			parameterQueryListVector.clear();
		}

		if (!(optionModel.getSize() == 0)) {
			parameterOptionListVector.clear();

			for (int i = 0; i < optionModel.getSize(); i++) {
				parameterOptionListVector.add(optionModel.getElementAt(i));
			}
		} else {
			parameterOptionListVector.clear();
		}

		order = (String) questionOrderComboBox.getSelectedItem();
		perspective = questionPerspectiveField.getText();
		if (!questionField.getText().equals("")) {
			question = questionField.getText();
		}
		layout = questionLayoutField.getText();
		sparql = questionSparql.getText().replace("\n", "").replace("\r", "")
				.replace("\t", ""); // removes carriage returns and odd
									// characters to avoid malformed query
									// issues
		questionKey = null;
		questionDescription = null;
		engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		xmlFile = "db/" + engineName + "/" + engineName + "_Questions.XML";
		baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		selectedPerspective = (String) questionPerspectiveSelector
				.getSelectedItem();

		DefaultComboBoxModel questionListModel = (DefaultComboBoxModel) questionSelector
				.getModel();

		questionList.clear();
		if (!selectedPerspective.equals("*NEW Perspective")) {
			for (int i = 0; i < questionListModel.getSize(); i++) {
				String[] questionSplit = ((String) questionListModel.getElementAt(i)).split("\\. ", 2);
				questionList.add(questionSplit[1]);
			}
		}
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		JButton button = (JButton) e.getSource();
		String modificationType = button.getText() + "";
		questionModType = modificationType;

		// populate the fields with data based on question
		getFieldData();
		
		questionAdmin = new QuestionAdministrator(engine, questionList,
				selectedPerspective, questionModType);

		// check to see if perspective already exists in the perspective
		// drop-down
		for (int i = 0; i < questionPerspectiveSelector.getItemCount(); i++) {
			if (questionPerspectiveSelector.getItemAt(i).equals(perspective)) {
				existingPerspective = true;
				questionAdmin.existingPerspective = true;
				
				if (existingPerspective) {
					for (int j = 0; j < questionSelector.getItemCount(); j++) {
						String question = questionSelector.getItemAt(j);
						String[] questionSplit = question.split("\\. ", 2);
						question = questionSplit[1];

						Insight in = ((AbstractEngine) engine).getInsight2(question)
								.get(0);

						String questionID = in.getId();
						String[] questionIDArray = questionID.split(":");
						String currentQuestionKey = questionIDArray[2];

						// checks if there has been any auto-generated question
						// key
						if (currentQuestionKey.contains(perspective)) {
							questionAdmin.existingAutoGenQuestionKey = true;
							break;
						}
					}
				}
				break;
			}
			else {
				existingPerspective = false;
				questionAdmin.existingPerspective = false;
			}
		}
		
		// get the perspectives from the combobox
		DefaultComboBoxModel model = (DefaultComboBoxModel) questionPerspectiveSelector
				.getModel();

		// if the user wants to edit/delete then get the missing data
		// (questionKey and questionDescription) from insight
		if (!(modificationType.equals("Add Question") && addQuestionRadioButton
				.isSelected())) {
			in = ((AbstractEngine) engine).getInsight2(
					QuestionAdministrator.currentQuestion).get(0);

			if (in.getDescription() != null) {
				questionDescription = in.getDescription();
				System.err.println(questionDescription);
			}

			String questionID = in.getId();
			if (questionID != "DN" && questionID != null) {
				String[] questionIDArray = questionID.split(":");
				questionKey = questionIDArray[2];
			}
		}

		if (modificationType.equals("Add Question")
				&& addQuestionRadioButton.isSelected()) {
			// check to make sure all fields are filled out, if not, throw a
			// warning
			// box.
			if ((perspective == null || perspective.equals(""))
					|| (question == null || question.equals(""))
					|| (layout == null || layout.equals(""))
					|| (sparql == null || sparql.equals(""))) {
				JOptionPane
						.showMessageDialog(null,
								"There are empty field(s). Please fill out all of the required fields.");
			} else if ((questionPerspectiveSelector.getSelectedItem()
					.equals("*NEW Perspective"))
					&& !(model.getIndexOf(perspective) == -1)
					&& !perspective.equals("*NEW Perspective")) {
				JOptionPane.showMessageDialog(null, perspective
						+ " already exists. Please select " + perspective
						+ " in the drop-down.");
			} else if (perspective.contains(" ")) {
				JOptionPane
						.showMessageDialog(null,
								"Perspective name cannot contain spaces. Please remove it.");
			} else if (!questionPerspectiveSelector.getSelectedItem().equals(
					perspective)
					&& !questionPerspectiveSelector.getSelectedItem().equals(
							"*NEW Perspective")) {
				JOptionPane
						.showMessageDialog(
								null,
								"To add a new perspective, please select \"*NEW Perspective\".\nTo change the perspective name, please select \"Edit Question\" as the modification type.");
			} else {
				//createQuestionKey();
				//Vector questionsVector = ((AbstractEngine) engine)
				//		.getInsights(perspective);
				questionKey = questionAdmin.createQuestionKey(perspective);

				questionAdmin.addQuestion(perspective, questionKey, order, question,
						sparql, layout, questionDescription,
						parameterDependListVector, parameterQueryListVector,
						parameterOptionListVector);

				emptyFields(questionPerspectiveField, questionField,
						questionLayoutField, questionSparql,
						parameterDependTextPane, parameterQueryTextPane,
						parameterDependList, parameterQueryList, parameterOptionList);

				questionAdmin.createQuestionXMLFile(xmlFile, baseFolder);
				// Refresh the questions by selecting the db again and
				// populating all of the perspectives/questions based on new
				// xmlfile/s
				String currentDBSelected = (String) questionDBSelector
						.getSelectedItem();
				questionDBSelector.setSelectedItem(currentDBSelected);

				//reload the db with modified questions
				reloadDB();
				
				JOptionPane.showMessageDialog(null,
						"The question has been added.");
			}
		} else if (modificationType.equals("Update Question")
				&& editQuestionRadioButton.isSelected()) {

			// check if perspective has changed and change qsKey here;

			if ((perspective == null || perspective.equals(""))
					|| (question == null || question.equals(""))
					|| (layout == null || layout.equals(""))
					|| (sparql == null || sparql.equals(""))
					|| (questionKey == null || questionKey.equals(""))) {
				JOptionPane
						.showMessageDialog(null,
								"There are empty field(s). Please fill out all of the required fields.");
			} else {
				if (((QuestionAdministrator.currentQuestion != null) && (QuestionAdministrator.currentQuestion
						.equals(question)))
						&& ((QuestionAdministrator.currentLayout != null) && (QuestionAdministrator.currentLayout
								.equals(layout)))
						&& ((QuestionAdministrator.currentParameterDependListVector != null) && (QuestionAdministrator.currentParameterDependListVector
								.equals(parameterDependListVector)))
						&& ((QuestionAdministrator.currentParameterQueryListVector != null) && (QuestionAdministrator.currentParameterQueryListVector
								.equals(parameterQueryListVector)))
						// &&
						// ((QuestionAdministrator.currentParameterOptionListArray!=null)
						// &&(QuestionAdministrator.currentParameterOptionListArray
						// .equals(parameterOptionListVector)))
						&& ((QuestionAdministrator.currentPerspective != null) && (QuestionAdministrator.currentPerspective
								.equals(perspective)))
						&& ((QuestionAdministrator.currentSparql != null) && (QuestionAdministrator.currentSparql
								.equals(sparql)))
						&& ((QuestionAdministrator.currentQuestionOrder.equals(order)))) {
					JOptionPane
							.showMessageDialog(null,
									"No modifications were found. Please modify the field/s and try again.");
				} else if (perspective.contains(" ")) {
					JOptionPane
							.showMessageDialog(null,
									"Perspective name cannot contain spaces. Please remove it.");
				} else {
					if (!perspective.equals(questionPerspectiveSelector
							.getSelectedItem())) {
						String originalPerspective = (String) questionPerspectiveSelector.getSelectedItem();
						int dialogButton = JOptionPane.YES_NO_OPTION;
						int dialogResult = JOptionPane.showConfirmDialog(
								null,
								"Changing the perspective name will remove it from "
										+ questionPerspectiveSelector
												.getSelectedItem()
										+ " and add it to " + perspective
										+ ". Continue?", "Warning",
								dialogButton);
						if (dialogResult == JOptionPane.YES_OPTION) {
							// change the question key here and the order in the
							// question before passing it into the method

							// need to set the perspective to the new
							// perspective
							// if(existing perspective)
							//createQuestionKey();
							questionKey = questionAdmin.createQuestionKey(perspective);

							if (existingPerspective) {
								questionPerspectiveSelector
										.setSelectedItem(perspective);
								String newOrderNumber = questionSelector
										.getItemCount() + 1 + "";
								questionPerspectiveSelector.setSelectedItem(originalPerspective);
								questionSelector.setSelectedItem(question);
								//QuestionAdministrator.currentNumberofQuestions = Integer.toString(questionSelector.getItemCount());
								
								order = newOrderNumber;
							} 
							else {
								order = "1";
							}

							questionAdmin.modifyQuestion(perspective,
									questionKey, order, question, sparql, layout,
									questionDescription,
									parameterDependListVector,
									parameterQueryListVector,
									parameterOptionListVector);

							questionAdmin.createQuestionXMLFile(xmlFile,
									baseFolder);
							// Refresh the questions by selecting the db again
							// and populating all of the perspectives/questions
							// based on new xmlfile/s
							String currentDBSelected = (String) questionDBSelector
									.getSelectedItem();
							questionDBSelector
									.setSelectedItem(currentDBSelected);

							//reload db with modified questions
							reloadDB();
							
							JOptionPane.showMessageDialog(null,
									"The question has been updated.");
						}
					} else {
						questionAdmin.modifyQuestion(perspective, questionKey, order,
								question, sparql, layout, questionDescription,
								parameterDependListVector,
								parameterQueryListVector,
								parameterOptionListVector);

						questionAdmin
								.createQuestionXMLFile(xmlFile, baseFolder);
						// Refresh the questions by selecting the db again and
						// populating all of the perspectives/questions based on
						// new xmlfile/s
						String currentDBSelected = (String) questionDBSelector
								.getSelectedItem();
						questionDBSelector.setSelectedItem(currentDBSelected);

						JOptionPane.showMessageDialog(null,
								"The question has been updated.");
					}
				}
			}
		} else if (modificationType.equals("Delete Question")
				&& deleteQuestionRadioButton.isSelected()) {
			if ((perspective == null || perspective.equals(""))
					|| (question == null || question.equals(""))
					|| (layout == null || layout.equals(""))
					|| (sparql == null || sparql.equals(""))
					|| (questionKey == null || questionKey.equals(""))) {
				JOptionPane
						.showMessageDialog(null,
								"There are empty field(s). All required fields must be filled in.");
			} else {
				int dialogButton = JOptionPane.YES_NO_OPTION;
				int dialogResult = JOptionPane.showConfirmDialog(null,
						"Do you want to delete this question?", "Warning",
						dialogButton);

				if (dialogResult == JOptionPane.YES_OPTION) {
					questionAdmin
							.deleteQuestion(perspective, questionKey, order, question,
									sparql, layout, questionDescription,
									parameterDependListVector,
									parameterQueryListVector,
									parameterOptionListVector);

					questionAdmin.createQuestionXMLFile(xmlFile, baseFolder);
					// Refresh the questions by selecting the db again and
					// populating all of the perspectives/questions based on new
					// xmlfile/s
					String currentDBSelected = (String) questionDBSelector
							.getSelectedItem();
					questionDBSelector.setSelectedItem(currentDBSelected);

					//reload db with modified questions
					reloadDB();
					
					JOptionPane.showMessageDialog(null,
							"The question has been deleted.");
				}
			}
		}
	}

	private void emptyFields(JTextField perspectiveField,
			JTextField questionField, JTextField layoutField, JTextPane sparql,
			JTextPane dependencyTextPane, JTextPane parameterQueryTextPane,
			JList<String> dependencyList, JList<String> queryList, JList<String> optionList) {
		Vector<String> listData = new Vector<String>();

		perspectiveField.setText("");
		questionField.setText("");
		layoutField.setText("");
		sparql.setText("");
		dependencyTextPane.setText("");
		parameterQueryTextPane.setText("");
		dependencyList.setListData(listData);
		queryList.setListData(listData);
		optionList.setListData(listData);
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}
}
