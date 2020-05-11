
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
/*
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.QuestionAdministrator;
import prerna.om.OldInsight;
import prerna.om.SEMOSSParam;
import prerna.sablecc2.reactor.legacy.playsheets.LegacyInsightDatabaseUtility;
import prerna.ui.components.MapComboBoxRenderer;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

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
	JComboBox<Map<String, String>> questionSelector = new JComboBox<Map<String, String>>();
	JComboBox<String> questionPerspectiveSelector = new JComboBox<String>();
	JComboBox<String> questionOrderComboBox = new JComboBox<String>();
	ArrayList<String> questionList = new ArrayList<String>();
	String selectedPerspective = null;
	String questionModType = null;

	String engineName = null;
	Vector<String> parameterDependListVector = new Vector<String>();
	Vector<String> parameterQueryListVector = new Vector<String>();
	Vector<String> parameterOptionListVector = new Vector<String>();

	// tracking the original/current insight information
	String currentPerspective;
	String currentQuestionKey;
	String currentQuestionOrder;
	String currentQuestion;
	String currentLayout;
	String currentSparql;
	Vector<String> currentParameterDependListVector;
	Vector<String> currentParameterQueryListVector;
	Vector<String> currentParameterOptionListVector;
	String currentNumberofQuestions;

	ListModel<String> dependModel = null;
	ListModel<String> queryModel = null;
	ListModel<String> optionModel = null;

	String perspective, question, layout, sparql,
			questionDescription, order;

	IEngine engine = null;
	QuestionAdministrator questionAdmin = null;
	OldInsight in = null;

	String xmlFile = null;
	String baseFolder = null;

	boolean existingPerspective = false;

	private void reloadDB() {
		// selects the db in repolist so the questions refresh with the changes
		// selects the db in repolist so the questions refresh with the changes
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		List selectedList = list.getSelectedValuesList();
		String selectedValue = selectedList.get(selectedList.size() - 1).toString();

		// don't need to refresh if selected db is not the db you're modifying.
		// when you click to it it will refresh anyway.
		if (engineName.equals(selectedValue)) {	IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(	selectedValue);
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);

			JComboBox<String> box = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();

			for (int itemIndex = 0; itemIndex < perspectives.size(); itemIndex++) {
				box.addItem(perspectives.get(itemIndex).toString());
			}
		}
	}

	public void getFieldData() {
		addQuestionRadioButton = (JRadioButton) DIHelper.getInstance().getLocalProp(Constants.ADD_QUESTION_BUTTON);

		editQuestionRadioButton = (JRadioButton) DIHelper.getInstance().getLocalProp(Constants.EDIT_QUESTION_BUTTON);

		deleteQuestionRadioButton = (JRadioButton) DIHelper.getInstance().getLocalProp(Constants.DELETE_QUESTION_BUTTON);

		questionPerspectiveField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_FIELD);
		// JTextField questionKeyField = (JTextField) DIHelper.getInstance()
		// .getLocalProp(Constants.QUESTION_KEY_FIELD);
		questionField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_FIELD);

		questionLayoutField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_LAYOUT_FIELD);

		questionSparql = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.QUESTION_SPARQL_TEXT_PANE);

		parameterDependTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_TEXT_PANE);

		parameterQueryTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_TEXT_PANE);
		parameterDependList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);
		parameterQueryList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERIES_JLIST);
		parameterOptionList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTIONS_JLIST);
		questionDBSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_DB_SELECTOR);
		questionSelector = (JComboBox<Map<String, String>>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_SELECTOR);
		questionPerspectiveSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(
						Constants.QUESTION_PERSPECTIVE_SELECTOR);
		questionOrderComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ORDER_COMBO_BOX);

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
		sparql = questionSparql.getText().replace("\n", "").replace("\r", "").replace("\t", ""); 
		// removes carriage returns and odd characters to avoid malformed query issues
		questionDescription = null;
		engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		xmlFile = "db/" + engineName + "/" + engineName + "_Questions.XML";
		baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		selectedPerspective = (String) questionPerspectiveSelector.getSelectedItem();

		DefaultComboBoxModel questionListModel = (DefaultComboBoxModel) questionSelector.getModel();

		questionList.clear();
		if (!selectedPerspective.equals("*NEW Perspective")) {
			for (int i = 0; i < questionListModel.getSize(); i++) {
				@SuppressWarnings("unchecked")
				Map<String, String> questionSplit = ((Map<String, String>) questionListModel.getElementAt(i));
				questionList.add(questionSplit.get(MapComboBoxRenderer.VALUE));
			}
		}

		// get the current insight values
		if (!questionModType.equals("Add Question")) {
			currentPerspective = (String) questionPerspectiveSelector.getSelectedItem();
			@SuppressWarnings("unchecked")
			Map<String, String> currentQuestionSplit = ((Map<String, String>) questionSelector.getSelectedItem());
			currentQuestion = currentQuestionSplit.get(MapComboBoxRenderer.VALUE);
			currentQuestionKey = currentQuestionSplit.get(MapComboBoxRenderer.KEY);

			in = (OldInsight) ((AbstractEngine) engine).getInsight(currentQuestionKey).get(0);

			currentQuestionOrder = in.getOrder();
			currentLayout = in.getOutput();
			currentSparql = in.getDataMakerComponents().get(0).getQuery();

			populateParamVectors();

			Vector<String> insights = engine.getInsights(currentPerspective);
			currentNumberofQuestions = insights.size() + "";
		}
	}

	public void populateParamVectors() {
		List<SEMOSSParam> paramInfoVector = LegacyInsightDatabaseUtility.getParamsFromInsightId(engine.getInsightDatabase(), currentQuestionKey);

		// if there are params, get any related data (queries/dependencies)
		// and store them
		if (!paramInfoVector.isEmpty()) {
			for (int i = 0; i < paramInfoVector.size(); i++) {
				if (paramInfoVector.get(i).getQuery() != null && !paramInfoVector.get(i).getQuery().equals(DIHelper.getInstance().getProperty("TYPE" + "_" + Constants.QUERY))) {
					currentParameterQueryListVector.add(paramInfoVector.get(i).getName()+ "_QUERY_-_"+ paramInfoVector.get(i).getQuery());
				}

				if (!paramInfoVector.get(i).getDependVars().isEmpty()&& !paramInfoVector.get(i).getDependVars().get(0).equals("None")) {
					for (int j = 0; j < paramInfoVector.get(i).getDependVars().size(); j++) {
						currentParameterDependListVector.add(paramInfoVector.get(i).getName()+ "_DEPEND_-_" + paramInfoVector.get(i).getDependVars().get(j));
					}
				}

				if (paramInfoVector.get(i).getOptions() != null	&& !paramInfoVector.get(i).getOptions().isEmpty()) {
					Vector options = paramInfoVector.get(i).getOptions();
					String optionsConcat = "";
					for (int j = 0; j < options.size(); j++) {
						optionsConcat += options.get(j);
						if (j != options.size() - 1) {
							optionsConcat += ";";
						}
					}
					currentParameterOptionListVector.add(paramInfoVector.get(i).getType() + "_OPTION_-_" + optionsConcat);
				}
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

		questionAdmin = new QuestionAdministrator((AbstractEngine) engine);

		// get the perspectives from the combobox
		@SuppressWarnings("rawtypes")
		DefaultComboBoxModel model = (DefaultComboBoxModel) questionPerspectiveSelector.getModel();

		// if the user wants to edit/delete then get the missing data
		// (questionKey and questionDescription) from insight
		if (!(modificationType.equals("Add Question") && addQuestionRadioButton.isSelected())) {
			in = (OldInsight) ((AbstractEngine) engine).getInsight(currentQuestionKey).get(0);
			in.getDataMakerComponents().get(0).setQuery(sparql);
		}

		if (modificationType.equals("Add Question")	&& addQuestionRadioButton.isSelected()) {
			// check to make sure all fields are filled out, if not, throw a warning box.
			if ((perspective == null || perspective.equals(""))	|| (question == null || question.equals(""))|| (layout == null || layout.equals(""))|| (sparql == null || sparql.equals(""))) {
				JOptionPane.showMessageDialog(null, "There are empty field(s). Please fill out all of the required fields.");
			} else if ((questionPerspectiveSelector.getSelectedItem().equals("*NEW Perspective"))&& !(model.getIndexOf(perspective) == -1)&& !perspective.equals("*NEW Perspective")) {
				JOptionPane.showMessageDialog(null, perspective	+ " already exists. Please select " + perspective + " in the drop-down.");
			} else if (!questionPerspectiveSelector.getSelectedItem().equals(perspective)&& !questionPerspectiveSelector.getSelectedItem().equals("*NEW Perspective")) {
				JOptionPane.showMessageDialog(null, "To add a new perspective, please select \"*NEW Perspective\".\nTo change the perspective name, please select \"Edit Question\" as the modification type.");
			} else {
				// createQuestionKey();
				// Vector questionsVector = ((AbstractEngine) engine)
				// .getInsights(perspective);
				// questionKey = questionAdmin.createQuestionKey(perspective);
				List<DataMakerComponent> dmcs = new Vector<DataMakerComponent>();
				dmcs.add(new DataMakerComponent(engine, sparql));
				Map<String, String> paramHash = Utility.getParamTypeHash(sparql);
				
				List<SEMOSSParam> params = generateSEMOSSParamObjects(parameterDependListVector, parameterQueryListVector, parameterOptionListVector, paramHash);

				questionAdmin.addQuestion(question, perspective, dmcs, layout, order, "", true, null, params, null);

				emptyFields(questionPerspectiveField, questionField, questionLayoutField, questionSparql, parameterDependTextPane, parameterQueryTextPane, parameterDependList, parameterQueryList, parameterOptionList);

				// Refresh the questions by selecting the db again and populating all of the perspectives/questions based on new xml files
				String currentDBSelected = (String) questionDBSelector.getSelectedItem();
				questionDBSelector.setSelectedItem(currentDBSelected);

				// reload the db with modified questions
				reloadDB();

				JOptionPane.showMessageDialog(null, "The question has been added.");
			}
		} else if (modificationType.equals("Update Question") && editQuestionRadioButton.isSelected()) {

			// check if perspective has changed and change qsKey here;

			if ((perspective == null || perspective.equals(""))	|| (question == null || question.equals("")) || (layout == null || layout.equals("")) || (sparql == null || sparql.equals("")) || (currentQuestionKey == null || currentQuestionKey.equals(""))) {
				JOptionPane.showMessageDialog(null,	"There are empty field(s). Please fill out all of the required fields.");
			} else {
				if (((currentQuestion != null) && (currentQuestion.equals(question)))
						&& ((currentLayout != null) && (currentLayout.equals(layout)))
						&& ((currentParameterDependListVector != null) && (currentParameterDependListVector.equals(parameterDependListVector)))
						&& ((currentParameterQueryListVector != null) && (currentParameterQueryListVector.equals(parameterQueryListVector)))
						&& ((currentPerspective != null) && (currentPerspective.equals(perspective)))
						&& ((currentSparql != null) && (currentSparql.equals(sparql)))
						&& ((currentQuestionOrder.equals(order)))) {
					JOptionPane.showMessageDialog(null,"No modifications were found. Please modify the field/s and try again.");
				} else {
					if (!perspective.equals(questionPerspectiveSelector.getSelectedItem())) {
						String originalPerspective = (String) questionPerspectiveSelector.getSelectedItem();
						int dialogButton = JOptionPane.YES_NO_OPTION;
						int dialogResult = JOptionPane.showConfirmDialog(null, "Changing the perspective name will remove it from " + questionPerspectiveSelector.getSelectedItem() + " and add it to " + perspective + ". Continue?", "Warning", dialogButton);
						if (dialogResult == JOptionPane.YES_OPTION) {
							// change the question key here and the order in the question before passing it into the method need to set the perspective to the new
							// perspective if(existing perspective) createQuestionKey();

							if (existingPerspective) {
								questionPerspectiveSelector.setSelectedItem(perspective);
								String newOrderNumber = questionSelector.getItemCount() + 1 + "";
								questionPerspectiveSelector.setSelectedItem(originalPerspective);
								questionSelector.setSelectedItem(question);
								// QuestionAdministrator.currentNumberofQuestions
								// =
								// Integer.toString(questionSelector.getItemCount());

								order = newOrderNumber;
							} else {
								order = "1";
							}
							List<DataMakerComponent> comps = in.getDataMakerComponents();
							comps.get(0).setQuery(sparql);
							Map<String, String> paramHash = Utility.getParamTypeHash(sparql);
							
							List<SEMOSSParam> params = generateSEMOSSParamObjects(parameterDependListVector, parameterQueryListVector, parameterOptionListVector, paramHash);

							questionAdmin.modifyQuestion(currentQuestionKey, question, perspective, comps, layout, order, "", true, null, params, null);

							// Refresh the questions by selecting the db again
							// and populating all of the perspectives/questions
							// based on new xmlfile/s
							String currentDBSelected = (String) questionDBSelector.getSelectedItem();
							questionDBSelector.setSelectedItem(currentDBSelected);

							// reload db with modified questions
							reloadDB();

							JOptionPane.showMessageDialog(null, "The question has been updated.");
						}
					} else {
						List<DataMakerComponent> comps = in.getDataMakerComponents();
						comps.get(0).setQuery(sparql);

						Map<String, String> paramHash = Utility.getParamTypeHash(sparql);
						
						List<SEMOSSParam> params = generateSEMOSSParamObjects(parameterDependListVector, parameterQueryListVector, parameterOptionListVector, paramHash);

						questionAdmin.modifyQuestion(currentQuestionKey, question, perspective, comps, layout, order, "", true, null, params, null);

						// Refresh the questions by selecting the db again and
						// populating all of the perspectives/questions based on
						// new xmlfile/s
						String currentDBSelected = (String) questionDBSelector.getSelectedItem();
						questionDBSelector.setSelectedItem(currentDBSelected);

						// reload db with modified questions
						reloadDB();
						
						JOptionPane.showMessageDialog(null,	"The question has been updated.");
					}
				}
			}
		} else if (modificationType.equals("Delete Question") && deleteQuestionRadioButton.isSelected()) {
			if ((perspective == null || perspective.equals(""))	|| (question == null || question.equals("")) || (layout == null || layout.equals("")) || (sparql == null || sparql.equals("")) || (currentQuestionKey == null || currentQuestionKey.equals(""))) {
				JOptionPane.showMessageDialog(null, "There are empty field(s). All required fields must be filled in.");
			} else {
				int dialogButton = JOptionPane.YES_NO_OPTION;
				int dialogResult = JOptionPane.showConfirmDialog(null, "Do you want to delete this question?", "Warning", dialogButton);

				if (dialogResult == JOptionPane.YES_OPTION) {
					questionAdmin.removeQuestion(currentQuestionKey);

					// Refresh the questions by selecting the db again and
					// populating all of the perspectives/questions based on new
					// xmlfile/s
					String currentDBSelected = (String) questionDBSelector.getSelectedItem();
					questionDBSelector.setSelectedItem(currentDBSelected);

					// reload db with modified questions
					reloadDB();

					JOptionPane.showMessageDialog(null, "The question has been deleted.");
				}
			}
		}
	}

	private void emptyFields(JTextField perspectiveField, JTextField questionField, JTextField layoutField, JTextPane sparql, JTextPane dependencyTextPane, JTextPane parameterQueryTextPane, JList<String> dependencyList, JList<String> queryList, JList<String> optionList) {
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

	private List<SEMOSSParam> generateSEMOSSParamObjects(Vector<String> parameterDependList, Vector<String> parameterQueryList, Vector<String> parameterOptionList, Map<String, String> paramsInQuery) {
		List<SEMOSSParam> params = new ArrayList<SEMOSSParam>();
		for (String paramName : paramsInQuery.keySet()) {
			SEMOSSParam param = new SEMOSSParam();
			param.setName(paramName);

			for (String s : parameterDependList) {
				String[] split = s.split("_DEPEND_-_");
				if (split[0].equals(paramName)) {
					param.addDependVar(split[1]);
					param.setDepends("true");
				}
			}

			boolean foundInList = false;
			for (String s : parameterQueryList) {
				String[] split = s.split("_QUERY_-_");
				if (split[0].equals(paramName)) {
					param.setQuery(split[1]);
					foundInList = true;
				}
			}
			for (String s : parameterOptionList) {
				String[] split = s.split("_OPTION_-_");
				if (split[0].equals(paramName)) {
					param.setOptions(split[1]);
					foundInList = true;
				}
			}
			if (!foundInList) {
				param.setType(paramsInQuery.get(paramName));
			}

			params.add(param);
		}

		return params;
	}
}
*/

