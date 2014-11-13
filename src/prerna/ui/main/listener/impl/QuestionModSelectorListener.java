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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JTextPane;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;

/**
 * Controls selection of the perspective.
 */
public class QuestionModSelectorListener implements IChakraListener {

	JTextPane questionSparqlTextPane = new JTextPane();
	JTextField questionPerspectiveField = new JTextField();
	JTextField questionField = new JTextField();
	JTextField questionLayoutField = new JTextField();
	JList<String> parameterDependList = new JList<String>();
	JList<String> parameterQueryList = new JList<String>();
	JList<String> parameterOptionList = new JList<String>();
	JComboBox<String> questionPerspectiveSelector = new JComboBox<String>();
	JRadioButton addQuestionModTypeBtn = new JRadioButton();
	JComboBox<String> questionModSelector = new JComboBox<String>();
	JComboBox<String> questionDBSelector = new JComboBox<String>();
	JComboBox<String> questionOrderComboBox = new JComboBox<String>();
	JComboBox<String> questionLayoutComboBox = new JComboBox<String>();
	
	String engineName = null;
	String question = null;
	String perspective = null;
	String order;
	Vector<String> parameterQueryVector = new Vector<String>();
	Vector<String> dependVector = new Vector<String>();
	Vector<String> optionVector = new Vector<String>();

	public void getFieldData() {
		questionSparqlTextPane = (JTextPane) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_SPARQL_TEXT_PANE);
		questionPerspectiveField = (JTextField) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_PERSPECTIVE_FIELD);
		questionField = (JTextField) DIHelper.getInstance().getLocalProp(
				Constants.QUESTION_FIELD);
		questionLayoutField = (JTextField) DIHelper.getInstance().getLocalProp(
				Constants.QUESTION_LAYOUT_FIELD);
		parameterDependList = (JList<String>) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);
		parameterQueryList = (JList<String>) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_QUERIES_JLIST);
		parameterOptionList = (JList<String>) DIHelper.getInstance()
				.getLocalProp(Constants.PARAMETER_OPTIONS_JLIST);

		questionPerspectiveSelector = (JComboBox<String>) DIHelper
				.getInstance().getLocalProp(
						Constants.QUESTION_PERSPECTIVE_SELECTOR);
		addQuestionModTypeBtn = (JRadioButton) DIHelper.getInstance()
				.getLocalProp(Constants.ADD_QUESTION_BUTTON);

		questionDBSelector = (JComboBox<String>) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_DB_SELECTOR);
		questionOrderComboBox = (JComboBox<String>) DIHelper.getInstance()
				.getLocalProp(Constants.QUESTION_ORDER_COMBO_BOX);
		questionLayoutComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_PLAYSHEET_COMBOBOXLIST);

		engineName = (String) questionDBSelector.getSelectedItem();
		question = (String) questionModSelector.getSelectedItem();
		
		if(question != null){
			String[] questionSplit = question.split(". ", 2);
			question = questionSplit[1];
		}
		perspective = (String) questionPerspectiveSelector.getSelectedItem();
	}

	/**
	 * Method actionPerformed. Dictates what actions to take when an Action
	 * Event is performed.
	 * 
	 * @param e
	 *            ActionEvent - The event that triggers the actions in the
	 *            method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		questionModSelector = (JComboBox) e.getSource();

		// gets the data based on selected perspective and question
		getFieldData();

		if (question != null && !perspective.equals("*NEW Perspective")) {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(
					engineName);
			//if the same question is used multiple times in different perspectives, vectorInsight will contain all those insights.
			//we need to loop through the insights and find the question that belongs to the perspective selected to get the correct order #
			Vector<Insight> vectorInsight = ((AbstractEngine)engine).getInsight2(question);
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
			
			order = in.getOrder();
			Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine) engine)
					.getParams(question);
			// empties the vectors so it doesn't duplicate existing elements
			parameterQueryVector.removeAllElements();
			dependVector.removeAllElements();
			optionVector.removeAllElements();
			// if there are params, get any related data (queries/dependencies)
			// and store them
			if (!paramInfoVector.isEmpty()) {
				for (int i = 0; i < paramInfoVector.size(); i++) {
					if (paramInfoVector.get(i).getQuery() != null
							&& !paramInfoVector
									.get(i)
									.getQuery()
									.equals(DIHelper.getInstance().getProperty(
											"TYPE" + "_" + Constants.QUERY))) {
						parameterQueryVector.add(paramInfoVector.get(i)
								.getName()
								+ "_QUERY_-_"
								+ paramInfoVector.get(i).getQuery());
					}
					
					if (!paramInfoVector.get(i).getDependVars().isEmpty()
							&& !paramInfoVector.get(i).getDependVars().get(0)
									.equals("None")) {
						for (int j = 0; j < paramInfoVector.get(i)
								.getDependVars().size(); j++) {
							dependVector.add(paramInfoVector.get(i).getName()
									+ "_DEPEND_-_"
									+ paramInfoVector.get(i).getDependVars()
											.get(j));
						}
					}

					if (paramInfoVector.get(i).getOptions() != null
							&& !paramInfoVector.get(i).getOptions().isEmpty()) {
						Vector options = paramInfoVector.get(i).getOptions();
						String optionsConcat = "";
						for (int j = 0; j < options.size(); j++) {
							optionsConcat += options.get(j);
							if(j!=options.size()-1){
								optionsConcat += ";";
							}
						}
						optionVector.add(paramInfoVector.get(i).getType()
								+ "_OPTION_-_" + optionsConcat);
					}
				}
			}

			// get the sparql from insight based on selected question
			String sparql = in.getSparql();
			// get the layout
			String layoutValue = in.getOutput();
			// get the id dbName:PERSPECTIVE:QuestionKey
			String questionID = in.getId();
			String[] questionIDArray = questionID.split(":");
			// split and get the questionKey
			String questionKey = questionIDArray[2];

			// sets the current values in QuestionAdministrator
			// will be used when user edits an existing question (deleting old
			// question and then adding the modified question)
			QuestionAdministrator.currentPerspective = perspective;
			QuestionAdministrator.currentQuestionKey = questionKey;
			QuestionAdministrator.currentQuestionOrder = order;
			QuestionAdministrator.currentQuestion = question;
			QuestionAdministrator.currentLayout = layoutValue;
			QuestionAdministrator.currentSparql = sparql;
			QuestionAdministrator.currentParameterDependListVector = dependVector;
			QuestionAdministrator.currentParameterQueryListVector = parameterQueryVector;
			QuestionAdministrator.currentParameterOptionListVector = optionVector;
			QuestionAdministrator.currentNumberofQuestions = Integer.toString(questionModSelector.getItemCount());


			// sets the field data if user is editing or deleting question
			if (!addQuestionModTypeBtn.isSelected()) {
				// questionPerspectiveField.setText(perspective);
				questionField.setText(question);

				questionSparqlTextPane.setText(sparql);
				questionLayoutField.setText(layoutValue);
				if(PlaySheetEnum.getNameFromClass(layoutValue) != ""){
					questionLayoutComboBox.setSelectedItem(PlaySheetEnum.getNameFromClass(layoutValue));
				} else{
					questionLayoutComboBox.setSelectedIndex(0);
				}
				questionOrderComboBox.setSelectedItem(order);
				// questionKeyField.setText(questionKey);
				// sets the dependencies in the UI
				if (!dependVector.isEmpty()) {
					parameterDependList.setListData(dependVector);
				} else {
					parameterDependList.setListData(new String[0]);
				}

				// sets the param queries in the UI
				if (!parameterQueryVector.isEmpty()) {
					parameterQueryList.setListData(parameterQueryVector);
				} else {
					parameterQueryList.setListData(new String[0]);
				}

				// sets the param options in the UI
				if (!optionVector.isEmpty()) {
					parameterOptionList.setListData(optionVector);
				} else {
					parameterOptionList.setListData(new String[0]);
				}
			} else {
				questionOrderComboBox.setSelectedItem(questionOrderComboBox
						.getItemCount() + "");
			}
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or
	 * modify when an action event occurs.
	 * 
	 * @param view
	 *            the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}

}
