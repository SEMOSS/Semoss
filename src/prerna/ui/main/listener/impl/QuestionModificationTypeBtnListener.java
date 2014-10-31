package prerna.ui.main.listener.impl;

import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.util.Vector;

import javax.swing.ComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JTextPane;

import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ParamComboBox;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class QuestionModificationTypeBtnListener extends AbstractListener {
	Logger logger = Logger.getLogger(getClass());
	ParamComboBox addParameterComboBox = null;
	JComboBox<String> questionPerspectiveSelector = new JComboBox<String>();
	JComboBox<String> questionModSelector = new JComboBox<String>();
	JLabel lblQuestionSelectPerspective = new JLabel();
	JLabel lblSelectQuestion = new JLabel();
	JTextPane questionSparqlTextPane = new JTextPane();
	JTextField questionPerspectiveField = new JTextField();
	JTextField questionField = new JTextField();
	JTextField questionLayoutField = new JTextField();
	JButton questionModButton = new JButton();
	JButton questionAddParameterButton = new JButton();
	JButton addParameterDependButton = new JButton();
	JButton addParameterQueryButton = new JButton();
	JButton addParameterOptionButton = new JButton();
	
	JTextPane parameterQueryTextPane = new JTextPane();
	JTextPane parameterDependTextPane = new JTextPane();
	JTextPane parameterOptionTextPane = new JTextPane();
	JList<String> parameterDependList = new JList<String>();
	JList<String> parameterQueryList = new JList<String>();
	JList<String> parameterOptionList = new JList<String>();
	JButton editParameterDependButton = new JButton();
	JButton deleteParameterDependButton = new JButton();
	JButton editParameterQueryButton = new JButton();
	JButton deleteParameterQueryButton = new JButton();
	JButton editParameterOptionButton = new JButton();
	JButton deleteParameterOptionButton = new JButton();
	
	JRadioButton clickedRadioButton = new JRadioButton();
	JComboBox<String> questionDBSelector = new JComboBox<String>();
	JComboBox<String> questionOrderComboBox = new JComboBox<String>();
	String engineName = null;
	String order = "";
	
	private void getFieldData(){
		addParameterComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ADD_PARAMETER_COMBO_BOX);
		questionPerspectiveSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_SELECTOR);
		questionModSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_SELECTOR);
		lblQuestionSelectPerspective = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_QUESTION_SELECT_PERSPECTIVE);
		lblSelectQuestion = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_SELECT_QUESTION);
		
		questionSparqlTextPane = (JTextPane ) DIHelper.getInstance().getLocalProp(Constants.QUESTION_SPARQL_TEXT_PANE);
		questionPerspectiveField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_FIELD);
		questionField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_FIELD);
		questionLayoutField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.QUESTION_LAYOUT_FIELD);
		
		questionModButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_BUTTON);
		questionAddParameterButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ADD_PARAMETER_BUTTON);
		
		addParameterOptionButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.ADD_PARAMETER_OPTION_BUTTON);
		addParameterDependButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.ADD_PARAMETER_DEPENDENCY_BUTTON);
		addParameterQueryButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.ADD_PARAMETER_QUERY_BUTTON);
		
		parameterQueryTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_TEXT_PANE);
		parameterDependTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_TEXT_PANE);
		parameterOptionTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_TEXT_PANE);
		
		parameterDependList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);
		parameterQueryList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERIES_JLIST);
		parameterOptionList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTIONS_JLIST);
		
		editParameterDependButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_EDIT_BUTTON);
		deleteParameterDependButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_DELETE_BUTTON);
		
		editParameterQueryButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_EDIT_BUTTON);
		deleteParameterQueryButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_DELETE_BUTTON);
		
		editParameterOptionButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_EDIT_BUTTON);
		deleteParameterOptionButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_DELETE_BUTTON);
		
		questionDBSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_DB_SELECTOR);
		questionOrderComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ORDER_COMBO_BOX);
		
		engineName = (String) questionDBSelector.getSelectedItem();
	}
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {

		clickedRadioButton = (JRadioButton)e.getSource();
		String modificationType = clickedRadioButton.getText()+ "";
			
		getFieldData();
		
		boolean existingNewPerspective = false;
		//checks if the *New Perspective item exists in the Perspective ComboBox selection
		ComboBoxModel model = questionPerspectiveSelector.getModel();
		int perspectiveSize = model.getSize();
		for(int i = 0; i < perspectiveSize; i++){
			String existingPerspective = (String) model.getElementAt(i);
			if(existingPerspective.equals("*NEW Perspective")){
				existingNewPerspective = true;
			}
		}
		
		if(!modificationType.equals("Add Question")){
			if(existingNewPerspective){
				questionPerspectiveSelector.removeItem("*NEW Perspective");
			}
		}
		else{
			if(!existingNewPerspective){
				questionPerspectiveSelector.insertItemAt("*NEW Perspective", 0);
			} 
		}
		
		questionPerspectiveField.setEnabled(true);
		//questionKeyField.setEnabled(true);
		questionField.setEnabled(true);
		questionSparqlTextPane.setEnabled(true);
		//questionParameterPropTextPane.setEnabled(true);
		questionLayoutField.setEnabled(true);
		questionAddParameterButton.setEnabled(true);
		addParameterComboBox.setEnabled(true);
		questionOrderComboBox.setEnabled(true);
		
		parameterQueryList.setEnabled(true);
		parameterDependList.setEnabled(true);
		parameterOptionList.setEnabled(true);
		parameterQueryTextPane.setEnabled(true);
		parameterOptionTextPane.setEnabled(true);
		parameterDependTextPane.setEnabled(true);
		addParameterQueryButton.setEnabled(true);
		addParameterDependButton.setEnabled(true);
		addParameterOptionButton.setEnabled(true);
		editParameterOptionButton.setEnabled(true);
		deleteParameterOptionButton.setEnabled(true);
		editParameterDependButton.setEnabled(true);
		deleteParameterDependButton.setEnabled(true);
		editParameterQueryButton.setEnabled(true);
		deleteParameterQueryButton.setEnabled(true);
		
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		String question = (String) questionModSelector.getSelectedItem();

		Insight in = ((AbstractEngine)engine).getInsight2(question).get(0);

		// now get the SPARQL query for this id
		String sparql = in.getSparql();
		//get the layout
		String layoutValue = in.getOutput();
		//get the selected question
		//get the selected perspective
		Vector<String> parameterQueryVector = new Vector<String>();
		Vector<String> dependVector = new Vector<String>();
		Vector<String> optionVector = new Vector<String>();
		Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine)engine).getParams(question);
		
		String[] questionArray = question.split("\\. ", 2);
		order = questionArray[0].trim();
		String questionWithOutCounter = questionArray[1].trim();
		
		if(!paramInfoVector.isEmpty()){
			for(int i = 0; i < paramInfoVector.size(); i++){
				if(paramInfoVector.get(i).getQuery() != null && !paramInfoVector.get(i).getQuery().equals(DIHelper.getInstance().getProperty(
						"TYPE" + "_" + Constants.QUERY))){
					parameterQueryVector.add(paramInfoVector.get(i).getName() + "_QUERY_-_" + paramInfoVector.get(i).getQuery());
				}
				
				if(!paramInfoVector.get(i).getDependVars().isEmpty() && !paramInfoVector.get(i).getDependVars().get(0).equals("None")){
					for(int j = 0; j < paramInfoVector.get(i).getDependVars().size(); j++){
						dependVector.add(paramInfoVector.get(i).getName() + "_DEPEND_-_" + paramInfoVector.get(i).getDependVars().get(j));
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
		
		if(modificationType.equals("Add Question")){
			questionPerspectiveSelector.setSelectedIndex(0);
			
			String perspective = (String) questionPerspectiveSelector.getSelectedItem();

			//questionPerspectiveSelector.setVisible(false);
			questionModSelector.setVisible(false);
			//lblQuestionSelectPerspective.setVisible(false);
			lblSelectQuestion.setVisible(false);
			
			questionSparqlTextPane.setText("");
			addParameterComboBox.setSelectedItem("");
			questionPerspectiveField.setText(perspective);
			questionOrderComboBox.setSelectedItem(questionOrderComboBox.getItemCount() + "");
			questionField.setText("");
			questionLayoutField.setText("prerna.ui.components.playsheets.PlaySheetName");
			parameterQueryList.setListData(new String[0]);
			parameterDependList.setListData(new String[0]);
			parameterOptionList.setListData(new String[0]);

			questionModButton.setText("Add Question");
			
			//hint text
			parameterQueryTextPane.setText("Example:" + "\r" + "Concept_QUERY" + "\t" 
					+ "SELECT ?entity WHERE { {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> ;} }");
			parameterQueryTextPane.setFont(new Font("Tahoma", Font.ITALIC, 11));
			parameterQueryTextPane.setForeground(Color.GRAY);
			parameterDependTextPane.setText("Example:" + "\r" + "Instance_DEPEND" + "\t" + "Concept");
			parameterDependTextPane.setFont(new Font("Tahoma", Font.ITALIC, 11));
			parameterDependTextPane.setForeground(Color.GRAY);
		}
		else if(modificationType.equals("Edit Question")){
			
			String perspective = (String) questionPerspectiveSelector.getSelectedItem();
			
			questionPerspectiveSelector.setVisible(true);
			questionModSelector.setVisible(true);
			lblQuestionSelectPerspective.setVisible(true);
			lblSelectQuestion.setVisible(true);
			
			questionSparqlTextPane.setText(sparql);
			addParameterComboBox.setSelectedItem("");
			//questionParameterPropTextPane.setText("");
			questionPerspectiveField.setText(perspective);
			questionOrderComboBox.setSelectedItem(order);
			//removes the extra number (new question indicator) in the combobox used in the Add functionality; users don't need this number when editing questions
			if(questionOrderComboBox.getItemCount()-questionModSelector.getItemCount() == 1){
				questionOrderComboBox.removeItemAt(questionOrderComboBox.getItemCount()-1);
			}
			//questionKeyField.setText(questionKey);
			questionField.setText(questionWithOutCounter);
			questionLayoutField.setText(layoutValue);
			if(!dependVector.isEmpty()){
				parameterDependList.setListData(dependVector.toArray(new String[dependVector.size()]));
			}
			else {
				parameterDependList.setListData(new String[0]);
			}
				
			if(!parameterQueryVector.isEmpty()){
				parameterQueryList.setListData(parameterQueryVector.toArray(new String[parameterQueryVector.size()]));
			}
			else {
				parameterQueryList.setListData(new String[0]);
			}
			
			// sets the param options in the UI
			if (!optionVector.isEmpty()) {
				parameterOptionList.setListData(optionVector);
			} else {
				parameterOptionList.setListData(new String[0]);
			}
			questionModButton.setText("Update Question");
		}
		else if(modificationType.equals("Delete Question")){
			String perspective = (String) questionPerspectiveSelector.getSelectedItem();

			questionPerspectiveSelector.setVisible(true);
			questionModSelector.setVisible(true);
			lblQuestionSelectPerspective.setVisible(true);
			lblSelectQuestion.setVisible(true);
			
			questionPerspectiveField.setEnabled(false);
			//questionKeyField.setEnabled(false);
			questionField.setEnabled(false);
			questionSparqlTextPane.setEnabled(false);
			//questionParameterPropTextPane.setEnabled(false);
			questionLayoutField.setEnabled(false);
			questionAddParameterButton.setEnabled(false);
			addParameterComboBox.setEnabled(false);
			questionOrderComboBox.setEnabled(false);
			
			parameterQueryList.setEnabled(false);
			parameterDependList.setEnabled(false);
			parameterOptionList.setEnabled(false);
			addParameterQueryButton.setEnabled(false);
			addParameterDependButton.setEnabled(false);
			addParameterOptionButton.setEnabled(false);
			parameterQueryTextPane.setEnabled(false);
			parameterDependTextPane.setEnabled(false);
			parameterOptionTextPane.setEnabled(false);
			
			editParameterOptionButton.setEnabled(false);
			deleteParameterOptionButton.setEnabled(false);
			editParameterDependButton.setEnabled(false);
			deleteParameterDependButton.setEnabled(false);
			editParameterQueryButton.setEnabled(false);
			deleteParameterQueryButton.setEnabled(false);
			
			questionSparqlTextPane.setText(sparql);
			addParameterComboBox.setSelectedItem("");
			questionOrderComboBox.setSelectedItem(order);
			//removes the extra number (new question indicator) in the combobox used in the Add functionality; users don't need this number when editing questions
			if(questionOrderComboBox.getItemCount()-questionModSelector.getItemCount() == 1){
				questionOrderComboBox.removeItemAt(questionOrderComboBox.getItemCount()-1);
			}
			questionPerspectiveField.setText(perspective);
			questionField.setText(questionWithOutCounter);
			questionLayoutField.setText(layoutValue);
			if(!dependVector.isEmpty()){
				parameterDependList.setListData(dependVector);
			}
			else {
				parameterDependList.setListData(new String[0]);
			}
				
			if(!parameterQueryVector.isEmpty()){
				parameterQueryList.setListData(parameterQueryVector);
			}
			else {
				parameterQueryList.setListData(new String[0]);
			}
			
			// sets the param options in the UI
			if (!optionVector.isEmpty()) {
				parameterOptionList.setListData(optionVector);
			} else {
				parameterOptionList.setListData(new String[0]);
			}
			
			questionModButton.setText("Delete Question");
		}
	}
	
	/**
	 * Method setView.
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
