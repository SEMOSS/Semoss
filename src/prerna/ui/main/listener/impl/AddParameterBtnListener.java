package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JTextPane;

import org.apache.log4j.Logger;

import prerna.ui.components.ParamComboBox;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class AddParameterBtnListener extends AbstractListener {
	Logger logger = Logger.getLogger(getClass());
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		JTextPane questionSparql = (JTextPane) DIHelper.getInstance().getLocalProp(
				Constants.QUESTION_SPARQL_TEXT_PANE);
		ParamComboBox addParameterComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_ADD_PARAMETER_COMBO_BOX);
		
		String currentQuery = questionSparql.getText();
		String parameter = (String) addParameterComboBox.getSelectedItem();
		
		//adds the sparql for parameters
		questionSparql.setText(currentQuery+" BIND(<@"+ parameter + "-http://semoss.org/ontologies/" + parameter + "@> AS ?" + parameter + ") ");
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
