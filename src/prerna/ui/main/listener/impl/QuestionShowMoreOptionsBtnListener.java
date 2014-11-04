package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JScrollPane;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class QuestionShowMoreOptionsBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JButton questionMoreOptionsButton = (JButton)e.getSource();
		
		JButton addParameterDependButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.ADD_PARAMETER_DEPENDENCY_BUTTON);
		JButton addParameterQueryButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.ADD_PARAMETER_QUERY_BUTTON);
		JButton addParameterOptionButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.ADD_PARAMETER_OPTION_BUTTON);

		JLabel lblParameterQuery = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_PARAMETER_QUERY);
		JLabel lblParameterDepend = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_PARAMETER_DEPEND);
		JLabel lblParameterOption = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_PARAMETER_OPTION);

		JScrollPane parameterQueryScroll = (JScrollPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_SCROLL);
		JScrollPane parameterDependScroll = (JScrollPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_SCROLL);
		JScrollPane parameterOptionScroll = (JScrollPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_SCROLL);

		JScrollPane parameterDependScrollList = (JScrollPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_SCROLL_LIST);
		JScrollPane parameterQueryScrollList = (JScrollPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_SCROLL_LIST);
		JScrollPane parameterOptionScrollList = (JScrollPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_SCROLL_LIST);

		JLabel lblParameterQueryList = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_PARAMETER_QUERY_LIST);
		JLabel lblParameterDependList = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_PARAMETER_DEPEND_LIST);
		JLabel lblParameterOptionList = (JLabel) DIHelper.getInstance().getLocalProp(Constants.LABEL_PARAMETER_OPTION_LIST);

		JButton parameterDependDeleteButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_DELETE_BUTTON);
		JButton parameterQueryDeleteButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_DELETE_BUTTON);
		JButton parameterOptionDeleteButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_DELETE_BUTTON);

		JButton parameterDependEditButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_EDIT_BUTTON);
		JButton parameterQueryEditButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERY_EDIT_BUTTON);
		JButton parameterOptionEditButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_EDIT_BUTTON);
		
		String buttonText = questionMoreOptionsButton.getText();
		if(buttonText.equals("+ Advanced Param Settings")){
			addParameterDependButton.setVisible(true);
			addParameterQueryButton.setVisible(true);
			parameterQueryScroll.setVisible(true);
			parameterDependScroll.setVisible(true);
			parameterDependScrollList.setVisible(true);
			parameterQueryScrollList.setVisible(true);
			lblParameterQueryList.setVisible(true);
			lblParameterDependList.setVisible(true);
			lblParameterQuery.setVisible(true);
			lblParameterDepend.setVisible(true);
			parameterDependDeleteButton.setVisible(true);
			parameterQueryDeleteButton.setVisible(true);
			parameterDependEditButton.setVisible(true);
			parameterQueryEditButton.setVisible(true);
			
			parameterOptionEditButton.setVisible(true);
			parameterOptionDeleteButton.setVisible(true);
			addParameterOptionButton.setVisible(true);
			lblParameterOption.setVisible(true);
			parameterOptionScroll.setVisible(true);
			parameterOptionScrollList.setVisible(true);
			lblParameterOptionList.setVisible(true);
			
			questionMoreOptionsButton.setText("- Advanced Param Settings");
		}
		else if (buttonText.equals("- Advanced Param Settings")){
			addParameterDependButton.setVisible(false);
			addParameterQueryButton.setVisible(false);
			lblParameterQuery.setVisible(false);
			lblParameterDepend.setVisible(false);
			parameterQueryScroll.setVisible(false);
			parameterDependScroll.setVisible(false);
			parameterDependScrollList.setVisible(false);
			parameterQueryScrollList.setVisible(false);
			lblParameterQueryList.setVisible(false);
			lblParameterDependList.setVisible(false);
			parameterDependDeleteButton.setVisible(false);
			parameterQueryDeleteButton.setVisible(false);
			parameterDependEditButton.setVisible(false);
			parameterQueryEditButton.setVisible(false);
			
			parameterOptionEditButton.setVisible(false);
			parameterOptionDeleteButton.setVisible(false);
			addParameterOptionButton.setVisible(false);
			lblParameterOption.setVisible(false);
			parameterOptionScroll.setVisible(false);
			parameterOptionScrollList.setVisible(false);
			lblParameterOptionList.setVisible(false);
			
			questionMoreOptionsButton.setText("+ Advanced Param Settings");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
