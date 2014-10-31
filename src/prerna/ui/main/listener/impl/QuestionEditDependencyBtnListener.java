package prerna.ui.main.listener.impl;

import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextPane;
import javax.swing.ListModel;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class QuestionEditDependencyBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JList<String> parameterDependenciesList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);
		JTextPane parameterDependTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_TEXT_PANE);

		if(parameterDependenciesList.getSelectedValue()!=null){
			String selectedDependency = (String) parameterDependenciesList.getSelectedValue();
			
			parameterDependTextPane.setText(selectedDependency);
			
			Vector<String> listData = new Vector<String>();
			ListModel<String> model = parameterDependenciesList.getModel();
			
			for(int i=0; i < model.getSize(); i++){
				listData.addElement(model.getElementAt(i));
			}
			
			listData.removeElement(selectedDependency);
			
			parameterDependenciesList.setListData(listData);
			
			parameterDependTextPane.setFont(new Font("Tahoma", Font.PLAIN, 11));
			parameterDependTextPane.setForeground(Color.BLACK);
		}
		else{
			JOptionPane
			.showMessageDialog(null,
					"Please select a dependency from the list.");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
