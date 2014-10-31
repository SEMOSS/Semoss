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

public class QuestionEditOptionBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JList<String> parameterOptionList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTIONS_JLIST);
		JTextPane parameterOptionTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_TEXT_PANE);

		if(parameterOptionList.getSelectedValue()!=null){
			String selectedDependency = (String) parameterOptionList.getSelectedValue();
			
			parameterOptionTextPane.setText(selectedDependency);
			
			Vector<String> listData = new Vector<String>();
			ListModel<String> model = parameterOptionList.getModel();
			
			for(int i=0; i < model.getSize(); i++){
				listData.addElement(model.getElementAt(i));
			}
			
			listData.removeElement(selectedDependency);
			
			parameterOptionList.setListData(listData);
			
			parameterOptionTextPane.setFont(new Font("Tahoma", Font.PLAIN, 11));
			parameterOptionTextPane.setForeground(Color.BLACK);
		}
		else{
			JOptionPane
			.showMessageDialog(null,
					"Please select an option from the list.");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
