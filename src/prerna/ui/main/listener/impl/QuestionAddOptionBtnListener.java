package prerna.ui.main.listener.impl;

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

public class QuestionAddOptionBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JList<String> parameterOptionList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTIONS_JLIST);
		JTextPane parameterOptionTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_OPTION_TEXT_PANE);
		
		Vector<String> listData = new Vector<String>();
		ListModel<String> model = parameterOptionList.getModel();
		
		String newOption = parameterOptionTextPane.getText().trim();

		newOption = newOption.replace("\t", "_-_").replace("\r", "_-_").replace("\n", "_-_").replace("_OPTION ", "_OPTION_-_");
		
		if(newOption.contains("_OPTION") && !newOption.contains("Example:")){
			for(int i=0; i < model.getSize(); i++){
				listData.addElement(model.getElementAt(i));
			}
			
			listData.addElement(newOption);

			parameterOptionList.setListData(listData);
			parameterOptionTextPane.setText("");
			parameterOptionTextPane.requestFocusInWindow();
		}
		else{
			JOptionPane
			.showMessageDialog(null,
					"Parameter Option format is incorrect. It must be in the format of [ParameterName]_OPTION [option list]");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
