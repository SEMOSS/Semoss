package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.ListModel;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class QuestionDeleteQueryBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JList<String> parameterQueriesList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_QUERIES_JLIST);

		if(parameterQueriesList.getSelectedValue()!=null){
			String selectedQuery = (String) parameterQueriesList.getSelectedValue();
			
			Vector<String> listData = new Vector<String>();
			ListModel<String> model = parameterQueriesList.getModel();
			
			for(int i=0; i < model.getSize(); i++){
				listData.addElement(model.getElementAt(i));
			}
			
			listData.removeElement(selectedQuery);
			
			parameterQueriesList.setListData(listData);
		}
		else{
			JOptionPane
			.showMessageDialog(null,
					"Please select a parameter query from the list.");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
