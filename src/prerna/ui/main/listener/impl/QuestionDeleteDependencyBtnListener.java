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

public class QuestionDeleteDependencyBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JList<String> parameterDependenciesList = new JList<String>();
		parameterDependenciesList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);

		if(parameterDependenciesList.getSelectedValue()!=null){
			String selectedQuery = (String) parameterDependenciesList.getSelectedValue();
			
			Vector<String> listData = new Vector<String>();
			ListModel<String> model = parameterDependenciesList.getModel();
			
			for(int i=0; i < model.getSize(); i++){
				listData.addElement(model.getElementAt(i));
			}
			
			listData.removeElement(selectedQuery);
			
			parameterDependenciesList.setListData(listData);
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
