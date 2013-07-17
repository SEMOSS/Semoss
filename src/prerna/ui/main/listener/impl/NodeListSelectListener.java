package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import prerna.ui.components.RelationshipGet;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class NodeListSelectListener implements IChakraListener {
	
	
	@Override
	public void actionPerformed(ActionEvent actionevent)
	{
		JComboBox nodeList= (JComboBox)DIHelper.getInstance().getLocalProp(Constants.NODELIST);
		JComboBox extendList= (JComboBox)DIHelper.getInstance().getLocalProp(Constants.EXTENDLIST);
		String type = (String) nodeList.getSelectedItem();
		extendList.removeAllItems();
		extendList.addItem("Select a extend Question");
		if (type!=null && nodeList.getSelectedIndex()!=0)
		{
			RelationshipGet relGet = new RelationshipGet();
			Vector questionV = RelationshipGet.getRelationship(type);
			int vSize = questionV.size();
			for (int i=0;i<vSize;i++)
			{
				String qString = (String) questionV.get(i);
	    		extendList.addItem(qString);
			}
		}
		
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}
	
	
}
