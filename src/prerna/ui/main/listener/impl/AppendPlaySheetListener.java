package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JToggleButton;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class AppendPlaySheetListener implements IChakraListener {

	JComboBox view = null;
	
	
	public void setModel(JComponent model)
	{
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		System.out.println("Came here " );
		// see if extend is on and if it is disable it
		JToggleButton extend = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.EXTEND);
		extend.setSelected(false);

	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		this.view = (JComboBox)view;
		
	}



}
