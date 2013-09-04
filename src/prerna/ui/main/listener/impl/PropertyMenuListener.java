package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JMenuItem;

import org.apache.log4j.Logger;

import prerna.ui.components.NeighborMenuItem;
import prerna.ui.components.PropertyMenuItem;

public class PropertyMenuListener implements ActionListener {

	public static PropertyMenuListener instance = null;
	Logger logger = Logger.getLogger(getClass());
	
	protected PropertyMenuListener()
	{
		
	}
	
	public static PropertyMenuListener getInstance()
	{
		if(instance == null)
			instance = new PropertyMenuListener();
		return instance;
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		// get the engine
		// execute the neighbor hood 
		// paint it
		// get the query from the 
		PropertyMenuItem item = (PropertyMenuItem)e.getSource();
		item.paintNeighborhood();
	}
}
