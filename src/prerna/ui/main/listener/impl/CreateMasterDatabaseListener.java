package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.CreateMasterDB;
import prerna.algorithm.impl.DeleteMasterDB;

public class CreateMasterDatabaseListener extends AbstractListener{

	static final Logger LOGGER = LogManager.getLogger(CreateMasterDatabaseListener.class.getName());

	@Override
	public void actionPerformed(ActionEvent arg0) {
		
	//	CreateMasterDB cmd = new CreateMasterDB();
	//	cmd.addEngine("Movie_DB");
	//	cmd.createDB();
		DeleteMasterDB del = new DeleteMasterDB();
		del.deleteEngine("Movie_DB");
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}

}
