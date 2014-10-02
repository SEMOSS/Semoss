package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class RDBMSDriverComboBoxListener extends AbstractListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		
		JComboBox<String> rdbmsDriver = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX);
		JTextField rdbmsUrlField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD);
		
		String driverType = (String) rdbmsDriver.getSelectedItem();
		
		if(driverType.equals("MySQL"))
		{
			rdbmsUrlField.setText("jdbc:mysql://<hostname>[:port]/<DBname>");
		}
		else if(driverType.equals("Oracle"))
		{
			rdbmsUrlField.setText("jdbc:oracle:thin:@<hostname>[:port]/<service or sid>[-schema name]");
		}
		else if(driverType.equals("MS SQL Server"))
		{
			rdbmsUrlField.setText("jdbc:sqlserver://<hostname>[:port];databaseName=<DBname>");
		}
		else if(driverType.equals("Aster Database")) {
			rdbmsUrlField.setText("jdbc:ncluster://192.168.100.100/beehive");
		}
		
		rdbmsDriver.removeItem("Select Relational Database Type");
	}
	

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}
	
}
