package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import prerna.rdf.main.ImportRDBMSProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RDBMSTestConnectionBtnListener extends AbstractListener {
	
	@Override
	public void actionPerformed(ActionEvent e) {
		JButton btn = (JButton)e.getSource();
		btn.setEnabled(false);
		btn.setText("Connecting...");
		
		JComboBox<String> dbType = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX);
		JTextField usernameField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_FIELD);
		JPasswordField passwordField = (JPasswordField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_FIELD);
		JTextField dbImportURLField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD);
		
		String username = usernameField.getText();
		char[] password = passwordField.getPassword();
		String url = dbImportURLField.getText();
		
		boolean validConnection = false;
		
		if(username != null &&  password != null && url != null && !username.isEmpty() && password.length > 0 && !url.isEmpty())
		{	
			ImportRDBMSProcessor t = new ImportRDBMSProcessor();
			validConnection = t.checkConnection(dbType.getSelectedItem().toString(), url, username, password);
			
			//zero out the password after use
			for(int i = 0; i < password.length; i++)
			{
				password[i] = 0;
			}
		}
		
		btn.setEnabled(true);
		btn.setText("Test Connection");
		
		if(validConnection) {
			Utility.showMessage("Valid connection!");
		} else {
			Utility.showError("Could not connect to " + username + "@" + url);
		}
	}

	@Override
	public void setView(JComponent view) {
		
	}

}