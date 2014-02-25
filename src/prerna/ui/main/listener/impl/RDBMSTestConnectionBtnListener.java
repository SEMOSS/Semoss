package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class RDBMSTestConnectionBtnListener extends AbstractListener {
	
	@Override
	public void actionPerformed(ActionEvent e) {
		
		JTextField usernameField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_FIELD);
		JPasswordField passwordField = (JPasswordField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_FIELD);
		JTextField dbImportURLField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD);

		String username = usernameField.getText();
		char[] password = passwordField.getPassword();
		String url = dbImportURLField.getText();
		
		if(username != null &&  password != null && url != null && !username.isEmpty() && !password.toString().isEmpty() && !url.isEmpty())
		{	
			//remember to use password.toString()
			//TODO: add jdbc test for connection
			
			
			
			//zero out the password after use
			for(int i = 0; i < password.length; i++)
			{
				password[i] = 0;
			}
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}