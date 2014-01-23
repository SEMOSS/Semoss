/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.Hashtable;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the type of import.  Tied to the JComboBox in the Import Data section of the DB modification pane.
 */
public class ImportTypeSelectionListener extends AbstractListener {

	public JComponent view = null;
	
	// will have 2 string arrays one for the perspective and one for the question
	Hashtable model = null;
	Logger logger = Logger.getLogger(getClass());
	
	// needs to find what is being selected from event
	// based on that refresh the view of questions for that given perspective
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox bx = (JComboBox)e.getSource();
		JComboBox typeBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.IMPORT_TYPE_COMBOBOX);
		JLabel typeLbl = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_TYPE_LABEL);
		JPanel panel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_PANEL);
		JLabel lbl1 = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_ENTERDB_LABEL);
		JLabel lbl2 = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_FILE_LABEL);
		JTextField jtf1 = (JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_NAME_FIELD);
		JTextField jtf2 = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_FILE_FIELD);
		JButton button1 = (JButton)DIHelper.getInstance().getLocalProp(Constants.IMPORT_BUTTON_BROWSE);
		JButton button2 = (JButton)DIHelper.getInstance().getLocalProp(Constants.IMPORT_BUTTON);
		JTextField mapText = (JTextField)DIHelper.getInstance().getLocalProp(Constants.MAP_TEXT_FIELD);
		JButton advancedButton = (JButton)DIHelper.getInstance().getLocalProp(Constants.ADVANCED_IMPORT_OPTIONS_BUTTON);
		JPanel advancedPanel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.ADVANCED_IMPORT_OPTIONS_PANEL);
		String selection = bx.getSelectedItem() + "";
		if(selection.equals("Add to existing database engine") )
		{
			typeBox.setVisible(true);
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel loading sheet format", "CSV format"}));
			typeLbl.setVisible(true);
			panel.setVisible(true);
			lbl1.setVisible(false);
			lbl2.setVisible(true);
			jtf1.setVisible(false);
			jtf2.setVisible(true);
			mapText.setVisible(false);
			advancedButton.setVisible(false);
			advancedPanel.setVisible(false);
		}
		else if(selection.equals("Modify/Replace data in existing engine"))
		{
			typeBox.setVisible(true);
			typeBox.removeAll();
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel loading sheet format"}));
			typeBox.setSelectedIndex(0);
			typeLbl.setVisible(true);
			panel.setVisible(true);
			lbl1.setVisible(false);
			lbl2.setVisible(true);
			jtf1.setVisible(false);
			jtf2.setVisible(true);
			mapText.setVisible(false);
			advancedButton.setVisible(false);
			advancedPanel.setVisible(false);
		}
		else if (selection.equals("Create new database engine"))
		{
			typeBox.setVisible(true);
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel loading sheet format", "CSV format"}));
			typeLbl.setVisible(true);
			panel.setVisible(true);
			lbl1.setVisible(true);
			lbl2.setVisible(true);
			jtf1.setVisible(true);
			jtf2.setVisible(true);
			mapText.setVisible(true);
			advancedButton.setVisible(true);
			if(!advancedPanel.isVisible() && advancedButton.getText().contains("Hide")) 
				advancedButton.setText(advancedButton.getText().replace("Hide", "Show"));
		}
		else if (selection.equals("Select a database import method"))
		{
			panel.setVisible(false);
			typeBox.setVisible(false);
			typeLbl.setVisible(false);
		}

	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		logger.debug("View is set " + view);
		this.view = view;
		
	}

}
