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
package prerna.ui.components.specific.tap;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.main.listener.specific.tap.ServiceSelectAllListener;
import prerna.ui.main.listener.specific.tap.ServiceSelectListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to select services using checkboxes.
 */
public class ServiceSelectPanel extends JPanel {
	public IEngine engine;
	public Hashtable checkBoxHash = new Hashtable();
	public JCheckBox selectAllCheck = new JCheckBox();
	
	/**
	 * Constructor for ServiceSelectPanel.
	 */
	public ServiceSelectPanel(){

	}
	
	/**
	 * Gets the name of services via SPARQL query.
	 */
	public void getServices()
	{
		Vector <String> names = new Vector<String>();
		String sparqlQuery = DIHelper.getInstance().getProperty(
				"TYPE" + "_" + Constants.QUERY);
		
		Hashtable paramTable = new Hashtable();
		String entityNS = DIHelper.getInstance().getProperty("Service"+Constants.CLASS);
		paramTable.put(Constants.ENTITY, entityNS );
		sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
		
		names = engine.getEntityOfType(sparqlQuery);
		Hashtable paramHash = Utility.getInstanceNameViaQuery(names);
		Set nameC = paramHash.keySet();
		names = new Vector(nameC);
		Collections.sort(names);
		removeAll();
		createCheckBoxes(names);
	}
	
	/**
	 * Method createCheckBoxes.
	 * @param serviceV 	Vector containing a list of all the services.
	 */
	public void createCheckBoxes(Vector serviceV)
	{
		GridBagLayout gridBagLayout = new GridBagLayout();
		this.setLayout(gridBagLayout);
		GridBagConstraints gbc_element = new GridBagConstraints();
		gbc_element = new GridBagConstraints();	
		gbc_element.anchor = GridBagConstraints.WEST;
		//gbc_element.fill = GridBagConstraints.BOTH;
		gbc_element.insets = new Insets(0, 5, 5, 5);
		gbc_element.gridx = 0;
		gbc_element.gridy = 0;
		JLabel label = new JLabel("Services");
		label.setFont(new Font("Tahoma", Font.PLAIN, 12));
		this.add(label, gbc_element);
		
		gbc_element.anchor = GridBagConstraints.WEST;
		//gbc_element.fill = GridBagConstraints.BOTH;
		gbc_element.insets = new Insets(0, 5, 5, 5);
		gbc_element.gridx = 0;
		gbc_element.gridy = 1;
		selectAllCheck= new JCheckBox("Select All");
		selectAllCheck.setSelected(true);
		selectAllCheck.setFont(new Font("Tahoma", Font.PLAIN, 12));
		Font f = selectAllCheck.getFont();
		Font newF = new Font(f.getName(), Font.BOLD, f.getSize());
		selectAllCheck.setFont(newF);
		this.add(selectAllCheck, gbc_element);
		
		ServiceSelectListener ssListener = new ServiceSelectListener();
		ssListener.setSelectAllCheck(selectAllCheck);
		
		for (int i=0; i < serviceV.size(); i++)
		{
			JCheckBox checkBox= new JCheckBox((String)serviceV.get(i));
			checkBox.setFont(new Font("Tahoma", Font.PLAIN, 12));
			checkBox.setSelected(true);
			checkBox.addActionListener(ssListener);
			checkBoxHash.put((String)serviceV.get(i), checkBox);
			gbc_element = new GridBagConstraints();	
			gbc_element.anchor = GridBagConstraints.WEST;
			//gbc_element.fill = GridBagConstraints.BOTH;
			gbc_element.insets = new Insets(0, 5, 5, 5);
			gbc_element.gridx = 0;
			gbc_element.gridy = i+2;
			this.add(checkBox, gbc_element);
		}
		
		ServiceSelectAllListener ssaListener = new ServiceSelectAllListener();
		ssaListener.setCheckHash(checkBoxHash);
		selectAllCheck.addActionListener(ssaListener);
	}

	
}
