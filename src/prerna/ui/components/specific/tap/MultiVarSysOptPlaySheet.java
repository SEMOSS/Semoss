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
import java.awt.Insets;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JTextField;

import prerna.ui.main.listener.specific.tap.MultiVarSysOptBtnListener;

/**
 * This is the playsheet used exclusively for TAP system optimization with annually varying budget.
 */
@SuppressWarnings("serial")
public class MultiVarSysOptPlaySheet extends SysOptPlaySheet{

	public JTextField maxEvalsField;
	
	/**
	 * Constructor for MultiVarSysOptPlaySheet.
	 */
	public MultiVarSysOptPlaySheet() {
		super();
	}

	@Override
	public void addOptimizationBtnListener(JButton btnRunOptimization) {
		MultiVarSysOptBtnListener obl = new MultiVarSysOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	@Override
	public void createAdvParamPanels() {
		super.createAdvParamPanels();
		maxEvalsField = new JTextField();
		maxEvalsField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_maxEvalsField = new GridBagConstraints();
		gbc_maxEvalsField.anchor = GridBagConstraints.WEST;
		gbc_maxEvalsField.insets = new Insets(0, 0, 5, 5);
		gbc_maxEvalsField.gridx = 2;
		gbc_maxEvalsField.gridy = 4;
		advParamPanel.add(maxEvalsField, gbc_maxEvalsField);
		maxEvalsField.setText("500");
		maxEvalsField.setColumns(3);

		JLabel lblMaxEvals = new JLabel("Maximum Iterations");
		lblMaxEvals.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMaxEvals = new GridBagConstraints();
		gbc_lblMaxEvals.gridwidth = 3;
		gbc_lblMaxEvals.insets = new Insets(0, 0, 5, 0);
		gbc_lblMaxEvals.anchor = GridBagConstraints.WEST;
		gbc_lblMaxEvals.gridx = 3;
		gbc_lblMaxEvals.gridy = 4;
		advParamPanel.add(lblMaxEvals, gbc_lblMaxEvals);
	}
}
