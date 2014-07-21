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

import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import prerna.ui.main.listener.specific.tap.DHMSMSysDecomissionSchedulingBtnListener;
import prerna.ui.swing.custom.CustomButton;
import aurelienribon.ui.css.Style;

/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class DHMSMSysDecommissionSchedulingPlaySheet extends InputPanelPlaySheet{
	
	//param panel components
	public JTextField yearField, mtnPctgField;
	public JTextField minBudgetField, maxBudgetField;
		
	//display overall analysis components
//	public BrowserGraphPanel tab1, tab2, tab3, tab4, tab5, tab6, timeline;
//	public JLabel savingLbl, costLbl, roiLbl, bkevenLbl, recoupLbl;
	
	//other display components
	public JPanel specificSysAlysPanel;

	/**
	 * Sets up the Param panel at the top of the split pane
	 */
	public void createGenericParamPanel()
	{
		super.createGenericParamPanel();

		yearField = new JTextField();
		yearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		yearField.setText("10");
		yearField.setColumns(4);

		GridBagConstraints gbc_yearField = new GridBagConstraints();
		gbc_yearField.anchor = GridBagConstraints.NORTHWEST;
		gbc_yearField.insets = new Insets(0, 0, 5, 5);
		gbc_yearField.gridx = 1;
		gbc_yearField.gridy = 1;
		ctlPanel.add(yearField, gbc_yearField);

		JLabel label = new JLabel("Maximum Number of Years");
		label.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.gridwidth = 4;
		gbc_label.anchor = GridBagConstraints.WEST;
		gbc_label.insets = new Insets(0, 0, 5, 5);
		gbc_label.gridx = 2;
		gbc_label.gridy = 1;
		ctlPanel.add(label, gbc_label);
		
		mtnPctgField = new JTextField();
		mtnPctgField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		mtnPctgField.setText("18");
		mtnPctgField.setColumns(4);
		GridBagConstraints gbc_mtnPctgField = new GridBagConstraints();
		gbc_mtnPctgField.insets = new Insets(0, 0, 5, 5);
		gbc_mtnPctgField.anchor = GridBagConstraints.NORTHWEST;
		gbc_mtnPctgField.gridx = 1;
		gbc_mtnPctgField.gridy = 2;
		ctlPanel.add(mtnPctgField, gbc_mtnPctgField);

		JLabel lblSoaSustainmentCost = new JLabel("Annual Service Sustainment Percentage (%)");
		lblSoaSustainmentCost.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSoaSustainmentCost = new GridBagConstraints();
		gbc_lblSoaSustainmentCost.anchor = GridBagConstraints.WEST;
		gbc_lblSoaSustainmentCost.gridwidth = 4;
		gbc_lblSoaSustainmentCost.insets = new Insets(0, 0, 5, 5);
		gbc_lblSoaSustainmentCost.gridx = 2;
		gbc_lblSoaSustainmentCost.gridy = 2;
		ctlPanel.add(lblSoaSustainmentCost, gbc_lblSoaSustainmentCost);


		minBudgetField = new JTextField();
		minBudgetField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		minBudgetField.setText("0");
		minBudgetField.setColumns(3);
		GridBagConstraints gbc_minBudgetField = new GridBagConstraints();
		gbc_minBudgetField.anchor = GridBagConstraints.WEST;
		gbc_minBudgetField.insets = new Insets(0, 0, 5, 5);
		gbc_minBudgetField.gridx = 6;
		gbc_minBudgetField.gridy = 1;
//		ctlPanel.add(minBudgetField, gbc_minBudgetField);

		JLabel lblMinimumYearlyBudget = new JLabel("Minimum Annual Budget ($M)");
		lblMinimumYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMinimumYearlyBudget = new GridBagConstraints();
		gbc_lblMinimumYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblMinimumYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblMinimumYearlyBudget.gridx = 7;
		gbc_lblMinimumYearlyBudget.gridy = 1;
//		ctlPanel.add(lblMinimumYearlyBudget, gbc_lblMinimumYearlyBudget);

		maxBudgetField = new JTextField();
		maxBudgetField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		maxBudgetField.setText("1000");
		maxBudgetField.setColumns(3);
		GridBagConstraints gbc_maxBudgetField = new GridBagConstraints();
		gbc_maxBudgetField.anchor = GridBagConstraints.WEST;
		gbc_maxBudgetField.insets = new Insets(0, 0, 5, 5);
		gbc_maxBudgetField.gridx = 6;
		gbc_maxBudgetField.gridy = 2;
//		ctlPanel.add(maxBudgetField, gbc_maxBudgetField);

		JLabel lblMaximumYearlyBudget = new JLabel("Maximum Annual Budget ($M)");
		lblMaximumYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMaximumYearlyBudget = new GridBagConstraints();
		gbc_lblMaximumYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblMaximumYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblMaximumYearlyBudget.gridx = 7;
		gbc_lblMaximumYearlyBudget.gridy = 2;
//		ctlPanel.add(lblMaximumYearlyBudget, gbc_lblMaximumYearlyBudget);

		Object hidePopupKey = new JComboBox<String>().getClientProperty("doNotCancelPopup");  
		JButton btnRunOptimization = new CustomButton("Run Optimization");
		btnRunOptimization.putClientProperty("doNotCancelPopup", hidePopupKey);

		btnRunOptimization.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRunOptimization = new GridBagConstraints();
		gbc_btnRunOptimization.gridwidth = 4;
		gbc_btnRunOptimization.insets = new Insets(0, 0, 0, 5);
		gbc_btnRunOptimization.anchor = GridBagConstraints.WEST;
		gbc_btnRunOptimization.gridx = 1;
		gbc_btnRunOptimization.gridy = 5;
		ctlPanel.add(btnRunOptimization, gbc_btnRunOptimization);
		addOptimizationBtnListener(btnRunOptimization);
		Style.registerTargetClassName(btnRunOptimization,  ".createBtn");
	}
	
	public void addOptimizationBtnListener(JButton btnRunOptimization)
	{
		DHMSMSysDecomissionSchedulingBtnListener obl = new DHMSMSysDecomissionSchedulingBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	
	public void createSpecificDisplayComponents()
	{
		specificSysAlysPanel = new JPanel();
		tabbedPane.addTab("System Analysis", null, specificSysAlysPanel, null);
		GridBagLayout gbl_specificSysAlysPanel = new GridBagLayout();
		gbl_specificSysAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificSysAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificSysAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificSysAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		specificSysAlysPanel.setLayout(gbl_specificSysAlysPanel);
	}
	
	public void createGenericDisplayPanel()
	{		
		super.createGenericDisplayPanel();
		
		chartPanel = new JPanel();
		chartPanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_chartPanel = new GridBagConstraints();
		gbc_chartPanel.anchor = GridBagConstraints.NORTHWEST;
		gbc_chartPanel.gridx = 1;
		gbc_chartPanel.gridy = 1;
		overallAlysPanel.add(chartPanel, gbc_chartPanel);
		GridBagLayout gbl_chartPanel = new GridBagLayout();
		gbl_chartPanel.columnWidths = new int[]{0,0};
		gbl_chartPanel.rowHeights = new int[]{0,0};
		gbl_chartPanel.columnWeights = new double[]{0.0};
		gbl_chartPanel.rowWeights = new double[]{0.0};
		chartPanel.setLayout(gbl_chartPanel);
		
		createSpecificDisplayComponents();
	}
}
