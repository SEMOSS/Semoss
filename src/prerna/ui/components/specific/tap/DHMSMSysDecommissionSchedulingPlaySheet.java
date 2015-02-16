/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import prerna.ui.components.BrowserGraphPanel;
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
	public BrowserGraphPanel tab1, tab2, tab3, tab4, tab5;
	public JLabel savingLbl, netSavingLbl, roiLbl, investLbl, budgetLbl;
	
	//other display components
	public JPanel sysNumSitesAnalPanel;
	public JPanel sysCostSavingsAnalPanel;
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
		sysNumSitesAnalPanel = new JPanel();
		tabbedPane.addTab("System Site Analysis", null, sysNumSitesAnalPanel, null);
		GridBagLayout gbl_specificSysAlysPanel = new GridBagLayout();
		gbl_specificSysAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificSysAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificSysAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificSysAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		sysNumSitesAnalPanel.setLayout(gbl_specificSysAlysPanel);
		
		sysCostSavingsAnalPanel = new JPanel();
		tabbedPane.addTab("System Cost/Savings Analysis", null, sysCostSavingsAnalPanel, null);
		GridBagLayout gbl_sysCostSavingsAnalPanel = new GridBagLayout();
		gbl_sysCostSavingsAnalPanel.columnWidths = new int[]{0, 0};
		gbl_sysCostSavingsAnalPanel.rowHeights = new int[]{0, 0};
		gbl_sysCostSavingsAnalPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_sysCostSavingsAnalPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		sysCostSavingsAnalPanel.setLayout(gbl_sysCostSavingsAnalPanel);
	}
	
	public void createGenericDisplayPanel()
	{		
		super.createGenericDisplayPanel();
		//	public JLabel savingLbl, netSavingLbl, roiLbl, investLbl, budgetLbl;
		JLabel lblSavingLabel = new JLabel("Total transition savings over time horizon:");
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel.gridx = 0;
		gbc_lblNewLabel.gridy = 1;
		panel_1.add(lblSavingLabel, gbc_lblNewLabel);

		savingLbl = new JLabel("");
		GridBagConstraints gbc_savingLbl = new GridBagConstraints();
		gbc_savingLbl.anchor = GridBagConstraints.WEST;
		gbc_savingLbl.insets = new Insets(0, 0, 5, 5);
		gbc_savingLbl.gridx = 1;
		gbc_savingLbl.gridy = 1;
		panel_1.add(savingLbl, gbc_savingLbl);
		
		JLabel lblNetSavinglabel = new JLabel("Total net transition savings over time horizon:");
		GridBagConstraints gbc_lblNetSavinglabel = new GridBagConstraints();
		gbc_lblNetSavinglabel.anchor = GridBagConstraints.WEST;
		gbc_lblNetSavinglabel.insets = new Insets(0, 30, 5, 5);
		gbc_lblNetSavinglabel.gridx = 2;
		gbc_lblNetSavinglabel.gridy = 1;
		panel_1.add(lblNetSavinglabel, gbc_lblNetSavinglabel);

		netSavingLbl = new JLabel("");
		GridBagConstraints gbc_netSavingLbl = new GridBagConstraints();
		gbc_netSavingLbl.anchor = GridBagConstraints.WEST;
		gbc_netSavingLbl.insets = new Insets(0, 0, 5, 5);
		gbc_netSavingLbl.gridx = 3;
		gbc_netSavingLbl.gridy = 1;
		panel_1.add(netSavingLbl, gbc_netSavingLbl);

		JLabel lblTotalRoiOver = new JLabel("Total ROI over time horizon:");
		GridBagConstraints gbc_lblTotalRoiOver = new GridBagConstraints();
		gbc_lblTotalRoiOver.anchor = GridBagConstraints.WEST;
		gbc_lblTotalRoiOver.insets = new Insets(0, 30, 5, 5);
		gbc_lblTotalRoiOver.gridx = 4;
		gbc_lblTotalRoiOver.gridy = 1;
		panel_1.add(lblTotalRoiOver, gbc_lblTotalRoiOver);

		roiLbl = new JLabel("");
		GridBagConstraints gbc_roiLbl = new GridBagConstraints();
		gbc_roiLbl.anchor = GridBagConstraints.WEST;
		gbc_roiLbl.insets = new Insets(0, 0, 5, 5);
		gbc_roiLbl.gridx = 5;
		gbc_roiLbl.gridy = 1;
		panel_1.add(roiLbl, gbc_roiLbl);
		
		JLabel lblInvestLabel = new JLabel("Total investment cost:");
		GridBagConstraints gbc_lblInvestLabel = new GridBagConstraints();
		gbc_lblInvestLabel.anchor = GridBagConstraints.WEST;
		gbc_lblInvestLabel.insets = new Insets(0, 0, 0, 5);
		gbc_lblInvestLabel.gridx = 0;
		gbc_lblInvestLabel.gridy = 2;
		panel_1.add(lblInvestLabel, gbc_lblInvestLabel);

		investLbl = new JLabel("");
		GridBagConstraints gbc_investLbl = new GridBagConstraints();
		gbc_investLbl.insets = new Insets(0, 0, 0, 5);
		gbc_investLbl.anchor = GridBagConstraints.WEST;
		gbc_investLbl.gridx = 1;
		gbc_investLbl.gridy = 2;
		panel_1.add(investLbl, gbc_investLbl);
		
		JLabel lblBudgetLabel = new JLabel("Annual Budget during transition:");
		GridBagConstraints gbc_lblBudgetLabel = new GridBagConstraints();
		gbc_lblBudgetLabel.anchor = GridBagConstraints.WEST;
		gbc_lblBudgetLabel.insets = new Insets(0, 30, 0, 5);
		gbc_lblBudgetLabel.gridx = 2;
		gbc_lblBudgetLabel.gridy = 2;
		panel_1.add(lblBudgetLabel, gbc_lblBudgetLabel);

		budgetLbl = new JLabel("");
		GridBagConstraints gbc_budgetLbl = new GridBagConstraints();
		gbc_budgetLbl.insets = new Insets(0, 0, 0, 5);
		gbc_budgetLbl.anchor = GridBagConstraints.WEST;
		gbc_budgetLbl.gridx = 3;
		gbc_budgetLbl.gridy = 2;
		panel_1.add(budgetLbl, gbc_budgetLbl);
		
		
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
		
		tab1 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab1.setPreferredSize(new Dimension(500, 400));
		tab1.setMinimumSize(new Dimension(500, 400));
		tab1.setVisible(false);
		tab2 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab2.setPreferredSize(new Dimension(500, 400));
		tab2.setMinimumSize(new Dimension(500, 400));
		tab2.setVisible(false);
		tab3 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab3.setPreferredSize(new Dimension(500, 400));
		tab3.setMinimumSize(new Dimension(500, 400));
		tab3.setVisible(false);
		tab4 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab4.setPreferredSize(new Dimension(500, 400));
		tab4.setMinimumSize(new Dimension(500, 400));
		tab4.setVisible(false);
		tab5 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab5.setPreferredSize(new Dimension(500, 400));
		tab5.setMinimumSize(new Dimension(500, 400));
		tab5.setVisible(false);
		
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		chartPanel.add(tab1, gbc_panel_1_1);

		GridBagConstraints gbc_advParamPanel1 = new GridBagConstraints();
		gbc_advParamPanel1.insets = new Insets(0, 0, 5, 0);
		gbc_advParamPanel1.fill = GridBagConstraints.BOTH;
		gbc_advParamPanel1.gridx = 1;
		gbc_advParamPanel1.gridy = 0;
		chartPanel.add(tab2, gbc_advParamPanel1);
		
		GridBagConstraints gbc_panel = new GridBagConstraints();
		gbc_panel.insets = new Insets(0, 0, 0, 5);
		gbc_panel.fill = GridBagConstraints.BOTH;
		gbc_panel.gridx = 0;
		gbc_panel.gridy = 1;
		chartPanel.add(tab3,  gbc_panel);
		
		GridBagConstraints gbc_panel2 = new GridBagConstraints();
		gbc_panel2.insets = new Insets(0, 0, 0, 5);
		gbc_panel2.fill = GridBagConstraints.BOTH;
		gbc_panel2.gridx = 1;
		gbc_panel2.gridy = 1;
		chartPanel.add(tab4,  gbc_panel2);

		GridBagConstraints gbc_panel3 = new GridBagConstraints();
		gbc_panel3.insets = new Insets(0, 0, 0, 5);
		gbc_panel3.fill = GridBagConstraints.BOTH;
		gbc_panel3.gridx = 0;
		gbc_panel3.gridy = 2;
		chartPanel.add(tab5,  gbc_panel3);
		
		createSpecificDisplayComponents();
	}
}
