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
import javax.swing.JTextPane;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import aurelienribon.ui.css.Style;



/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class OptPlaySheet extends InputPanelPlaySheet{

	//param panel components
	public JLabel lblSoaSustainmentCost;
	public JTextField yearField, icdSusField, mtnPctgField;
	public JTextField minBudgetField, maxBudgetField, hourlyRateField;
	
	//advanced param panel components
	public JPanel advParamPanel;
	public JToggleButton showParamBtn;
	public JTextField iniLearningCnstField, scdLearningTimeField, scdLearningCnstField, startingPtsField;
	public JTextField attRateField, hireRateField, infRateField,disRateField;
	public ButtonMenuDropDown sysSelect;
	public JComboBox<String> sysSpecComboBox;
	
	//display overall analysis components
	public BrowserGraphPanel tab1, tab2, tab3, tab4, tab5, tab6, timeline;
	public JLabel savingLbl, costLbl, roiLbl, bkevenLbl, recoupLbl;
	
	//other display components
	public JPanel specificAlysPanel = new JPanel();
	public JPanel playSheetPanel = new JPanel();
	public JPanel specificSysAlysPanel;
	public JPanel timelinePanel;
	public JTextPane helpTextArea;

	public JLabel lblInvestmentRecoupTime;
	
	
	/**
	 * Sets up the Param panel at the top of the split pane
	 */
	protected void createParamPanel()
	{
		super.createParamPanel();

		createBasicParamComponents();

		createOptimizationTypeComponents();

		createAdvParamPanels();
		
		createAdvParamPanelsToggles();
		
		createAdvParamPanelsToggleListeners();
		
	}

	protected void createBasicParamComponents() {

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

		minBudgetField = new JTextField();
		minBudgetField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		minBudgetField.setText("0");
		minBudgetField.setColumns(3);
		GridBagConstraints gbc_minBudgetField = new GridBagConstraints();
		gbc_minBudgetField.anchor = GridBagConstraints.WEST;
		gbc_minBudgetField.insets = new Insets(0, 0, 5, 5);
		gbc_minBudgetField.gridx = 6;
		gbc_minBudgetField.gridy = 1;
		ctlPanel.add(minBudgetField, gbc_minBudgetField);

		JLabel lblMinimumYearlyBudget = new JLabel("Minimum Annual Budget ($M)");
		lblMinimumYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMinimumYearlyBudget = new GridBagConstraints();
		gbc_lblMinimumYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblMinimumYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblMinimumYearlyBudget.gridx = 7;
		gbc_lblMinimumYearlyBudget.gridy = 1;
		ctlPanel.add(lblMinimumYearlyBudget, gbc_lblMinimumYearlyBudget);

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

		lblSoaSustainmentCost = new JLabel("Annual Service Sustainment Percentage (%)");
		lblSoaSustainmentCost.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSoaSustainmentCost = new GridBagConstraints();
		gbc_lblSoaSustainmentCost.anchor = GridBagConstraints.WEST;
		gbc_lblSoaSustainmentCost.gridwidth = 4;
		gbc_lblSoaSustainmentCost.insets = new Insets(0, 0, 5, 5);
		gbc_lblSoaSustainmentCost.gridx = 2;
		gbc_lblSoaSustainmentCost.gridy = 2;
		ctlPanel.add(lblSoaSustainmentCost, gbc_lblSoaSustainmentCost);

		maxBudgetField = new JTextField();
		maxBudgetField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		maxBudgetField.setText("100");
		maxBudgetField.setColumns(3);
		GridBagConstraints gbc_maxBudgetField = new GridBagConstraints();
		gbc_maxBudgetField.anchor = GridBagConstraints.WEST;
		gbc_maxBudgetField.insets = new Insets(0, 0, 5, 5);
		gbc_maxBudgetField.gridx = 6;
		gbc_maxBudgetField.gridy = 2;
		ctlPanel.add(maxBudgetField, gbc_maxBudgetField);

		JLabel lblMaximumYearlyBudget = new JLabel("Maximum Annual Budget ($M)");
		lblMaximumYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMaximumYearlyBudget = new GridBagConstraints();
		gbc_lblMaximumYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblMaximumYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblMaximumYearlyBudget.gridx = 7;
		gbc_lblMaximumYearlyBudget.gridy = 2;
		ctlPanel.add(lblMaximumYearlyBudget, gbc_lblMaximumYearlyBudget);

		hourlyRateField = new JTextField();
		hourlyRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		hourlyRateField.setText("150");
		hourlyRateField.setColumns(3);

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
	
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
	}
	
	
	protected void createOptimizationTypeComponents() {
	}


	protected void createAdvParamPanels()
	{
		advParamPanel = new JPanel();
		advParamPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		advParamPanel.setVisible(false);
		
		GridBagConstraints gbc_advParamPanel = new GridBagConstraints();
		gbc_advParamPanel.gridheight = 6;
		gbc_advParamPanel.fill = GridBagConstraints.BOTH;
		gbc_advParamPanel.gridx = 8;
		gbc_advParamPanel.gridy = 0;
		ctlPanel.add(advParamPanel, gbc_advParamPanel);
		
		GridBagLayout gbl_advParamPanel = new GridBagLayout();
		gbl_advParamPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_advParamPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_advParamPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_advParamPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		advParamPanel.setLayout(gbl_advParamPanel);

		JLabel lblAdvancedParameters = new JLabel("Advanced Input Parameters:");
		lblAdvancedParameters.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblAdvancedParameters = new GridBagConstraints();
		gbc_lblAdvancedParameters.gridwidth = 5;
		gbc_lblAdvancedParameters.anchor = GridBagConstraints.WEST;
		gbc_lblAdvancedParameters.insets = new Insets(10, 0, 5, 5);
		gbc_lblAdvancedParameters.gridx = 0;
		gbc_lblAdvancedParameters.gridy = 0;
		advParamPanel.add(lblAdvancedParameters, gbc_lblAdvancedParameters);

		attRateField = new JTextField();
		attRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_attRateField = new GridBagConstraints();
		gbc_attRateField.insets = new Insets(0, 0, 5, 5);
		gbc_attRateField.gridx = 0;
		gbc_attRateField.gridy = 1;
		advParamPanel.add(attRateField, gbc_attRateField);
		attRateField.setText("3");
		attRateField.setColumns(3);

		JLabel lblYearlyRetentionRate = new JLabel("Attrition Rate (%)");
		lblYearlyRetentionRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblYearlyRetentionRate = new GridBagConstraints();
		gbc_lblYearlyRetentionRate.anchor = GridBagConstraints.WEST;
		gbc_lblYearlyRetentionRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblYearlyRetentionRate.gridx = 1;
		gbc_lblYearlyRetentionRate.gridy = 1;
		advParamPanel.add(lblYearlyRetentionRate, gbc_lblYearlyRetentionRate);

		iniLearningCnstField = new JTextField();
		iniLearningCnstField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_iniLearningCnstField = new GridBagConstraints();
		gbc_iniLearningCnstField.insets = new Insets(0, 0, 5, 5);
		gbc_iniLearningCnstField.gridx = 2;
		gbc_iniLearningCnstField.gridy = 1;
		advParamPanel.add(iniLearningCnstField, gbc_iniLearningCnstField);
		iniLearningCnstField.setText("0");
		iniLearningCnstField.setColumns(3);

		JLabel lblNewLabel_1 = new JLabel("Experience Level at year 0 (%)");
		lblNewLabel_1.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblNewLabel_1 = new GridBagConstraints();
		gbc_lblNewLabel_1.gridwidth = 3;
		gbc_lblNewLabel_1.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_1.insets = new Insets(0, 0, 5, 0);
		gbc_lblNewLabel_1.gridx = 3;
		gbc_lblNewLabel_1.gridy = 1;
		advParamPanel.add(lblNewLabel_1, gbc_lblNewLabel_1);

		hireRateField = new JTextField();
		hireRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_hireRateField = new GridBagConstraints();
		gbc_hireRateField.insets = new Insets(0, 0, 5, 5);
		gbc_hireRateField.gridx = 0;
		gbc_hireRateField.gridy = 2;
		advParamPanel.add(hireRateField, gbc_hireRateField);
		hireRateField.setText("3");
		hireRateField.setColumns(3);

		JLabel lblHiringRate = new JLabel("Hiring Rate (%)");
		lblHiringRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblHiringRate = new GridBagConstraints();
		gbc_lblHiringRate.anchor = GridBagConstraints.WEST;
		gbc_lblHiringRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblHiringRate.gridx = 1;
		gbc_lblHiringRate.gridy = 2;
		advParamPanel.add(lblHiringRate, gbc_lblHiringRate);

		scdLearningCnstField = new JTextField();
		scdLearningCnstField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_scdLearningCnstField = new GridBagConstraints();
		gbc_scdLearningCnstField.insets = new Insets(0, 0, 5, 5);
		gbc_scdLearningCnstField.gridx = 2;
		gbc_scdLearningCnstField.gridy = 2;
		advParamPanel.add(scdLearningCnstField, gbc_scdLearningCnstField);
		scdLearningCnstField.setText("0.9");
		scdLearningCnstField.setColumns(3);

		JLabel lblSecondLearningCurve_1 = new JLabel("Experience Level at year");
		lblSecondLearningCurve_1.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSecondLearningCurve_1 = new GridBagConstraints();
		gbc_lblSecondLearningCurve_1.gridwidth = 2;
		gbc_lblSecondLearningCurve_1.anchor = GridBagConstraints.WEST;
		gbc_lblSecondLearningCurve_1.insets = new Insets(0, 0, 5, 5);
		gbc_lblSecondLearningCurve_1.gridx = 3;
		gbc_lblSecondLearningCurve_1.gridy = 2;
		advParamPanel.add(lblSecondLearningCurve_1, gbc_lblSecondLearningCurve_1);

		scdLearningTimeField = new JTextField();
		scdLearningTimeField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_scdLearningTimeField = new GridBagConstraints();
		gbc_scdLearningTimeField.anchor = GridBagConstraints.WEST;
		gbc_scdLearningTimeField.insets = new Insets(0, 0, 5, 0);
		gbc_scdLearningTimeField.gridx = 5;
		gbc_scdLearningTimeField.gridy = 2;
		advParamPanel.add(scdLearningTimeField, gbc_scdLearningTimeField);
		scdLearningTimeField.setText("5");
		scdLearningTimeField.setColumns(3);

		infRateField = new JTextField();
		infRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		infRateField.setText("1.5");
		infRateField.setColumns(3);
		GridBagConstraints gbc_infRateField = new GridBagConstraints();
		gbc_infRateField.insets = new Insets(0, 0, 5, 5);
		gbc_infRateField.fill = GridBagConstraints.HORIZONTAL;
		gbc_infRateField.gridx = 0;
		gbc_infRateField.gridy = 3;
		advParamPanel.add(infRateField, gbc_infRateField);

		JLabel lblYearlyInflationRate = new JLabel("Inflation Rate (%)");
		lblYearlyInflationRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblYearlyInflationRate = new GridBagConstraints();
		gbc_lblYearlyInflationRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblYearlyInflationRate.anchor = GridBagConstraints.WEST;
		gbc_lblYearlyInflationRate.gridx = 1;
		gbc_lblYearlyInflationRate.gridy = 3;
		advParamPanel.add(lblYearlyInflationRate, gbc_lblYearlyInflationRate);

		startingPtsField = new JTextField();
		startingPtsField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_startingPtsField = new GridBagConstraints();
		gbc_startingPtsField.anchor = GridBagConstraints.WEST;
		gbc_startingPtsField.insets = new Insets(0, 0, 5, 5);
		gbc_startingPtsField.gridx = 2;
		gbc_startingPtsField.gridy = 3;
		advParamPanel.add(startingPtsField, gbc_startingPtsField);
		startingPtsField.setText("5");
		startingPtsField.setColumns(3);

		JLabel lblInitialYearlyBudget = new JLabel("Number of Starting Points");
		lblInitialYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblInitialYearlyBudget = new GridBagConstraints();
		gbc_lblInitialYearlyBudget.gridwidth = 3;
		gbc_lblInitialYearlyBudget.insets = new Insets(0, 0, 5, 0);
		gbc_lblInitialYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblInitialYearlyBudget.gridx = 3;
		gbc_lblInitialYearlyBudget.gridy = 3;
		advParamPanel.add(lblInitialYearlyBudget, gbc_lblInitialYearlyBudget);

		disRateField = new JTextField();
		disRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		disRateField.setText("2.5");
		disRateField.setColumns(3);
		GridBagConstraints gbc_disRateField = new GridBagConstraints();
		gbc_disRateField.insets = new Insets(0, 0, 5, 5);
		gbc_disRateField.fill = GridBagConstraints.HORIZONTAL;
		gbc_disRateField.gridx = 0;
		gbc_disRateField.gridy = 4;
		advParamPanel.add(disRateField, gbc_disRateField);

		JLabel lblYearlyDiscountRate = new JLabel("Discount Rate (%)");
		lblYearlyDiscountRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblYearlyDiscountRate = new GridBagConstraints();
		gbc_lblYearlyDiscountRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblYearlyDiscountRate.gridx = 1;
		gbc_lblYearlyDiscountRate.gridy = 4;
		advParamPanel.add(lblYearlyDiscountRate, gbc_lblYearlyDiscountRate);


	}
	protected void createAdvParamPanelsToggles() {
		showParamBtn = new ToggleButton("Show Advanced Parameters");
		showParamBtn.setName("showParamBtn");
		showParamBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showParamBtn,  ".toggleButton");
		
		GridBagConstraints gbc_showParamBtn = new GridBagConstraints();
		gbc_showParamBtn.anchor = GridBagConstraints.WEST;
		gbc_showParamBtn.gridwidth = 2;
		gbc_showParamBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showParamBtn.gridx = 6;
		gbc_showParamBtn.gridy = 4;
		ctlPanel.add(showParamBtn, gbc_showParamBtn);

	}
	
	protected void createAdvParamPanelsToggleListeners() {
	}
	
	protected void createDisplayPanel()
	{
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
		tab6 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab6.setPreferredSize(new Dimension(500, 400));
		tab6.setMinimumSize(new Dimension(500, 400));
		tab6.setVisible(false);

		timeline = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/timeline.html");
		
		super.createDisplayPanel();

		JLabel lblNewLabel = new JLabel("Total transition savings over time horizon:");
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel.gridx = 0;
		gbc_lblNewLabel.gridy = 1;
		panel_1.add(lblNewLabel, gbc_lblNewLabel);

		savingLbl = new JLabel("");
		GridBagConstraints gbc_savingLbl = new GridBagConstraints();
		gbc_savingLbl.anchor = GridBagConstraints.WEST;
		gbc_savingLbl.insets = new Insets(0, 0, 5, 5);
		gbc_savingLbl.gridx = 1;
		gbc_savingLbl.gridy = 1;
		panel_1.add(savingLbl, gbc_savingLbl);

		JLabel lblTotalRoiOver = new JLabel("Total ROI over time horizon:");
		GridBagConstraints gbc_lblTotalRoiOver = new GridBagConstraints();
		gbc_lblTotalRoiOver.anchor = GridBagConstraints.WEST;
		gbc_lblTotalRoiOver.insets = new Insets(0, 30, 5, 5);
		gbc_lblTotalRoiOver.gridx = 2;
		gbc_lblTotalRoiOver.gridy = 1;
		panel_1.add(lblTotalRoiOver, gbc_lblTotalRoiOver);

		roiLbl = new JLabel("");
		GridBagConstraints gbc_roiLbl = new GridBagConstraints();
		gbc_roiLbl.anchor = GridBagConstraints.WEST;
		gbc_roiLbl.insets = new Insets(0, 0, 5, 5);
		gbc_roiLbl.gridx = 3;
		gbc_roiLbl.gridy = 1;
		panel_1.add(roiLbl, gbc_roiLbl);
		
		JLabel lblBreakevenPointDuring = new JLabel("Breakeven point during time horizon:");
		GridBagConstraints gbc_lblBreakevenPointDuring = new GridBagConstraints();
		gbc_lblBreakevenPointDuring.anchor = GridBagConstraints.WEST;
		gbc_lblBreakevenPointDuring.insets = new Insets(0, 30, 0, 5);
		gbc_lblBreakevenPointDuring.gridx = 2;
		gbc_lblBreakevenPointDuring.gridy = 2;
		panel_1.add(lblBreakevenPointDuring, gbc_lblBreakevenPointDuring);

		bkevenLbl = new JLabel("");
		GridBagConstraints gbc_bkevenLbl = new GridBagConstraints();
		gbc_bkevenLbl.insets = new Insets(0, 0, 0, 5);
		gbc_bkevenLbl.anchor = GridBagConstraints.WEST;
		gbc_bkevenLbl.gridx = 3;
		gbc_bkevenLbl.gridy = 2;
		panel_1.add(bkevenLbl, gbc_bkevenLbl);
		
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

		GridBagConstraints gbc_panel4 = new GridBagConstraints();
		gbc_panel4.insets = new Insets(0, 0, 0, 5);
		gbc_panel4.fill = GridBagConstraints.BOTH;
		gbc_panel4.gridx = 1;
		gbc_panel4.gridy = 2;
		chartPanel.add(tab6,  gbc_panel4);

		//second tab: timeline panel
		timelinePanel = new JPanel();
		tabbedPane.addTab("Timeline", null, timelinePanel, null);
		GridBagLayout gbl_timelinePanel = new GridBagLayout();
		gbl_timelinePanel.columnWidths = new int[]{0, 0};
		gbl_timelinePanel.rowHeights = new int[]{0, 0};
		gbl_timelinePanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_timelinePanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		timelinePanel.setLayout(gbl_timelinePanel);

		//		timeline = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/timeline.html");
		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.fill = GridBagConstraints.BOTH;
		gbc_panel_2.gridx = 0;
		gbc_panel_2.gridy = 0;
		timelinePanel.add(timeline, gbc_panel_2);
		timeline.setVisible(false);

	}

	public void setGraphsVisible(boolean visible) {

	}
	
	/**
	 * Clears panels within the playsheet
	 */
	public void clearPanels() {
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations. Allows for different TAP algorithms to be run as empty functions.
	 */
	public void clearLabels()
	{
	}

}
