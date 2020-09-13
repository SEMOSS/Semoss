/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import aurelienribon.ui.css.Style;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;



/**
 * This is the playsheet used exclusively for algorithms with numerous input parameters,
 * Generally this means optimization.
 * Top parameter input panel contains a basic param panel, 
 * advanced param components, algorithm types, and and run algorithm button.
 * Bottom display panel contains overall analysis tab with high level metrics and graphs,
 * console tab with log outputs, and any additional tabs with detailed results.
 */
@SuppressWarnings("serial")
public class OptPlaySheet extends InputPanelPlaySheet{
	
	//advanced param panel components
	protected JPanel advParamPanel;
	protected JToggleButton showParamBtn;
	
	
	/**
	 * Sets up the Param panel at the top of the split pane.
	 * Includes three major sections:
	 * 1) Basic param input on left side
	 * 2) Optimization button and any required selections for this on bottom left
	 * 3) Advanced param panel that toggles on/off on right side
	 */
	protected void createParamPanel()
	{
		super.createParamPanel();

		createBasicParamComponents();

		createOptimizationComponents();

		createAdvParamPanels();
		
		createAdvParamPanelsToggles();
		
		createAdvParamPanelsToggleListeners();
		
	}

	/**
	 *  Sets up the Basic param input on the left of the param panel
	 * 	Generally includes min/max budget and number of years. 
	 */
	protected void createBasicParamComponents() {

	}
	
	/**
	 * Sets up the optimization components on the bottom left of the param panel
	 * Adds a button for optimization and the listener.
	 * Extensions may also include different types of optimization:
	 * Savings, ROI, IRR, Break even, etc.
	 */
	protected void createOptimizationComponents() {
		//Button to run the optimization
		JButton btnRunOptimization = new CustomButton("Run Optimization");
		btnRunOptimization.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(btnRunOptimization,  ".createBtn");
		
		GridBagConstraints gbc_btnRunOptimization = new GridBagConstraints();
		gbc_btnRunOptimization.gridwidth = 4;
		gbc_btnRunOptimization.insets = new Insets(0, 0, 0, 5);
		gbc_btnRunOptimization.anchor = GridBagConstraints.WEST;
		gbc_btnRunOptimization.gridx = 1;
		gbc_btnRunOptimization.gridy = 5;
		ctlPanel.add(btnRunOptimization, gbc_btnRunOptimization);
		
		addOptimizationBtnListener(btnRunOptimization);
	}
	
	/**
	 * Adds the appropriate listener to the run optimization button.
	 * Extensions should include the proper type of listener
	 * @param btnRunOptimization
	 */
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
	}
	

	/**
	 * Sets up the advanced components on the right of the param panel
	 * Extensions may include additional panels: system selection panel, 
	 * data/blu selection panel, forced mod/decom extension panel , etc.
	 */
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

	}	

	/**
	 * Sets up the toggles to switch between advanced parameter panels.
	 * Extensions may include toggles to switch between additional panels: 
	 * system selection panel, data/blu selection panel, forced mod/decom panel , etc.
	 */
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
	
	/**
	 * Sets up the listeners to switch between advanced param panels
	 * Extensions include the logic for switching the panels.
	 */
	protected void createAdvParamPanelsToggleListeners() {
	}
	
	/**
	 * Sets up the display panel at the bottom of the split pane.
	 * tabbedPane is protected so any extending classes can add additional tabs for display
	 * overallAlysPanel is the original tab in the tabbedPane.
	 * Extensions should populate this with high level metric labels and the graphs
	 */
	protected void createDisplayPanel() {
		
		super.createDisplayPanel();
		
		overallAlysChartPanel = new JPanel();
		overallAlysChartPanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_chartPanel = new GridBagConstraints();
		gbc_chartPanel.anchor = GridBagConstraints.NORTHWEST;
		gbc_chartPanel.gridx = 1;
		gbc_chartPanel.gridy = 1;
		overallAlysPanel.add(overallAlysChartPanel, gbc_chartPanel);
		
		GridBagLayout gbl_chartPanel = new GridBagLayout();
		gbl_chartPanel.columnWidths = new int[]{0,0};
		gbl_chartPanel.rowHeights = new int[]{0,0};
		gbl_chartPanel.columnWeights = new double[]{0.0};
		gbl_chartPanel.rowWeights = new double[]{0.0};
		overallAlysChartPanel.setLayout(gbl_chartPanel);
	}

	
	//TODO comment
	protected JTextField addNewButtonToCtrlPanel(String fieldValue, String labelText, int width, int xLoc, int yLoc) {
		JTextField field = new JTextField();
		field.setFont(new Font("Tahoma", Font.PLAIN, 11));
		field.setText(fieldValue);
		field.setColumns(4);

		GridBagConstraints gbc_Field = new GridBagConstraints();
		gbc_Field.anchor = GridBagConstraints.NORTHWEST;
		gbc_Field.insets = new Insets(0, 0, 5, 5);
		gbc_Field.gridx = xLoc;//1;
		gbc_Field.gridy = yLoc;//1;
		ctlPanel.add(field, gbc_Field);

		JLabel lbl = new JLabel(labelText);
		lbl.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lbl = new GridBagConstraints();
		gbc_lbl.gridwidth = width;//4;
		gbc_lbl.anchor = GridBagConstraints.WEST;
		gbc_lbl.insets = new Insets(0, 0, 5, 5);
		gbc_lbl.gridx = xLoc + 1;//2;
		gbc_lbl.gridy = yLoc;//1;
		ctlPanel.add(lbl, gbc_lbl);

		return field;
	}
	
	//TODO comment
	protected JTextField addNewButtonToAdvParamPanel(String fieldValue, String labelText, int width, int xLoc, int yLoc) {
		JTextField field = new JTextField();
		field.setFont(new Font("Tahoma", Font.PLAIN, 11));
		field.setText(fieldValue);
		field.setColumns(3);
		
		GridBagConstraints gbc_Field = new GridBagConstraints();
		gbc_Field.insets = new Insets(0, 0, 5, 5);
		gbc_Field.gridx = xLoc;
		gbc_Field.gridy = yLoc;
		advParamPanel.add(field, gbc_Field);

		JLabel lbl = new JLabel(labelText);
		lbl.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lbl = new GridBagConstraints();
		gbc_lbl.anchor = GridBagConstraints.WEST;
		gbc_lbl.insets = new Insets(0, 0, 5, 5);
		gbc_lbl.gridx = xLoc+1;
		gbc_lbl.gridy = yLoc;
		advParamPanel.add(lbl, gbc_lbl);
		
		return field;
	}
	
	//TODO comment
	protected JRadioButton addOptimizationTypeButton(String btnText, int xLoc, int yLoc) {
		JRadioButton btn = new JRadioButton(btnText);
		btn.setName(btnText);
		btn.setFont(new Font("Tahoma", Font.PLAIN, 12));
		
		GridBagConstraints gbc_btn = new GridBagConstraints();
		gbc_btn.gridwidth = 2;
		gbc_btn.anchor = GridBagConstraints.WEST;
		gbc_btn.insets = new Insets(0, 0, 5, 5);
		gbc_btn.gridx = xLoc;
		gbc_btn.gridy = yLoc;
		ctlPanel.add(btn, gbc_btn);
		
		return btn;
	}
	
	//TODO comment
	protected BrowserGraphPanel addNewChartToOverviewPanel(int xLoc, int yLoc) {
		BrowserGraphPanel graph = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		graph.setPreferredSize(new Dimension(500, 400));
		graph.setMinimumSize(new Dimension(500, 400));
		graph.setVisible(false);
		
		GridBagConstraints gbc_graph = new GridBagConstraints();
		gbc_graph.insets = new Insets(0, 0, 0, 5);
		gbc_graph.fill = GridBagConstraints.BOTH;
		gbc_graph.gridx = xLoc;
		gbc_graph.gridy = yLoc;
		overallAlysChartPanel.add(graph,  gbc_graph);
		
		return graph;
	}
	
	//TODO comment
	protected JLabel addNewLabelToOverviewPanel(String lblText, int xLoc, int yLoc) {
		
		JLabel lblNewLabel = new JLabel(lblText);
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel.gridx = xLoc;
		gbc_lblNewLabel.gridy = yLoc;
		overallAlysMetricsPanel.add(lblNewLabel, gbc_lblNewLabel);

		JLabel lbl = new JLabel("");
		GridBagConstraints gbc_lbl = new GridBagConstraints();
		gbc_lbl.anchor = GridBagConstraints.WEST;
		gbc_lbl.insets = new Insets(0, 0, 5, 5);
		gbc_lbl.gridx = xLoc+1;
		gbc_lbl.gridy = yLoc;
		overallAlysMetricsPanel.add(lbl, gbc_lbl);
		
		return lbl;
	}
	
	//TODO comment
	protected JPanel addNewDisplayPanel(String panelLabel) {
		JPanel panel = new JPanel();
		displayTabbedPane.addTab(panelLabel, null, panel, null);
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0};
		gbl_panel.rowHeights = new int[]{0, 0};
		gbl_panel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gbl_panel);
		
		return panel;
	}

}
