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

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.SystemColor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import aurelienribon.ui.css.Style;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.main.listener.specific.tap.SysOptBtnListener;
import prerna.ui.main.listener.specific.tap.UpdateDataBLUListListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;

/**
 * This is the playsheet used exclusively for TAP system optimization.
 * Optimizes systems to find minimum future sustainment budget while still maintaining 
 * same functionality (data and business logic) either enterprise wide or regionally 
 * and in same environments (theater/garrison) if desired.
 * 
 */
@SuppressWarnings("serial")
public class SysOptPlaySheet extends OptPlaySheet {
	
	private SysOptCheckboxListUpdater checkboxListUpdater;
	
	//param panel components
	public JTextField yearField, icdSusField, mtnPctgField;
	public JTextField minBudgetField, maxBudgetField, hourlyRateField;
	
	//advanced param panel components
	public JTextField iniLearningCnstField, scdLearningTimeField, scdLearningCnstField, startingPtsField;
	public JTextField attRateField, hireRateField, infRateField,disRateField;
	public JCheckBox includeRegionalizationCheckbox;
	public JCheckBox garrTheaterCheckbox;
	
	// toggles to show the system Functionality and Capability Functionality panels
	public JToggleButton showSystemSelectBtn, showSystemCapSelectBtn, showSystemModDecomBtn;
	// panel that holds the system,data,blu and capability selectors if needed
	public JPanel systemDataBLUSelectPanel;
	public JPanel systemModDecomSelectPanel;
	// individual components of the panel
	public DHMSMSystemSelectPanel systemSelectPanel, systemModernizePanel, systemDecomissionPanel;
	public DHMSMCapabilitySelectPanel capabilitySelectPanel;
	public DHMSMDataBLUSelectPanel dataBLUSelectPanel;
	
	// toggle to show the data/blu panel (dataBLUSelectPanel) within the systemDataBLUSelectPanel
	public JToggleButton updateDataBLUPanelButton;
	
	// param panel components to select the type of optimization to run
	public JRadioButton rdbtnProfit, rdbtnROI, rdbtnIRR;
	
	//display components - overview tab showing high level metrics after algorithm is run
	public JLabel solutionLbl, irrLbl, annualBudgetLbl, timeTransitionLbl, savingLbl, roiLbl, bkevenLbl;
	//display components - overview tab showing graphs after algorithm is run
	public BrowserGraphPanel tab3, tab4, tab5, tab6;
	//display components - additional tabs showing functionality after algorithm is run
	public BrowserGraphPanel replacementHeatMap, geoSpatialMap;
	public JPanel specificSysAlysPanel, currentFuncPanel, futureFuncPanel, replacementHeatMapPanel, sysCapOptionsPanel, sysCapDisplayPanel,
			geoSpatialOptionsPanel;
	public static JComboBox<String> capComboBox, sysComboBox, geoCapComboBox;
	public static JButton createSysCapButton, createGeoSpatialMapButton;
//TODO why are these static?
	
	/**
	 * Creates the data needed for the system, capability, data, and blu selection/scroll lists.
	 */
	@Override
	public void createData() {
		checkboxListUpdater = new SysOptCheckboxListUpdater(engine);
		
	}
	
	/**
	 *  Sets up the Basic param input on the left of the param panel
	 * 	Includes number of years, maintenance percentage for exposed data,
	 * 	hourly build cost rate, minimum and maximum annual budgets
	 */
	@Override
	protected void createBasicParamComponents() {
		
		super.createBasicParamComponents();
		yearField = addNewButtonToCtrlPanel("10", "Maximum Number of Years", 4, 1, 1);
		mtnPctgField = addNewButtonToCtrlPanel("10", "Annual Maint Exposed Data (%)", 4, 1,2);
		hourlyRateField = addNewButtonToCtrlPanel("150", "Hourly Build Cost Rate ($)", 4, 1, 3);
		minBudgetField = addNewButtonToCtrlPanel("0", "Minimum Annual Budget ($M)", 1, 6, 1);
		maxBudgetField = addNewButtonToCtrlPanel("500", "Maximum Annual Budget ($M)", 1, 6, 2);
	}
	
	/**
	 * Sets up the optimization components on the bottom left of the param panel
	 * Adds a button for optimization and the listener.
	 * Adds button so user can select what to optimize: Savings, ROI, IRR, etc.
	 */
	@Override
	protected void createOptimizationComponents() {
		super.createOptimizationComponents();
		
		rdbtnProfit = addOptimizationTypeButton("Savings", 1, 4);
		rdbtnROI = addOptimizationTypeButton("ROI", 3, 4);
		rdbtnIRR = addOptimizationTypeButton("IRR", 5, 4);

		ButtonGroup btnGrp = new ButtonGroup();
		btnGrp.add(rdbtnProfit);
		btnGrp.add(rdbtnROI);
		btnGrp.add(rdbtnIRR);
		btnGrp.setSelected(rdbtnProfit.getModel(), true);
	}	
	
	/**
	 * Adds the SysOptBtnListener to the run optimization button.
	 * @param btnRunOptimization
	 */
	@Override
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
		SysOptBtnListener obl = new SysOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}

	/**
	 * Sets up the advanced panels on the right of the param panel
	 * 1) Advanced param panel: attrition rate, hiring rate, inflation rate,
	 * discount rate, experience level at year 0, experience level in future year,
	 * and number of starting point and whether to approach regionally or to ignore environment.
	 * 2) System select panel: select systems using checkboxes or manually.
	 * User can also limit data/blu through this.
	 * 3) Capability select panel: filter data/blu by 
	 * only considering those that pertain to desired capabilities.
	 * 4) Force mod/decom panel: force the algorithm to modernize or decomission specified systems.
	 */
	@Override
	protected void createAdvParamPanels() {
		super.createAdvParamPanels();

		attRateField = addNewButtonToAdvParamPanel("3", "Attrition Rate (%)", 1, 0, 1);
		hireRateField = addNewButtonToAdvParamPanel("3", "Hiring Rate (%)", 1, 0, 2);
		infRateField = addNewButtonToAdvParamPanel("1.5", "Inflation Rate (%)", 1, 0, 3);
		disRateField = addNewButtonToAdvParamPanel("2.5", "Discount Rate (%)", 1, 0, 4);
		iniLearningCnstField = addNewButtonToAdvParamPanel("0", "Experience Level (%) at year 0", 3, 2, 1);
		scdLearningCnstField = addNewButtonToAdvParamPanel("0.9", "Experience Level (%) at year", 2, 2, 2);
		startingPtsField = addNewButtonToAdvParamPanel("5", "Number of Starting Points", 3, 2, 3);

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

		includeRegionalizationCheckbox = new JCheckBox("Include Regionalization");
		GridBagConstraints gbc_includeRegionalizationCheckbox = new GridBagConstraints();
		gbc_includeRegionalizationCheckbox.gridwidth = 2;
		gbc_includeRegionalizationCheckbox.insets = new Insets(0, 0, 5, 20);
		gbc_includeRegionalizationCheckbox.gridx = 0;
		gbc_includeRegionalizationCheckbox.gridy = 5;
		advParamPanel.add(includeRegionalizationCheckbox, gbc_includeRegionalizationCheckbox);
		
		garrTheaterCheckbox = new JCheckBox("Ignore Theater/Garrison Tag");
		GridBagConstraints gbc_garrTheaterCheckbox = new GridBagConstraints();
		gbc_garrTheaterCheckbox.gridwidth = 3;
		gbc_garrTheaterCheckbox.insets = new Insets(0, 5, 5, 20);
		gbc_garrTheaterCheckbox.gridx = 2;
		gbc_garrTheaterCheckbox.gridy = 5;
		advParamPanel.add(garrTheaterCheckbox, gbc_garrTheaterCheckbox);
		
		systemDataBLUSelectPanel = new JPanel();
		systemDataBLUSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		
		GridBagConstraints gbc_systemDataBLUSelectPanel = new GridBagConstraints();
		gbc_systemDataBLUSelectPanel.gridheight = 6;
		gbc_systemDataBLUSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemDataBLUSelectPanel.gridx = 8;
		gbc_systemDataBLUSelectPanel.gridy = 0;
		ctlPanel.add(systemDataBLUSelectPanel, gbc_systemDataBLUSelectPanel);
		
		GridBagLayout gbl_systemDataBLUSelectPanel = new GridBagLayout();
		gbl_systemDataBLUSelectPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemDataBLUSelectPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemDataBLUSelectPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_systemDataBLUSelectPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		systemDataBLUSelectPanel.setLayout(gbl_systemDataBLUSelectPanel);
		
		systemSelectPanel = new DHMSMSystemSelectPanel(checkboxListUpdater);
		GridBagConstraints gbc_systemSelectPanel = new GridBagConstraints();
		gbc_systemSelectPanel.gridheight = 6;
		gbc_systemSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectPanel.gridx = 0;
		gbc_systemSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(systemSelectPanel, gbc_systemSelectPanel);
		
		capabilitySelectPanel = new DHMSMCapabilitySelectPanel(checkboxListUpdater);
		capabilitySelectPanel.setVisible(false);
		GridBagConstraints gbc_capabilitySelectPanel = new GridBagConstraints();
		gbc_capabilitySelectPanel.gridheight = 6;
		gbc_capabilitySelectPanel.fill = GridBagConstraints.BOTH;
		gbc_capabilitySelectPanel.gridx = 1;
		gbc_capabilitySelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(capabilitySelectPanel, gbc_capabilitySelectPanel);
		
		updateDataBLUPanelButton = new ToggleButton("View Data/BLU");
		updateDataBLUPanelButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateDataBLUPanelButton, ".toggleButton");
		
		GridBagConstraints gbc_updateDataBLUPanelButton = new GridBagConstraints();
		gbc_updateDataBLUPanelButton.anchor = GridBagConstraints.WEST;
		gbc_updateDataBLUPanelButton.gridheight = 2;
		gbc_updateDataBLUPanelButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateDataBLUPanelButton.gridx = 2;
		gbc_updateDataBLUPanelButton.gridy = 0;
		systemDataBLUSelectPanel.add(updateDataBLUPanelButton, gbc_updateDataBLUPanelButton);
		
		dataBLUSelectPanel = new DHMSMDataBLUSelectPanel();
		dataBLUSelectPanel.setVisible(false);
		GridBagConstraints gbc_dataBLUSelectPanel = new GridBagConstraints();
		gbc_dataBLUSelectPanel.gridheight = 6;
		gbc_dataBLUSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_dataBLUSelectPanel.gridx = 3;
		gbc_dataBLUSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(dataBLUSelectPanel, gbc_dataBLUSelectPanel);
		dataBLUSelectPanel.addElements(systemSelectPanel);
		
		// instantiate systemModDecomSelectPanel, systemModernizePanel, systemDecomissionPanel
		systemModDecomSelectPanel = new JPanel();
		systemModDecomSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		systemModDecomSelectPanel.setVisible(false);
		
		GridBagConstraints gbc_systemModDecomSelectPanel = new GridBagConstraints();
		gbc_systemModDecomSelectPanel.gridheight = 6;
		gbc_systemModDecomSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemModDecomSelectPanel.gridx = 8;
		gbc_systemModDecomSelectPanel.gridy = 0;
		ctlPanel.add(systemModDecomSelectPanel, gbc_systemModDecomSelectPanel);
		
		GridBagLayout gbl_systemModDecomSelectPanel = new GridBagLayout();
		gbl_systemModDecomSelectPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemModDecomSelectPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemModDecomSelectPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_systemModDecomSelectPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		systemModDecomSelectPanel.setLayout(gbl_systemModDecomSelectPanel);
		
		systemModernizePanel = new DHMSMSystemSelectPanel("Select Systems that MUST be modernized:",true,checkboxListUpdater);
		GridBagConstraints gbc_systemModernizePanel = new GridBagConstraints();
		gbc_systemModernizePanel.gridheight = 6;
		gbc_systemModernizePanel.fill = GridBagConstraints.BOTH;
		gbc_systemModernizePanel.gridx = 0;
		gbc_systemModernizePanel.gridy = 0;
		systemModDecomSelectPanel.add(systemModernizePanel, gbc_systemModernizePanel);
		
		systemDecomissionPanel = new DHMSMSystemSelectPanel("Select Systems that MUST be decommissioned:",true,checkboxListUpdater);
		GridBagConstraints gbc_systemDecomissionPanel = new GridBagConstraints();
		gbc_systemDecomissionPanel.gridheight = 6;
		gbc_systemDecomissionPanel.fill = GridBagConstraints.BOTH;
		gbc_systemDecomissionPanel.gridx = 1;
		gbc_systemDecomissionPanel.gridy = 0;
		systemModDecomSelectPanel.add(systemDecomissionPanel, gbc_systemDecomissionPanel);		
	}
	
	/**
	 * Sets up the toggles to switch between advanced parameter panels:
	 * Advanced param panel, System select panel, Capability select panel,
	 * and Force mod/decom panel.
	 */
	@Override
	protected void createAdvParamPanelsToggles() {
		super.createAdvParamPanelsToggles();
		
		GridBagConstraints gbc_showParamBtn = new GridBagConstraints();
		gbc_showParamBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showParamBtn.gridwidth = 2;
		gbc_showParamBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showParamBtn.gridx = 6;
		gbc_showParamBtn.gridy = 6;
		ctlPanel.add(showParamBtn, gbc_showParamBtn);
		
		showSystemSelectBtn = new ToggleButton("Select System Functionality");
		showSystemSelectBtn.setName("showSystemSelectBtn");
		showSystemSelectBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemSelectBtn, ".toggleButton");
		
		GridBagConstraints gbc_showSystemSelectBtn = new GridBagConstraints();
		gbc_showSystemSelectBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showSystemSelectBtn.gridwidth = 2;
		gbc_showSystemSelectBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemSelectBtn.gridx = 6;
		gbc_showSystemSelectBtn.gridy = 3;
		ctlPanel.add(showSystemSelectBtn, gbc_showSystemSelectBtn);
		
		showSystemCapSelectBtn = new ToggleButton("Select Capability Functionality");
		showSystemCapSelectBtn.setName("showSystemCapSelectBtn");
		showSystemCapSelectBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemCapSelectBtn, ".toggleButton");
		
		GridBagConstraints gbc_showSystemCapSelectBtn = new GridBagConstraints();
		gbc_showSystemCapSelectBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showSystemCapSelectBtn.gridwidth = 2;
		gbc_showSystemCapSelectBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemCapSelectBtn.gridx = 6;
		gbc_showSystemCapSelectBtn.gridy = 4;
		ctlPanel.add(showSystemCapSelectBtn, gbc_showSystemCapSelectBtn);
		
		showSystemModDecomBtn = new ToggleButton("Manually Set Systems");
		showSystemModDecomBtn.setName("showSystemModDecomBtn");
		showSystemModDecomBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemModDecomBtn, ".toggleButton");
		
		GridBagConstraints gbc_showSystemModDecomBtn = new GridBagConstraints();
		gbc_showSystemModDecomBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showSystemModDecomBtn.gridwidth = 2;
		gbc_showSystemModDecomBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemModDecomBtn.gridx = 6;
		gbc_showSystemModDecomBtn.gridy = 5;
		ctlPanel.add(showSystemModDecomBtn, gbc_showSystemModDecomBtn);
	}
	
	/**
	 * Sets up the listeners to switch between advanced param panels
	 * Extensions include the logic for switching the panels.
	 */
	@Override
	protected void createAdvParamPanelsToggleListeners() {
		super.createAdvParamPanelsToggleListeners();
		
		showParamBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				if(showParamBtn.isSelected()) {
					showSystemSelectBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					systemDataBLUSelectPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(false);
					advParamPanel.setVisible(true);
				} else
					advParamPanel.setVisible(false);
			}
		});
		
		showSystemSelectBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				if(showSystemSelectBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					advParamPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(false);
					systemDataBLUSelectPanel.setVisible(true);
					capabilitySelectPanel.setVisible(false);
					capabilitySelectPanel.clearList();
					//if data or BLU was selected, show it. otherwise hide panel
					if(dataBLUSelectPanel.noneSelected()) {
						updateDataBLUPanelButton.setSelected(false);
						dataBLUSelectPanel.setVisible(false);
					} else {
						updateDataBLUPanelButton.setSelected(true);
						dataBLUSelectPanel.setVisible(true);
						dataBLUSelectPanel.setFromSystem(true);
					}
				} else {
					systemDataBLUSelectPanel.setVisible(false);
				}
			}
		});
		
		showSystemCapSelectBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e) {

				if(showSystemCapSelectBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					advParamPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(false);
					systemDataBLUSelectPanel.setVisible(true);
					capabilitySelectPanel.setVisible(true);
					//if data or BLU was selected, show it. otherwise hide panel
					if(dataBLUSelectPanel.noneSelected()) {
						updateDataBLUPanelButton.setSelected(false);
						dataBLUSelectPanel.setVisible(false);
					} else {
						updateDataBLUPanelButton.setSelected(true);
						dataBLUSelectPanel.setVisible(true);
						dataBLUSelectPanel.setFromSystem(false);					}
				} else {
					systemDataBLUSelectPanel.setVisible(false);
				}
			}
		});
		
		showSystemModDecomBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e) {
				if(showSystemModDecomBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemSelectBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
					advParamPanel.setVisible(false);
					systemDataBLUSelectPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(true);
				} else {
					systemModDecomSelectPanel.setVisible(false);
				}
			}
		});
		ActionListener viewDataBLUPanelListener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				// if the updateDataBLUPanelButton is unselected by user, hide the panel
				if (!updateDataBLUPanelButton.isSelected()) {
					dataBLUSelectPanel.setVisible(false);
					dataBLUSelectPanel.clearList();
				}
				// otherwise, if the updateDataBLUPanelButton is selected or the user clicks to update the list
				else {
					dataBLUSelectPanel.setVisible(true);
					if (showSystemSelectBtn.isSelected())
						dataBLUSelectPanel.setFromSystem(true);
					else
						dataBLUSelectPanel.setFromSystem(false);
				}
			}
		};
		updateDataBLUPanelButton.addActionListener(viewDataBLUPanelListener);
		
		UpdateDataBLUListListener updateDataBLUListener = new UpdateDataBLUListListener();
		updateDataBLUListener.setEngine(engine);
		updateDataBLUListener.setUpDHMSMHelper();
		updateDataBLUListener.setComponents(systemSelectPanel, capabilitySelectPanel, dataBLUSelectPanel, showSystemSelectBtn);
		updateDataBLUListener.setUpdateButtons(dataBLUSelectPanel.updateProvideDataBLUButton, dataBLUSelectPanel.updateConsumeDataBLUButton,
				dataBLUSelectPanel.updateComplementDataBLUButton);
		dataBLUSelectPanel.updateProvideDataBLUButton.addActionListener(updateDataBLUListener);
		dataBLUSelectPanel.updateConsumeDataBLUButton.addActionListener(updateDataBLUListener);
		dataBLUSelectPanel.updateComplementDataBLUButton.addActionListener(updateDataBLUListener);
	}	
	
	/**
	 * Sets up the display panel at the bottom of the split pane.
	 * 1) overallAlysPanel shows high level metric labels and the graphs. This includes:
	 * 		a) solution label depicting success of algorithm,
	 *		b) total savings, number of years for transition, total roi, breakeven point,
	 * 		internal rate of return, and annual budget
	 * 		c) graphs
	 * 2) specificSysAlysPanel shows system speciffic details
	 * 3) currentFuncPanel shows current data/blu provided by each system
	 * 4) futureFuncPanel shows future data/blu provided by each system sustained
	 * 5) replacementHeatMapPanel shows whicch systems overlap with each functionality
	 * 6) capability comparison panel
	 * 7) geospatial map panel
	 */
	@Override
	protected void createDisplayPanel() {

		super.createDisplayPanel();

		solutionLbl = new JLabel("");
		solutionLbl.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_solutionLbl = new GridBagConstraints();
		gbc_solutionLbl.insets = new Insets(0, 0, 5, 5);
		gbc_solutionLbl.gridwidth = 5;
		gbc_solutionLbl.gridx = 0;
		gbc_solutionLbl.gridy = 0;
		overallAlysMetricsPanel.add(solutionLbl, gbc_solutionLbl);
		
		savingLbl = addNewLabelToOverviewPanel("Total transition savings over time horizon:", 0, 1);
		timeTransitionLbl = addNewLabelToOverviewPanel("Number of Years for Transition:", 0, 2);
		roiLbl = addNewLabelToOverviewPanel("Total ROI over time horizon:", 2, 1);
		bkevenLbl = addNewLabelToOverviewPanel("Breakeven point during time horizon:", 2, 2);
		irrLbl = addNewLabelToOverviewPanel("Internal Rate of Return:", 4, 1);
		annualBudgetLbl = addNewLabelToOverviewPanel("Annual Budget During Transition:", 4, 2);

		tab3 = addNewChartToOverviewPanel(0, 2);
		tab4 = addNewChartToOverviewPanel(1, 2);
		tab5 = addNewChartToOverviewPanel(0, 3);
		tab6 = addNewChartToOverviewPanel(1, 3);
		
		specificSysAlysPanel = addNewDisplayPanel("System Analysis");
		currentFuncPanel = addNewDisplayPanel("As-Is Functionality");
		futureFuncPanel = addNewDisplayPanel("Future Functionality");
		
		replacementHeatMapPanel = addNewDisplayPanel("Replacement Heat Map");
				
		replacementHeatMap = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/heatmap.html");
		GridBagConstraints gbc_replacementHeatMap = new GridBagConstraints();
		gbc_replacementHeatMap.insets = new Insets(0, 0, 0, 5);
		gbc_replacementHeatMap.fill = GridBagConstraints.BOTH;
		gbc_replacementHeatMap.gridx = 0;
		gbc_replacementHeatMap.gridy = 0;
		replacementHeatMapPanel.add(replacementHeatMap, gbc_replacementHeatMap);
	
		createSysCapComparisonPanel();
	
		createGeoSpatialMapPanel();
	}
	
	/**
	 * Adds a tab in the display panel for a capability system comparison
	 */
	private void createSysCapComparisonPanel() {
		JPanel sysCapPanel = addNewDisplayPanel("Capability System Comparison");
		
		sysCapOptionsPanel = new JPanel();
		sysCapOptionsPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_sysCapOptionsPanel = new GridBagConstraints();
		gbc_sysCapOptionsPanel.anchor = GridBagConstraints.NORTH;
		gbc_sysCapOptionsPanel.gridwidth = 1;
		gbc_sysCapOptionsPanel.insets = new Insets(0, 0, 5, 5);
		gbc_sysCapOptionsPanel.gridx = 0;
		gbc_sysCapOptionsPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysCapOptionsPanel.gridy = 0;
		sysCapPanel.add(sysCapOptionsPanel, gbc_sysCapOptionsPanel);
		
		sysCapDisplayPanel = new JPanel();
		sysCapDisplayPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_sysCapDisplayPanel = new GridBagConstraints();
		gbc_sysCapDisplayPanel.anchor = GridBagConstraints.SOUTH;
		gbc_sysCapDisplayPanel.gridwidth = 1;
		gbc_sysCapDisplayPanel.insets = new Insets(0, 0, 5, 5);
		gbc_sysCapDisplayPanel.gridx = 0;
		gbc_sysCapDisplayPanel.fill = GridBagConstraints.BOTH;
		gbc_sysCapDisplayPanel.gridy = 1;
		sysCapPanel.add(sysCapDisplayPanel, gbc_sysCapDisplayPanel);
		
		JLabel capLabel = new JLabel("Capability:");
		GridBagConstraints gbc_capLabel = new GridBagConstraints();
		gbc_capLabel.anchor = GridBagConstraints.WEST;
		gbc_capLabel.insets = new Insets(0, 0, 5, 5);
		gbc_capLabel.gridx = 0;
		gbc_capLabel.gridy = 0;
		sysCapOptionsPanel.add(capLabel, gbc_capLabel);
		
		capComboBox = new JComboBox<String>();
		GridBagConstraints gbc_capComboBox = new GridBagConstraints();
		gbc_capComboBox.gridwidth = 2;
		gbc_capComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_capComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_capComboBox.gridx = 1;
		gbc_capComboBox.gridy = 0;
		sysCapOptionsPanel.add(capComboBox, gbc_capComboBox);
		
		JLabel sysLabel = new JLabel("System:");
		GridBagConstraints gbc_sysLabel = new GridBagConstraints();
		gbc_sysLabel.anchor = GridBagConstraints.WEST;
		gbc_sysLabel.insets = new Insets(0, 0, 5, 5);
		gbc_sysLabel.gridx = 0;
		gbc_sysLabel.gridy = 1;
		sysCapOptionsPanel.add(sysLabel, gbc_sysLabel);
		
		sysComboBox = new JComboBox<String>();
		GridBagConstraints gbc_sysComboBox = new GridBagConstraints();
		gbc_sysComboBox.gridwidth = 2;
		gbc_sysComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_sysComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysComboBox.gridx = 1;
		gbc_sysComboBox.gridy = 1;
		sysCapOptionsPanel.add(sysComboBox, gbc_sysComboBox);
		
		createSysCapButton = new CustomButton("Create");
		createSysCapButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_createSysCapButton = new GridBagConstraints();
		gbc_createSysCapButton.anchor = GridBagConstraints.WEST;
		gbc_createSysCapButton.insets = new Insets(0, 0, 5, 5);
		gbc_createSysCapButton.gridx = 0;
		gbc_createSysCapButton.gridy = 2;
		sysCapOptionsPanel.add(createSysCapButton, gbc_createSysCapButton);
		Style.registerTargetClassName(createSysCapButton, ".standardButton");
		
		JLabel noteLabel = new JLabel("* notifies systems that are inferred to be related to said capability");
		GridBagConstraints gbc_noteLabel = new GridBagConstraints();
		gbc_noteLabel.anchor = GridBagConstraints.SOUTH;
		gbc_noteLabel.insets = new Insets(0, 0, 5, 5);
		gbc_noteLabel.gridx = 0;
		gbc_noteLabel.gridy = 0;
		sysCapOptionsPanel.add(noteLabel, gbc_noteLabel);

	}

	/**
	 * Adds a tab in the display panel for a geospatial map
	 */
	private void createGeoSpatialMapPanel(){
		JPanel geoSpatialPanel = addNewDisplayPanel("GeoSpatial Map");
		
		geoSpatialOptionsPanel = new JPanel();
		geoSpatialOptionsPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_geoSpatialOptionsPanel = new GridBagConstraints();
		gbc_geoSpatialOptionsPanel.anchor = GridBagConstraints.NORTH;
		gbc_geoSpatialOptionsPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_geoSpatialOptionsPanel.insets = new Insets(0, 0, 5, 5);
		gbc_geoSpatialOptionsPanel.gridx = 0;
		gbc_geoSpatialOptionsPanel.gridy = 0;
		geoSpatialPanel.add(geoSpatialOptionsPanel, gbc_geoSpatialOptionsPanel);
		
		geoSpatialMap = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/worldmap.html");
		
		GridBagConstraints gbc_geoSpatialMap = new GridBagConstraints();
		gbc_geoSpatialMap.insets = new Insets(0, 0, 0, 5);
		gbc_geoSpatialMap.fill = GridBagConstraints.BOTH;
		gbc_geoSpatialMap.gridx = 0;
		gbc_geoSpatialMap.gridy = 1;
		geoSpatialPanel.add(geoSpatialMap, gbc_geoSpatialMap);
		
		JLabel capLabel2 = new JLabel("Capability:");
		GridBagConstraints gbc_capLabel2 = new GridBagConstraints();
		gbc_capLabel2.anchor = GridBagConstraints.WEST;
		gbc_capLabel2.insets = new Insets(0, 0, 5, 5);
		gbc_capLabel2.gridx = 0;
		gbc_capLabel2.gridy = 0;
		geoSpatialOptionsPanel.add(capLabel2, gbc_capLabel2);
		
		geoCapComboBox = new JComboBox<String>();
		GridBagConstraints gbc_geoCapComboBox = new GridBagConstraints();
		gbc_geoCapComboBox.gridwidth = 2;
		gbc_geoCapComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_geoCapComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_geoCapComboBox.gridx = 1;
		gbc_geoCapComboBox.gridy = 0;
		geoSpatialOptionsPanel.add(geoCapComboBox, gbc_geoCapComboBox);
		
		createGeoSpatialMapButton = new CustomButton("Create");
		createGeoSpatialMapButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_createGeoSpatialMapButton = new GridBagConstraints();
		gbc_createGeoSpatialMapButton.anchor = GridBagConstraints.WEST;
		gbc_createGeoSpatialMapButton.insets = new Insets(0, 0, 5, 5);
		gbc_createGeoSpatialMapButton.gridx = 0;
		gbc_createGeoSpatialMapButton.gridy = 1;
		geoSpatialOptionsPanel.add(createGeoSpatialMapButton, gbc_createGeoSpatialMapButton);
		Style.registerTargetClassName(createGeoSpatialMapButton, ".standardButton");
	}
	
	/**
	 * Sets graphs to be visible after algorithm is run.
	 * @param visible
	 */
	@Override
	public void setGraphsVisible(boolean visible) {
		super.setGraphsVisible(visible);
		tab3.setVisible(visible);
		tab4.setVisible(visible);
		tab5.setVisible(visible);
		tab6.setVisible(visible);
	}
	
	/**
	 * Clears panels within the playsheet.
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	@Override
	public void clearPanels() {
		super.clearPanels();
		specificSysAlysPanel.removeAll();
		currentFuncPanel.removeAll();
		futureFuncPanel.removeAll();
		replacementHeatMapPanel.removeAll();
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations.
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	@Override
	public void clearLabels() {
		super.clearLabels();
		bkevenLbl.setText("N/A");
        savingLbl.setText("$0");
		roiLbl.setText("N/A");
		irrLbl.setText("N/A");
		annualBudgetLbl.setText("$0");
		timeTransitionLbl.setText("N/A");
	}
	
}
