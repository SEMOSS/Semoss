/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JToggleButton;
import javax.swing.ListSelectionModel;
import javax.swing.border.BevelBorder;

import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.AdvParamListener;
import prerna.ui.main.listener.specific.tap.OptFunctionRadioBtnListener;
import prerna.ui.main.listener.specific.tap.SerOptBtnListener;
import prerna.ui.main.listener.specific.tap.SysSpecComboBoxListener;
import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.Constants;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class SerOptPlaySheet extends InputPanelPlaySheet{
	
	//param panel components
	public JLabel lblSoaSustainmentCost;
	public JTextField yearField, icdSusField, mtnPctgField;
	public JTextField minBudgetField, maxBudgetField, hourlyRateField;
	public JRadioButton rdbtnBreakeven, rdbtnProfit, rdbtnROI;
	
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

	public void createOptimizationTypeComponents()
	{
		rdbtnProfit = new JRadioButton("Savings");
		rdbtnProfit.setName("rdbtnProfit");
		rdbtnProfit.setFont(new Font("Tahoma", Font.PLAIN, 12));
		rdbtnProfit.setSelected(true);
		GridBagConstraints gbc_rdbtnProfit = new GridBagConstraints();
		gbc_rdbtnProfit.gridwidth = 2;
		gbc_rdbtnProfit.anchor = GridBagConstraints.WEST;
		gbc_rdbtnProfit.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnProfit.gridx = 1;
		gbc_rdbtnProfit.gridy = 4;
		ctlPanel.add(rdbtnProfit, gbc_rdbtnProfit);


		OptFunctionRadioBtnListener opl = new OptFunctionRadioBtnListener();
		rdbtnROI = new JRadioButton("ROI");
		rdbtnROI.setName("rdbtnROI");
		rdbtnROI.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnRoi = new GridBagConstraints();
		gbc_rdbtnRoi.anchor = GridBagConstraints.WEST;
		gbc_rdbtnRoi.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnRoi.gridx = 3;
		gbc_rdbtnRoi.gridy = 4;
		ctlPanel.add(rdbtnROI, gbc_rdbtnRoi);
		rdbtnROI.addActionListener(opl);


		rdbtnBreakeven = new JRadioButton("Recoup Period");
		rdbtnBreakeven.setName("rdbtnBreakeven");
		rdbtnBreakeven.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnBreakeven = new GridBagConstraints();
		gbc_rdbtnBreakeven.gridwidth = 2;
		gbc_rdbtnBreakeven.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnBreakeven.anchor = GridBagConstraints.WEST;
		gbc_rdbtnBreakeven.gridx = 4;
		gbc_rdbtnBreakeven.gridy = 4;
		ctlPanel.add(rdbtnBreakeven, gbc_rdbtnBreakeven);
		rdbtnProfit.addActionListener(opl);
		rdbtnBreakeven.addActionListener(opl);
		opl.setSerOptRadioBtn(rdbtnProfit, rdbtnROI, rdbtnBreakeven);
		

	}

	public void createSpecificParamComponents()
	{
		icdSusField = new JTextField();
		icdSusField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		icdSusField.setText(".1");
		icdSusField.setColumns(4);
		GridBagConstraints gbc_icdSusField = new GridBagConstraints();
		gbc_icdSusField.anchor = GridBagConstraints.NORTHWEST;
		gbc_icdSusField.insets = new Insets(0, 0, 5, 5);
		gbc_icdSusField.gridx = 1;
		gbc_icdSusField.gridy = 3;
		ctlPanel.add(icdSusField, gbc_icdSusField);

		JLabel lblInterfaceSustainmentCostyear = new JLabel("Annual Interface Sustainment Cost ($M)");
		lblInterfaceSustainmentCostyear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblInterfaceSustainmentCostyear = new GridBagConstraints();
		gbc_lblInterfaceSustainmentCostyear.insets = new Insets(0, 0, 5, 5);
		gbc_lblInterfaceSustainmentCostyear.anchor = GridBagConstraints.WEST;
		gbc_lblInterfaceSustainmentCostyear.gridwidth = 4;
		gbc_lblInterfaceSustainmentCostyear.gridx = 2;
		gbc_lblInterfaceSustainmentCostyear.gridy = 3;
		ctlPanel.add(lblInterfaceSustainmentCostyear, gbc_lblInterfaceSustainmentCostyear);
		
		GridBagConstraints gbc_hourlyRateField = new GridBagConstraints();
		gbc_hourlyRateField.anchor = GridBagConstraints.WEST;
		gbc_hourlyRateField.insets = new Insets(0, 0, 5, 5);
		gbc_hourlyRateField.gridx = 6;
		gbc_hourlyRateField.gridy = 3;
		ctlPanel.add(hourlyRateField, gbc_hourlyRateField);

		JLabel lblHourlyRate = new JLabel("Hourly Build Cost Rate ($)");
		lblHourlyRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblHourlyRate = new GridBagConstraints();
		gbc_lblHourlyRate.anchor = GridBagConstraints.WEST;
		gbc_lblHourlyRate.gridwidth = 2;
		gbc_lblHourlyRate.insets = new Insets(0, 0, 5, 0);
		gbc_lblHourlyRate.gridx = 7;
		gbc_lblHourlyRate.gridy = 3;
		ctlPanel.add(lblHourlyRate, gbc_lblHourlyRate);
		
		sysSpecComboBox = new JComboBox<String>();
		DefaultComboBoxModel<String> sysSpecComboBoxModel = new DefaultComboBoxModel<String>(new String[] {"Choose Optimization Option:", "Enterprise(Default)", "System Specific"});
		sysSpecComboBox.setModel(sysSpecComboBoxModel);
		GridBagConstraints gbc_sysSpecComboBox = new GridBagConstraints();
		gbc_sysSpecComboBox.gridwidth = 4;
		gbc_sysSpecComboBox.insets = new Insets(0, 0, 0, 5);
		gbc_sysSpecComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysSpecComboBox.gridx = 0;
		gbc_sysSpecComboBox.gridy = 5;
		advParamPanel.add(sysSpecComboBox, gbc_sysSpecComboBox);

//		String [] fetching = {"Fetching"};
		EntityFiller filler = new EntityFiller();
		filler.engineName = "TAP_Core_Data";
		filler.type = "System";
		filler.setExternalQuery("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/System> ;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;} BIND(<http://health.mil/ontologies/Concept/SystemOwner/Central> AS ?central){?entity ?OwnedBy ?central}}");
		filler.run();
		Vector<String> names = filler.nameVector;
		String[] listArray=new String[names.size()];
		for (int i = 0;i<names.size();i++)
		{
			listArray[i]=(String) names.get(i);
		}
		sysSelect = new ButtonMenuDropDown("Select Systems");
		sysSelect.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		SysSpecComboBoxListener sscb = new SysSpecComboBoxListener();
		sscb.setShowItem(sysSelect);
		sysSpecComboBox.addActionListener(sscb);
		sysSelect.setVisible(false);
		GridBagConstraints gbc_sysSelect = new GridBagConstraints();
		gbc_sysSelect.anchor = GridBagConstraints.WEST;
		gbc_sysSelect.gridwidth = 2;
		gbc_sysSelect.gridx = 4;
		gbc_sysSelect.gridy = 5;
		advParamPanel.add(sysSelect, gbc_sysSelect);
		sysSelect.setupButton(listArray);
		final JPopupMenu popupMenu = sysSelect.popupMenu;

		final JComponent contentPane = (JComponent) this.getContentPane();
		contentPane.addMouseListener(new MouseAdapter() {  

			@Override  
			public void mouseClicked(MouseEvent e) {  
				maybeShowPopup(e);  
			}  

			@Override  
			public void mousePressed(MouseEvent e) {  
				maybeShowPopup(e);  
			}  

			@Override  
			public void mouseReleased(MouseEvent e) {  
				maybeShowPopup(e);  
			}  

			private void maybeShowPopup(MouseEvent e) {  
				if (e.isPopupTrigger()) {  
					popupMenu.show(contentPane, e.getX(), e.getY());  
				}  
			}  
		}); 	

	}
	public void createAdvParamPanels()
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
	public void createAdvParamPanelsToggles()
	{
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
	
	public void createAdvParamPanelsToggleListeners()
	{

		AdvParamListener saLis = new AdvParamListener();
		saLis.setPlaySheet(this);
		saLis.setParamButton(showParamBtn);
		showParamBtn.addActionListener(saLis);
	}
	
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

		createOptimizationTypeComponents();

		createAdvParamPanels();
		
		createAdvParamPanelsToggles();
		
		createAdvParamPanelsToggleListeners();
		
		createSpecificParamComponents();
	}
	
	public void addOptimizationBtnListener(JButton btnRunOptimization)
	{
		SerOptBtnListener obl = new SerOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	
	public void createSpecificDisplayComponents()
	{
		//first tab: overall systems with charts
		//top panel that has labels

		lblInvestmentRecoupTime = new JLabel("Investment Recoup Time:");
		GridBagConstraints gbc_lblInvestmentRecoupTime = new GridBagConstraints();
		gbc_lblInvestmentRecoupTime.insets = new Insets(0, 0, 5, 5);
		gbc_lblInvestmentRecoupTime.gridx = 4;
		gbc_lblInvestmentRecoupTime.gridy = 1;
		panel_1.add(lblInvestmentRecoupTime, gbc_lblInvestmentRecoupTime);

		recoupLbl = new JLabel("");
		GridBagConstraints gbc_recoupLbl = new GridBagConstraints();
		gbc_recoupLbl.insets = new Insets(0, 0, 5, 5);
		gbc_recoupLbl.gridx = 5;
		gbc_recoupLbl.gridy = 1;
		panel_1.add(recoupLbl, gbc_recoupLbl);

		JLabel label_1 = new JLabel("Total SOA build cost over time horizon:\r\n");
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.anchor = GridBagConstraints.WEST;
		gbc_label_1.insets = new Insets(0, 0, 0, 5);
		gbc_label_1.gridx = 0;
		gbc_label_1.gridy = 2;
		panel_1.add(label_1, gbc_label_1);

		costLbl = new JLabel("");
		GridBagConstraints gbc_costLbl = new GridBagConstraints();
		gbc_costLbl.anchor = GridBagConstraints.WEST;
		gbc_costLbl.insets = new Insets(0, 0, 0, 5);
		gbc_costLbl.gridx = 1;
		gbc_costLbl.gridy = 2;
		panel_1.add(costLbl, gbc_costLbl);

		//charts for first tab
		tab1 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab1.setPreferredSize(new Dimension(500, 400));
		tab1.setMinimumSize(new Dimension(500, 400));
		tab1.setVisible(false);
		tab2 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab2.setPreferredSize(new Dimension(500, 400));
		tab2.setMinimumSize(new Dimension(500, 400));
		tab2.setVisible(false);


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



		specificAlysPanel = new JPanel();
		tabbedPane.addTab("Service Analysis", null, specificAlysPanel, null);
		GridBagLayout gbl_specificAlysPanel = new GridBagLayout();
		gbl_specificAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		specificAlysPanel.setLayout(gbl_specificAlysPanel);

		specificSysAlysPanel = new JPanel();
		tabbedPane.addTab("System Analysis", null, specificSysAlysPanel, null);
		GridBagLayout gbl_specificSysAlysPanel = new GridBagLayout();
		gbl_specificSysAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificSysAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificSysAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificSysAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		specificSysAlysPanel.setLayout(gbl_specificSysAlysPanel);

		playSheetPanel = new JPanel();
		tabbedPane.addTab("Graph Representation", null, playSheetPanel,null);

		String helpNotesData = "";
		BufferedReader releaseNotesTextReader = null;
		try{
			//Here we read the help text file
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String  releaseNotesTextFile= workingDir + "/help/optimizationHelp.txt";
			FileReader fr = new FileReader(releaseNotesTextFile);
			releaseNotesTextReader = new BufferedReader(fr);

			helpNotesData = "<html>";
			String line = null;
			while ((line = releaseNotesTextReader.readLine()) != null)
			{
				helpNotesData = helpNotesData + line +"<br>";
			}
			helpNotesData = helpNotesData + "</body></html>";
		} catch(IOException e){
			helpNotesData = "File Load Error";
		}finally{
			try{
				if(releaseNotesTextReader!=null)
					releaseNotesTextReader.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		JPanel optimizationHelpPanel = new JPanel();
		tabbedPane.addTab("Help", null, optimizationHelpPanel, null);
		GridBagLayout gbl_optimizationHelpPanel = new GridBagLayout();
		gbl_optimizationHelpPanel.columnWidths = new int[]{0, 0};
		gbl_optimizationHelpPanel.rowHeights = new int[]{0, 0};
		gbl_optimizationHelpPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_optimizationHelpPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		optimizationHelpPanel.setLayout(gbl_optimizationHelpPanel);

		JScrollPane optimizationHelpScrollPane = new JScrollPane();
		GridBagConstraints gbc_optimizationHelpScrollPane1 = new GridBagConstraints();
		gbc_optimizationHelpScrollPane1.fill = GridBagConstraints.BOTH;
		gbc_optimizationHelpScrollPane1.gridx = 0;
		gbc_optimizationHelpScrollPane1.gridy = 0;
		optimizationHelpPanel.add(optimizationHelpScrollPane, gbc_optimizationHelpScrollPane1);
		helpTextArea = new JTextPane();
		helpTextArea.setEditable(false);
		optimizationHelpScrollPane.setViewportView(helpTextArea);
		helpTextArea.setContentType("text/html");
		helpTextArea.setText(helpNotesData);

	}
	
	public void createGenericDisplayPanel()
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
		
		super.createGenericDisplayPanel();

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

		createSpecificDisplayComponents();
	}
	
	public void setGraphsVisible(boolean visible) {
		tab1.setVisible(visible);
		tab2.setVisible(visible);
		tab3.setVisible(visible);
		tab4.setVisible(visible);
		tab5.setVisible(visible);
		tab6.setVisible(visible);
		timeline.setVisible(visible);
	}
	
	/**
	 * Clears panels within the playsheet
	 */
	public void clearPanels() {
		specificAlysPanel.removeAll();
		specificSysAlysPanel.removeAll();
		playSheetPanel.removeAll();
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations. Allows for different TAP algorithms to be run as empty functions.
	 */
	public void clearLabels()
	{
		bkevenLbl.setText("N/A");
        savingLbl.setText("$0");
		roiLbl.setText("N/A");
		recoupLbl.setText("N/A");
		costLbl.setText("$0");
	}
}
