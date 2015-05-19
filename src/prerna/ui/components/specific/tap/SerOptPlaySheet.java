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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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
import javax.swing.ListSelectionModel;

import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.OptFunctionRadioBtnListener;
import prerna.ui.main.listener.specific.tap.SerOptBtnListener;
import prerna.ui.main.listener.specific.tap.SysSpecComboBoxListener;
import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class SerOptPlaySheet extends OptPlaySheet{

	//input panel- optimization types
	public JRadioButton rdbtnBreakeven, rdbtnProfit, rdbtnROI;
	
	//overview tab components
	public JLabel costLbl, recoupLbl;
	public BrowserGraphPanel tab1, tab2;
	
	//timeline tab
	public JPanel timelinePanel;
	public BrowserGraphPanel timeline;
	
	//other tabs
	public JPanel specificAlysPanel = new JPanel();
	public JPanel playSheetPanel = new JPanel();
	public JTextPane helpTextArea;
	
	
	@Override
	protected void createBasicParamComponents() {
		
		super.createBasicParamComponents();
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
		


	}
	
	@Override
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
		SerOptBtnListener obl = new SerOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	@Override
	protected void createAdvParamPanels() {
		super.createAdvParamPanels();
		
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
	
	@Override
	protected void createAdvParamPanelsToggleListeners() {
		
		showParamBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e) {

				if(showParamBtn.isSelected())
					advParamPanel.setVisible(true);
				else
					advParamPanel.setVisible(false);
			}
		});
	}
	
	@Override
	protected void createOptimizationTypeComponents()
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

		rdbtnROI = new JRadioButton("ROI");
		rdbtnROI.setName("rdbtnROI");
		rdbtnROI.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnRoi = new GridBagConstraints();
		gbc_rdbtnRoi.anchor = GridBagConstraints.WEST;
		gbc_rdbtnRoi.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnRoi.gridx = 3;
		gbc_rdbtnRoi.gridy = 4;
		ctlPanel.add(rdbtnROI, gbc_rdbtnRoi);

		//sys opt has different TODO
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
		
		OptFunctionRadioBtnListener opl = new OptFunctionRadioBtnListener();
		rdbtnProfit.addActionListener(opl);
		rdbtnROI.addActionListener(opl);
		rdbtnBreakeven.addActionListener(opl);
		
		opl.setSerOptRadioBtn(rdbtnProfit, rdbtnROI, rdbtnBreakeven);
		

	}
	
	@Override
	protected void createDisplayPanel()
	{
		//first tab: overall systems with charts
		//top panel that has labels

		timeline = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/timeline.html");
		super.createDisplayPanel();

		JLabel lblInvestmentRecoupTime = new JLabel("Investment Recoup Time:");
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
	
	@Override
	public void setGraphsVisible(boolean visible) {
		super.setGraphsVisible(visible);
		tab1.setVisible(visible);
		tab2.setVisible(visible);
		timeline.setVisible(visible);
	}
	
	/**
	 * Clears panels within the playsheet
	 */
	@Override
	public void clearPanels() {
		super.clearPanels();
		specificAlysPanel.removeAll();
		playSheetPanel.removeAll();
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations. Allows for different TAP algorithms to be run as empty functions.
	 */
	@Override
	public void clearLabels() {
		super.clearLabels();
		recoupLbl.setText("N/A");
		costLbl.setText("$0");
	}
}
