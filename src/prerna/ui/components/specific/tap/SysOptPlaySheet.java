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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.ListSelectionModel;
import javax.swing.border.BevelBorder;

import aurelienribon.ui.css.Style;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.AdvParamListener;
import prerna.ui.main.listener.specific.tap.CapCheckBoxSelectorListener;
import prerna.ui.main.listener.specific.tap.CheckBoxSelectorListener;
import prerna.ui.main.listener.specific.tap.OptFunctionRadioBtnListener;
import prerna.ui.main.listener.specific.tap.SysOptBtnListener;
import prerna.ui.main.listener.specific.tap.UpdateDataBLUListListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.ui.swing.custom.ToggleButton;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
public class SysOptPlaySheet extends SerOptPlaySheet{

	public JCheckBox includeRegionalizationCheckbox;
	
	//select functionality panel and toggle button
	public JToggleButton showSystemSelectBtn, showSystemCapSelectBtn;
	public JPanel systemDataBLUSelectPanel;
	public DHMSMSystemSelectPanel systemSelectPanel;
	public JPanel capScrollPanel;
	public JLabel lblDataSelectHeader,lblBLUSelectHeader;
	
	//system, capability, data, and blu selects
	public JCheckBox allCapButton, dhmsmCapButton;
	public JCheckBox hsdCapButton, hssCapButton, fhpCapButton;
	public SelectScrollList capSelectDropDown,dataSelectDropDown,bluSelectDropDown;
	public JToggleButton updateDataBLUPanelButton;
	public JButton updateProvideDataBLUButton,updateConsumeDataBLUButton,updateComplementDataBLUButton;

	 //overall analysis tab
	public JRadioButton rdbtnIRR;
	public JLabel solutionLbl, irrLbl, annualBudgetLbl, timeTransitionLbl;
	public BrowserGraphPanel tabModernizedHeatMap;
	public JPanel specificFuncAlysPanel;
	
	/**
	 * Constructor for SysOptPlaySheet.
	 */
	public SysOptPlaySheet()
	{
		super();
	}
	public String[] makeListFromQuery(String type, String query)
	{
		EntityFiller filler = new EntityFiller();
		filler.engineName = engine.getEngineName();
		filler.type = "Capability";
		filler.setExternalQuery(query);
		filler.run();
		Vector names = filler.nameVector;
		String[] listArray=new String[names.size()];
		for (int i = 0;i<names.size();i++)
		{
			listArray[i]=(String) names.get(i);
		}
		return listArray;
	}
	@Override
	public void createAdvParamPanels()
	{
		super.createAdvParamPanels();
		
		includeRegionalizationCheckbox = new JCheckBox("Include Regionalization");
		GridBagConstraints gbc_includeRegionalizationCheckbox = new GridBagConstraints();
		gbc_includeRegionalizationCheckbox.gridwidth = 3;
		gbc_includeRegionalizationCheckbox.insets = new Insets(0, 0, 5, 20);
		gbc_includeRegionalizationCheckbox.gridx = 2;
		gbc_includeRegionalizationCheckbox.gridy = 4;
		advParamPanel.add(includeRegionalizationCheckbox, gbc_includeRegionalizationCheckbox);
				
		systemDataBLUSelectPanel = new JPanel();
		systemDataBLUSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		systemDataBLUSelectPanel.setVisible(false);
		
		GridBagConstraints gbc_systemDataBLUSelectPanel = new GridBagConstraints();
		gbc_systemDataBLUSelectPanel.gridheight = 6;
		gbc_systemDataBLUSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemDataBLUSelectPanel.gridx = 8;
		gbc_systemDataBLUSelectPanel.gridy = 0;
		ctlPanel.add(systemDataBLUSelectPanel, gbc_systemDataBLUSelectPanel);
		
		GridBagLayout gbl_systemDataBLUSelectPanel = new GridBagLayout();
		gbl_systemDataBLUSelectPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_systemDataBLUSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemDataBLUSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemDataBLUSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		systemDataBLUSelectPanel.setLayout(gbl_systemDataBLUSelectPanel);
		
		systemSelectPanel = new DHMSMSystemSelectPanel();
		
		GridBagConstraints gbc_systemSelectPanel = new GridBagConstraints();
		gbc_systemSelectPanel.gridwidth = 4;
		gbc_systemSelectPanel.gridheight = 6;
		gbc_systemSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectPanel.gridx = 1;
		gbc_systemSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(systemSelectPanel, gbc_systemSelectPanel);
		systemSelectPanel.addElements();
		
		updateDataBLUPanelButton = new ToggleButton("View Data/BLU");
		updateDataBLUPanelButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateDataBLUPanelButton,  ".toggleButton");
		
		GridBagConstraints gbc_updateDataBLUPanelButton = new GridBagConstraints();
		gbc_updateDataBLUPanelButton.anchor = GridBagConstraints.WEST;
		gbc_updateDataBLUPanelButton.gridheight = 2;
		gbc_updateDataBLUPanelButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateDataBLUPanelButton.gridx = 8;
		gbc_updateDataBLUPanelButton.gridy = 0;
		systemDataBLUSelectPanel.add(updateDataBLUPanelButton, gbc_updateDataBLUPanelButton);
		
		updateProvideDataBLUButton = new CustomButton("Select Provide");
		updateProvideDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateProvideDataBLUButton,  ".toggleButton");
		updateProvideDataBLUButton.setVisible(false);		
		
		GridBagConstraints gbc_updateDataBLUButton = new GridBagConstraints();
		gbc_updateDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateDataBLUButton.gridheight = 2;
		gbc_updateDataBLUButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateDataBLUButton.gridx = 9;
		gbc_updateDataBLUButton.gridy = 0;
		systemDataBLUSelectPanel.add(updateProvideDataBLUButton, gbc_updateDataBLUButton);
		
		updateConsumeDataBLUButton = new CustomButton("Select Consume");
		updateConsumeDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateConsumeDataBLUButton,  ".toggleButton");
		updateConsumeDataBLUButton.setVisible(false);		
		
		GridBagConstraints gbc_updateConsumeDataBLUButton = new GridBagConstraints();
		gbc_updateConsumeDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateConsumeDataBLUButton.gridheight = 2;
		gbc_updateConsumeDataBLUButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateConsumeDataBLUButton.gridx = 10;
		gbc_updateConsumeDataBLUButton.gridy = 0;
		systemDataBLUSelectPanel.add(updateConsumeDataBLUButton, gbc_updateConsumeDataBLUButton);		
		
		updateComplementDataBLUButton = new CustomButton("Select Complement");
		updateComplementDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateComplementDataBLUButton,  ".toggleButton");
		updateComplementDataBLUButton.setVisible(false);		
		
		GridBagConstraints gbc_updateComplementDataBLUButton = new GridBagConstraints();
		gbc_updateComplementDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateComplementDataBLUButton.gridwidth = 3;
		gbc_updateComplementDataBLUButton.gridheight = 2;
		gbc_updateComplementDataBLUButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateComplementDataBLUButton.gridx = 11;
		gbc_updateComplementDataBLUButton.gridy = 0;
		systemDataBLUSelectPanel.add(updateComplementDataBLUButton, gbc_updateComplementDataBLUButton);

		lblDataSelectHeader = new JLabel("Select Data Objects:");
		lblDataSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblDataSelectHeader.setVisible(false);
		GridBagConstraints gbc_lblDataSelectHeader = new GridBagConstraints();
		gbc_lblDataSelectHeader.gridwidth = 3;
		gbc_lblDataSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblDataSelectHeader.insets = new Insets(0, 5, 5, 5);
		gbc_lblDataSelectHeader.gridx = 8;
		gbc_lblDataSelectHeader.gridy = 2;
		systemDataBLUSelectPanel.add(lblDataSelectHeader, gbc_lblDataSelectHeader);
		
		lblBLUSelectHeader = new JLabel("Select BLUs:");
		lblBLUSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblBLUSelectHeader.setVisible(false);
		GridBagConstraints gbc_lblBLUSelectHeader = new GridBagConstraints();
		gbc_lblBLUSelectHeader.gridwidth = 3;
		gbc_lblBLUSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblBLUSelectHeader.insets = new Insets(0, 0, 5, 5);
		gbc_lblBLUSelectHeader.gridx = 12;
		gbc_lblBLUSelectHeader.gridy = 2;
		systemDataBLUSelectPanel.add(lblBLUSelectHeader, gbc_lblBLUSelectHeader);
		
//		sysSelectDropDown = new SelectScrollList("Select Individual Systems");
//		sysSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
//		GridBagConstraints gbc_sysSelectDropDown = new GridBagConstraints();
//		gbc_sysSelectDropDown.gridwidth = 3;
//		gbc_sysSelectDropDown.insets = new Insets(0, 0, 0, 5);
//		gbc_sysSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
//		gbc_sysSelectDropDown.gridx = 0;
//		gbc_sysSelectDropDown.gridy = 3;
//		systemSelectPanel.add(sysSelectDropDown.pane, gbc_sysSelectDropDown);
		
//		//String[] sysArray = makeListFromQuery("System","SELECT DISTINCT ?entity WHERE {BIND(<http://health.mil/ontologies/Concept/TaskerLifecyclePhase/Submitted> AS ?TaskerStatus) {?SystemTasker <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemTasker>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Submits <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Receives-Submits> ;}  {?BeingIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BeingIn> ;}{?TaskerLifecycleSystemPhase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TaskerLifecycleSystemPhase>;}{?TypeOf2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;}{?TaskerStatus <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TaskerLifecyclePhase>;}{?entity ?Submits ?SystemTasker}{?SystemTasker ?BeingIn ?TaskerLifecycleSystemPhase}{?TaskerLifecycleSystemPhase ?TypeOf2 ?TaskerStatus} } ");
//		String[] sysArray = makeListFromQuery("System","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}} ");
//		sysSelectDropDown.setupButton(sysArray,40,120); //need to give list of all systems

//		CheckBoxSelectorListener sysCheckBoxListener = new CheckBoxSelectorListener();
//		sysCheckBoxListener.setEngine(engine);
//		sysCheckBoxListener.setScrollList(sysSelectDropDown);
//		sysCheckBoxListener.setCheckBox(allSysButton,recdSysButton, intDHMSMSysButton,notIntDHMSMSysButton,theaterSysButton,garrisonSysButton,lowProbButton, highProbButton);
//		allSysButton.addActionListener(sysCheckBoxListener);
//		recdSysButton.addActionListener(sysCheckBoxListener);
//		intDHMSMSysButton.addActionListener(sysCheckBoxListener);
//		notIntDHMSMSysButton.addActionListener(sysCheckBoxListener);
//		theaterSysButton.addActionListener(sysCheckBoxListener);
//		garrisonSysButton.addActionListener(sysCheckBoxListener);
//		lowProbButton.addActionListener(sysCheckBoxListener);
//		highProbButton.addActionListener(sysCheckBoxListener);
		
		
		capScrollPanel = new JPanel();
		capScrollPanel.setVisible(false);
		
		GridBagConstraints gbc_capScrollPanel = new GridBagConstraints();
		gbc_capScrollPanel.gridheight = 6;
		gbc_capScrollPanel.fill = GridBagConstraints.BOTH;
		gbc_capScrollPanel.gridx = 5;
		gbc_capScrollPanel.gridy = 0;
		systemDataBLUSelectPanel.add(capScrollPanel, gbc_capScrollPanel);
		
		GridBagLayout gbl_capScrollPanel = new GridBagLayout();
		gbl_capScrollPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_capScrollPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_capScrollPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_capScrollPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		capScrollPanel.setLayout(gbl_capScrollPanel);
		
		JLabel lblCapSelectHeader = new JLabel("Select Capabilities:");
		lblCapSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblCapSelectHeader = new GridBagConstraints();
		gbc_lblCapSelectHeader.gridwidth = 3;
		gbc_lblCapSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblCapSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblCapSelectHeader.gridx = 0;
		gbc_lblCapSelectHeader.gridy = 0;
		capScrollPanel.add(lblCapSelectHeader, gbc_lblCapSelectHeader);
		
		allCapButton = new JCheckBox("All Cap");
		GridBagConstraints gbc_allCapButton = new GridBagConstraints();
		gbc_allCapButton.anchor = GridBagConstraints.WEST;
		gbc_allCapButton.gridx = 0;
		gbc_allCapButton.gridy = 1;
		capScrollPanel.add(allCapButton, gbc_allCapButton);

		dhmsmCapButton = new JCheckBox("DHMSM");
		GridBagConstraints gbc_dhmsmCapButton = new GridBagConstraints();
		gbc_dhmsmCapButton.anchor = GridBagConstraints.WEST;
		gbc_dhmsmCapButton.gridx = 1;
		gbc_dhmsmCapButton.gridy = 1;
		capScrollPanel.add(dhmsmCapButton, gbc_dhmsmCapButton);

		hsdCapButton = new JCheckBox("HSD");
		GridBagConstraints gbc_hsdCapButton = new GridBagConstraints();
		gbc_hsdCapButton.anchor = GridBagConstraints.WEST;
		gbc_hsdCapButton.gridx = 0;
		gbc_hsdCapButton.gridy = 2;
		capScrollPanel.add(hsdCapButton, gbc_hsdCapButton);
		
		hssCapButton = new JCheckBox("HSS");
		GridBagConstraints gbc_hssCapButton = new GridBagConstraints();
		gbc_hssCapButton.anchor = GridBagConstraints.WEST;
		gbc_hssCapButton.gridx = 1;
		gbc_hssCapButton.gridy = 2;
		capScrollPanel.add(hssCapButton, gbc_hssCapButton);
		
		fhpCapButton = new JCheckBox("FHP");
		GridBagConstraints gbc_fhpCapButton = new GridBagConstraints();
		gbc_fhpCapButton.anchor = GridBagConstraints.WEST;
		gbc_fhpCapButton.gridx = 2;
		gbc_fhpCapButton.gridy = 2;
		capScrollPanel.add(fhpCapButton, gbc_fhpCapButton);
		
		capSelectDropDown = new SelectScrollList("Select Individual Capabilities");
		capSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_capSelectDropDown = new GridBagConstraints();
		gbc_capSelectDropDown.gridwidth = 3;
		gbc_capSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_capSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_capSelectDropDown.gridx = 0;
		gbc_capSelectDropDown.gridy = 3;
		capScrollPanel.add(capSelectDropDown.pane, gbc_capSelectDropDown);
		
		String[] capArray = makeListFromQuery("Capability","SELECT DISTINCT ?entity WHERE {{?CapabilityTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityTag>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/Capability> ;}{?CapabilityTag ?TaggedBy ?entity}}");
		capSelectDropDown.setupButton(capArray,40,120); //need to give list of all systems
		
		CapCheckBoxSelectorListener capCheckBoxListener = new CapCheckBoxSelectorListener();
		capCheckBoxListener.setEngine(engine);
		capCheckBoxListener.setScrollList(capSelectDropDown);
		capCheckBoxListener.setCheckBox(allCapButton,dhmsmCapButton, hsdCapButton,hssCapButton, fhpCapButton);
		allCapButton.addActionListener(capCheckBoxListener);
		dhmsmCapButton.addActionListener(capCheckBoxListener);
		hsdCapButton.addActionListener(capCheckBoxListener);
		hssCapButton.addActionListener(capCheckBoxListener);
		fhpCapButton.addActionListener(capCheckBoxListener);
		
		dataSelectDropDown = new SelectScrollList("Select Individual Data");
		dataSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_dataSelectDropDown = new GridBagConstraints();
		gbc_dataSelectDropDown.gridwidth = 3;
		gbc_dataSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_dataSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_dataSelectDropDown.gridx = 8;
		gbc_dataSelectDropDown.gridy = 3;
		systemDataBLUSelectPanel.add(dataSelectDropDown.pane, gbc_dataSelectDropDown);

		//String[] dataArray = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.} }");
		String[] dataArray = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}");
		dataSelectDropDown.setupButton(dataArray,40,120); //need to give list of all systems
		dataSelectDropDown.setVisible(false);
		
		bluSelectDropDown = new SelectScrollList("Select Individual BLU");
		bluSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_bluSelectDropDown = new GridBagConstraints();
		gbc_bluSelectDropDown.gridwidth = 3;
		gbc_bluSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_bluSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_bluSelectDropDown.gridx = 12;
		gbc_bluSelectDropDown.gridy = 3;
		systemDataBLUSelectPanel.add(bluSelectDropDown.pane, gbc_bluSelectDropDown);
		
		String[] bluArray = makeListFromQuery("BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}");
		bluSelectDropDown.setupButton(bluArray,40,120); //need to give list of all systems
		bluSelectDropDown.setVisible(false);

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
				}  
			}  
		});

	}
	@Override
	public void createAdvParamPanelsToggles()
	{
		super.createAdvParamPanelsToggles();

		GridBagConstraints gbc_showParamBtn = new GridBagConstraints();
		gbc_showParamBtn.anchor = GridBagConstraints.WEST;
		gbc_showParamBtn.gridwidth = 2;
		gbc_showParamBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showParamBtn.gridx = 6;
		gbc_showParamBtn.gridy = 5;
		ctlPanel.add(showParamBtn, gbc_showParamBtn);
		
		showSystemSelectBtn = new ToggleButton("Select System Functionality");
		showSystemSelectBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemSelectBtn,  ".toggleButton");
		
		GridBagConstraints gbc_showSystemSelectBtn = new GridBagConstraints();
		gbc_showSystemSelectBtn.anchor = GridBagConstraints.WEST;
		gbc_showSystemSelectBtn.gridwidth = 2;
		gbc_showSystemSelectBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemSelectBtn.gridx = 6;
		gbc_showSystemSelectBtn.gridy = 3;
		ctlPanel.add(showSystemSelectBtn, gbc_showSystemSelectBtn);
		
		showSystemCapSelectBtn = new ToggleButton("Select Capability Functionality");
		showSystemCapSelectBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemCapSelectBtn,  ".toggleButton");
		
		GridBagConstraints gbc_showSystemCapSelectBtn = new GridBagConstraints();
		gbc_showSystemCapSelectBtn.anchor = GridBagConstraints.WEST;
		gbc_showSystemCapSelectBtn.gridwidth = 2;
		gbc_showSystemCapSelectBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemCapSelectBtn.gridx = 6;
		gbc_showSystemCapSelectBtn.gridy = 4;
		ctlPanel.add(showSystemCapSelectBtn, gbc_showSystemCapSelectBtn);
		
		
	}
	@Override
	public void createAdvParamPanelsToggleListeners()
	{
		AdvParamListener saLis = new AdvParamListener();
		saLis.setPlaySheet(this);
		saLis.setParamButtons(showParamBtn,showSystemSelectBtn,showSystemCapSelectBtn);
		showParamBtn.addActionListener(saLis);
		showSystemSelectBtn.addActionListener(saLis);
		showSystemCapSelectBtn.addActionListener(saLis);
		
		UpdateDataBLUListListener updateDataBLUListener = new UpdateDataBLUListListener();
		updateDataBLUListener.setEngine(engine);
		updateDataBLUListener.setUpDHMSMHelper();
		updateDataBLUListener.setComponents(systemSelectPanel,capSelectDropDown,dataSelectDropDown,bluSelectDropDown,lblDataSelectHeader,lblBLUSelectHeader,showSystemSelectBtn);
		updateDataBLUListener.setUpdateButtons(updateDataBLUPanelButton,updateProvideDataBLUButton,updateConsumeDataBLUButton,updateComplementDataBLUButton);
		updateDataBLUPanelButton.addActionListener(updateDataBLUListener);
		updateProvideDataBLUButton.addActionListener(updateDataBLUListener);
		updateConsumeDataBLUButton.addActionListener(updateDataBLUListener);
		updateComplementDataBLUButton.addActionListener(updateDataBLUListener);
	}
	@Override
	public void createSpecificParamComponents()
	{
		lblSoaSustainmentCost.setText("Annual Maint Exposed Data (%)");
		maxBudgetField.setText("500");
		
		GridBagConstraints gbc_progressBar = new GridBagConstraints();
		gbc_progressBar.anchor = GridBagConstraints.SOUTH;
		gbc_progressBar.fill = GridBagConstraints.HORIZONTAL;
		gbc_progressBar.gridwidth = 2;
		gbc_progressBar.insets = new Insets(0, 0, 0, 5);
		gbc_progressBar.gridx = 6;
		gbc_progressBar.gridy = 0;
		ctlPanel.add(progressBar, gbc_progressBar);
		progressBar.setVisible(false);
		
		hourlyRateField.setColumns(4);
		GridBagConstraints gbc_hourlyRateField = new GridBagConstraints();
		gbc_hourlyRateField.anchor = GridBagConstraints.NORTHWEST;
		gbc_hourlyRateField.insets = new Insets(0, 0, 5, 5);
		gbc_hourlyRateField.gridx = 1;
		gbc_hourlyRateField.gridy = 3;
		ctlPanel.add(hourlyRateField, gbc_hourlyRateField);

		JLabel lblHourlyRate = new JLabel("Hourly Build Cost Rate ($)");
		lblHourlyRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblHourlyRate = new GridBagConstraints();
		gbc_lblHourlyRate.anchor = GridBagConstraints.WEST;
		gbc_lblHourlyRate.gridwidth = 4;
		gbc_lblHourlyRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblHourlyRate.gridx = 2;
		gbc_lblHourlyRate.gridy = 3;
		ctlPanel.add(lblHourlyRate, gbc_lblHourlyRate);

	}

	public void addOptimizationBtnListener(JButton btnRunOptimization)
	{
		SysOptBtnListener obl = new SysOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	@Override
	public void createSpecificDisplayComponents()
	{
		tabModernizedHeatMap = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/heatmap.html");
		tabModernizedHeatMap.setPreferredSize(new Dimension(500, 400));
		tabModernizedHeatMap.setMinimumSize(new Dimension(500, 400));
		tabModernizedHeatMap.setVisible(false);
		
		GridBagConstraints gbc_tabModernizedHeatMap = new GridBagConstraints();
		gbc_tabModernizedHeatMap.insets = new Insets(0, 0, 0, 5);
		gbc_tabModernizedHeatMap.fill = GridBagConstraints.BOTH;
		gbc_tabModernizedHeatMap.gridwidth = 2;
		gbc_tabModernizedHeatMap.gridx = 0;
		gbc_tabModernizedHeatMap.gridy = 1;
		chartPanel.add(tabModernizedHeatMap,  gbc_tabModernizedHeatMap);
		
		GridBagConstraints gbc_panel = new GridBagConstraints();
		gbc_panel.insets = new Insets(0, 0, 0, 5);
		gbc_panel.fill = GridBagConstraints.BOTH;
		gbc_panel.gridx = 0;
		gbc_panel.gridy = 2;
		chartPanel.add(tab3,  gbc_panel);
		
		GridBagConstraints gbc_panel2 = new GridBagConstraints();
		gbc_panel2.insets = new Insets(0, 0, 0, 5);
		gbc_panel2.fill = GridBagConstraints.BOTH;
		gbc_panel2.gridx = 1;
		gbc_panel2.gridy = 2;
		chartPanel.add(tab4,  gbc_panel2);

		GridBagConstraints gbc_panel3 = new GridBagConstraints();
		gbc_panel3.insets = new Insets(0, 0, 0, 5);
		gbc_panel3.fill = GridBagConstraints.BOTH;
		gbc_panel3.gridx = 0;
		gbc_panel3.gridy = 3;
		chartPanel.add(tab5,  gbc_panel3);

		GridBagConstraints gbc_panel4 = new GridBagConstraints();
		gbc_panel4.insets = new Insets(0, 0, 0, 5);
		gbc_panel4.fill = GridBagConstraints.BOTH;
		gbc_panel4.gridx = 1;
		gbc_panel4.gridy = 3;
		chartPanel.add(tab6,  gbc_panel4);
		

		
		solutionLbl = new JLabel("");
		solutionLbl.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_solutionLbl = new GridBagConstraints();
		gbc_solutionLbl.insets = new Insets(0, 0, 5, 5);
		gbc_solutionLbl.gridx = 0;
		gbc_solutionLbl.gridwidth = 5;
		gbc_solutionLbl.gridy = 0;
		panel_1.add(solutionLbl, gbc_solutionLbl);
		
		JLabel lblIRR = new JLabel("Internal Rate of Return:");
		GridBagConstraints gbc_lblIRR = new GridBagConstraints();
		gbc_lblIRR.insets = new Insets(0, 0, 5, 5);
		gbc_lblIRR.gridx = 4;
		gbc_lblIRR.gridy = 1;
		panel_1.add(lblIRR, gbc_lblIRR);
		
		irrLbl = new JLabel("");
		GridBagConstraints gbc_IRRLbl = new GridBagConstraints();
		gbc_IRRLbl.insets = new Insets(0, 0, 5, 5);
		gbc_IRRLbl.gridx = 5;
		gbc_IRRLbl.gridy = 1;
		panel_1.add(irrLbl, gbc_IRRLbl);
		
		JLabel lblTimeSpentTransitioning = new JLabel("Number of Years for Transition:");
		GridBagConstraints gbc_lblTimeSpentTransitioning = new GridBagConstraints();
		gbc_lblTimeSpentTransitioning.anchor = GridBagConstraints.WEST;
		gbc_lblTimeSpentTransitioning.insets = new Insets(0, 0, 0, 5);
		gbc_lblTimeSpentTransitioning.gridx = 0;
		gbc_lblTimeSpentTransitioning.gridy = 2;
		panel_1.add(lblTimeSpentTransitioning, gbc_lblTimeSpentTransitioning);

		timeTransitionLbl = new JLabel("");
		GridBagConstraints gbc_timeTransitionLbl = new GridBagConstraints();
		gbc_timeTransitionLbl.anchor = GridBagConstraints.WEST;
		gbc_timeTransitionLbl.insets = new Insets(0, 0, 0, 5);
		gbc_timeTransitionLbl.gridx = 1;
		gbc_timeTransitionLbl.gridy = 2;
		panel_1.add(timeTransitionLbl, gbc_timeTransitionLbl);
		
		JLabel lblAnnualBudget = new JLabel("Annual Budget During Transition:");
		GridBagConstraints gbc_lblAnnualBudget = new GridBagConstraints();
		gbc_lblAnnualBudget.insets = new Insets(0, 30, 5, 5);
		gbc_lblAnnualBudget.gridx = 4;
		gbc_lblAnnualBudget.gridy = 2;
		panel_1.add(lblAnnualBudget, gbc_lblAnnualBudget);
		
		annualBudgetLbl = new JLabel("");
		GridBagConstraints gbc_annualBudgetLbl = new GridBagConstraints();
		gbc_annualBudgetLbl.insets = new Insets(0, 0, 5, 5);
		gbc_annualBudgetLbl.gridx = 5;
		gbc_annualBudgetLbl.gridy = 2;
		panel_1.add(annualBudgetLbl, gbc_annualBudgetLbl);
		
		specificSysAlysPanel = new JPanel();
		tabbedPane.addTab("System Analysis", null, specificSysAlysPanel, null);
		GridBagLayout gbl_specificSysAlysPanel = new GridBagLayout();
		gbl_specificSysAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificSysAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificSysAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificSysAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		specificSysAlysPanel.setLayout(gbl_specificSysAlysPanel);
		
		specificFuncAlysPanel = new JPanel();
		tabbedPane.addTab("Functionality Analysis", null, specificFuncAlysPanel, null);
		GridBagLayout gbl_specificFuncAlysPanel = new GridBagLayout();
		gbl_specificFuncAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificFuncAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificFuncAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificFuncAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		specificFuncAlysPanel.setLayout(gbl_specificFuncAlysPanel);
	}
	
	/**
	 * Creates the user interface of the playsheet.
	 * Calls functions to create param panel and tabbed display panel
	 * Stitches the param and display panels together.
	 */
	public void createOptimizationTypeComponents()
	{

		rdbtnProfit = new JRadioButton("Savings");
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
		rdbtnROI.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnRoi = new GridBagConstraints();
		gbc_rdbtnRoi.anchor = GridBagConstraints.WEST;
		gbc_rdbtnRoi.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnRoi.gridx = 3;
		gbc_rdbtnRoi.gridy = 4;
		ctlPanel.add(rdbtnROI, gbc_rdbtnRoi);
		
		rdbtnIRR = new JRadioButton("IRR");
		rdbtnIRR.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnIRR = new GridBagConstraints();
		gbc_rdbtnIRR.anchor = GridBagConstraints.WEST;
		gbc_rdbtnIRR.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnIRR.gridx = 5;
		gbc_rdbtnIRR.gridy = 4;
		ctlPanel.add(rdbtnIRR, gbc_rdbtnIRR);

		OptFunctionRadioBtnListener opl = new OptFunctionRadioBtnListener();
		rdbtnROI.addActionListener(opl);
		rdbtnProfit.addActionListener(opl);
		rdbtnIRR.addActionListener(opl);
		opl.setSerOptRadioBtn(rdbtnProfit, rdbtnROI,rdbtnIRR);
	}
	
	
	public void hideAndClearSystemSelectPanel()
	{
		systemDataBLUSelectPanel.setVisible(false);
		systemSelectPanel.setVisible(false);
		capSelectDropDown.setVisible(false);
		capScrollPanel.setVisible(false);
		allCapButton.setSelected(false);
		dhmsmCapButton.setSelected(false);
		hsdCapButton.setSelected(false);
		hssCapButton.setSelected(false);
		fhpCapButton.setSelected(false);
		updateDataBLUPanelButton.setSelected(false);
		lblDataSelectHeader.setVisible(false);
		lblBLUSelectHeader.setVisible(false);
		updateProvideDataBLUButton.setVisible(false);
		updateConsumeDataBLUButton.setVisible(false);
		updateComplementDataBLUButton.setVisible(false);
		dataSelectDropDown.setVisible(false);
		bluSelectDropDown.setVisible(false);
		capSelectDropDown.clearList();
	}
	
	
}
