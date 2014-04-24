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
import javax.swing.JProgressBar;
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

	//select functionality panel and toggle button
	public JToggleButton showSystemSelectBtn, showSystemCapSelectBtn;
	public JPanel systemSelectPanel;
	public JPanel capScrollPanel;
	public JLabel lblDataSelectHeader,lblBLUSelectHeader;
	
	//system, capability, data, and blu selects
	public JCheckBox theaterSysButton, garrisonSysButton, allSysButton;
	public JCheckBox lowProbButton, medProbButton, highProbButton;
	public JCheckBox allCapButton, dhmsmCapButton;
	public JCheckBox hsdCapButton, hssCapButton, fhpCapButton;
	public SelectScrollList sysSelectDropDown, capSelectDropDown,dataSelectDropDown,bluSelectDropDown;
	public JToggleButton updateDataBLUPanelButton;
	public JButton updateDataBLUButton,updateComplementDataBLUButton;

	 //overall analysis tab
	public JLabel solutionLbl, annualBudgetLbl, timeTransitionLbl;
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
				
		systemSelectPanel = new JPanel();
		systemSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		systemSelectPanel.setVisible(false);
		
		GridBagConstraints gbc_systemSelectPanel = new GridBagConstraints();
		gbc_systemSelectPanel.gridheight = 6;
		gbc_systemSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectPanel.gridx = 8;
		gbc_systemSelectPanel.gridy = 0;
		ctlPanel.add(systemSelectPanel, gbc_systemSelectPanel);
		
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		systemSelectPanel.setLayout(gbl_systemSelectPanel);
		
		JLabel lblSystemSelectHeader = new JLabel("Select Systems:");
		lblSystemSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSystemSelectHeader = new GridBagConstraints();
		gbc_lblSystemSelectHeader.gridwidth = 3;
		gbc_lblSystemSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblSystemSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblSystemSelectHeader.gridx = 0;
		gbc_lblSystemSelectHeader.gridy = 0;
		systemSelectPanel.add(lblSystemSelectHeader, gbc_lblSystemSelectHeader);
		
		allSysButton = new JCheckBox("All Systems");
		GridBagConstraints gbc_allSysButton = new GridBagConstraints();
		gbc_allSysButton.anchor = GridBagConstraints.WEST;
		gbc_allSysButton.gridx = 0;
		gbc_allSysButton.gridy = 1;
		systemSelectPanel.add(allSysButton, gbc_allSysButton);
		
		theaterSysButton = new JCheckBox("Theater");
		GridBagConstraints gbc_theaterSysButton = new GridBagConstraints();
		gbc_theaterSysButton.anchor = GridBagConstraints.WEST;
		gbc_theaterSysButton.gridx = 1;
		gbc_theaterSysButton.gridy = 1;
		systemSelectPanel.add(theaterSysButton, gbc_theaterSysButton);

		garrisonSysButton = new JCheckBox("Garrison");
		GridBagConstraints gbc_garrisonSysButton = new GridBagConstraints();
		gbc_garrisonSysButton.anchor = GridBagConstraints.WEST;
		gbc_garrisonSysButton.gridx = 2;
		gbc_garrisonSysButton.gridy = 1;
		systemSelectPanel.add(garrisonSysButton, gbc_garrisonSysButton);
		
		lowProbButton = new JCheckBox("Low Prob");
		GridBagConstraints gbc_lowProbButton = new GridBagConstraints();
		gbc_lowProbButton.anchor = GridBagConstraints.WEST;
		gbc_lowProbButton.gridx = 0;
		gbc_lowProbButton.gridy = 2;
		systemSelectPanel.add(lowProbButton, gbc_lowProbButton);
		
		medProbButton = new JCheckBox("Med Prob");
		GridBagConstraints gbc_medProbButton = new GridBagConstraints();
		gbc_medProbButton.anchor = GridBagConstraints.WEST;
		gbc_medProbButton.gridx = 1;
		gbc_medProbButton.gridy = 2;
		systemSelectPanel.add(medProbButton, gbc_medProbButton);
		
		highProbButton = new JCheckBox("High Prob");
		GridBagConstraints gbc_highProbButton = new GridBagConstraints();
		gbc_highProbButton.anchor = GridBagConstraints.WEST;
		gbc_highProbButton.gridx = 2;
		gbc_highProbButton.gridy = 2;
		systemSelectPanel.add(highProbButton, gbc_highProbButton);		


		
		updateDataBLUPanelButton = new ToggleButton("View Data/BLU");
		updateDataBLUPanelButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateDataBLUPanelButton,  ".toggleButton");
		
		GridBagConstraints gbc_updateDataBLUPanelButton = new GridBagConstraints();
		gbc_updateDataBLUPanelButton.anchor = GridBagConstraints.WEST;
		gbc_updateDataBLUPanelButton.gridheight = 2;
		gbc_updateDataBLUPanelButton.insets = new Insets(0, 0, 5, 5);
		gbc_updateDataBLUPanelButton.gridx = 8;
		gbc_updateDataBLUPanelButton.gridy = 0;
		systemSelectPanel.add(updateDataBLUPanelButton, gbc_updateDataBLUPanelButton);
		
		updateDataBLUButton = new CustomButton("Select Data/BLU");
		updateDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateDataBLUButton,  ".toggleButton");
		updateDataBLUButton.setVisible(false);		
		
		GridBagConstraints gbc_updateDataBLUButton = new GridBagConstraints();
		gbc_updateDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateDataBLUButton.gridheight = 2;
		gbc_updateDataBLUButton.insets = new Insets(0, 0, 5, 5);
		gbc_updateDataBLUButton.gridx = 9;
		gbc_updateDataBLUButton.gridy = 0;
		systemSelectPanel.add(updateDataBLUButton, gbc_updateDataBLUButton);
		
		updateComplementDataBLUButton = new CustomButton("Select Complement");
		updateComplementDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateComplementDataBLUButton,  ".toggleButton");
		updateComplementDataBLUButton.setVisible(false);		
		
		GridBagConstraints gbc_updateComplementDataBLUButton = new GridBagConstraints();
		gbc_updateComplementDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateComplementDataBLUButton.gridwidth = 3;
		gbc_updateComplementDataBLUButton.gridheight = 2;
		gbc_updateComplementDataBLUButton.insets = new Insets(0, 0, 5, 5);
		gbc_updateComplementDataBLUButton.gridx = 11;
		gbc_updateComplementDataBLUButton.gridy = 0;
		systemSelectPanel.add(updateComplementDataBLUButton, gbc_updateComplementDataBLUButton);

		lblDataSelectHeader = new JLabel("Select Data Objects:");
		lblDataSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblDataSelectHeader.setVisible(false);
		GridBagConstraints gbc_lblDataSelectHeader = new GridBagConstraints();
		gbc_lblDataSelectHeader.gridwidth = 3;
		gbc_lblDataSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblDataSelectHeader.insets = new Insets(0, 5, 5, 5);
		gbc_lblDataSelectHeader.gridx = 8;
		gbc_lblDataSelectHeader.gridy = 2;
		systemSelectPanel.add(lblDataSelectHeader, gbc_lblDataSelectHeader);
		
		lblBLUSelectHeader = new JLabel("Select BLUs:");
		lblBLUSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblBLUSelectHeader.setVisible(false);
		GridBagConstraints gbc_lblBLUSelectHeader = new GridBagConstraints();
		gbc_lblBLUSelectHeader.gridwidth = 3;
		gbc_lblBLUSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblBLUSelectHeader.insets = new Insets(0, 0, 5, 5);
		gbc_lblBLUSelectHeader.gridx = 12;
		gbc_lblBLUSelectHeader.gridy = 2;
		systemSelectPanel.add(lblBLUSelectHeader, gbc_lblBLUSelectHeader);
		
		sysSelectDropDown = new SelectScrollList("Select Individual Systems");
		sysSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_sysSelectDropDown = new GridBagConstraints();
		gbc_sysSelectDropDown.gridwidth = 3;
		gbc_sysSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_sysSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysSelectDropDown.gridx = 0;
		gbc_sysSelectDropDown.gridy = 3;
		systemSelectPanel.add(sysSelectDropDown.pane, gbc_sysSelectDropDown);
		
		String[] sysArray = makeListFromQuery("System","SELECT DISTINCT ?entity WHERE {BIND(<http://health.mil/ontologies/Concept/TaskerLifecyclePhase/Submitted> AS ?TaskerStatus) {?SystemUser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?SystemTasker <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemTasker>;} {?BelongsTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo> ;} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Submits <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Receives-Submits> ;}  {?BeingIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BeingIn> ;}{?TaskerLifecycleSystemPhase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TaskerLifecycleSystemPhase>;}{?TypeOf2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;}{?TaskerStatus <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TaskerLifecyclePhase>;}{?entity ?Submits ?SystemTasker}{?SystemTasker ?BelongsTo ?SystemUser}{?SystemTasker ?BeingIn ?TaskerLifecycleSystemPhase}{?TaskerLifecycleSystemPhase ?TypeOf2 ?TaskerStatus} } ");
		sysSelectDropDown.setupButton(sysArray,40,120); //need to give list of all systems

		CheckBoxSelectorListener sysCheckBoxListener = new CheckBoxSelectorListener();
		sysCheckBoxListener.setEngine(engine);
		sysCheckBoxListener.setScrollList(sysSelectDropDown);
		sysCheckBoxListener.setCheckBox(allSysButton,theaterSysButton, garrisonSysButton,lowProbButton, medProbButton, highProbButton);
		allSysButton.addActionListener(sysCheckBoxListener);
		theaterSysButton.addActionListener(sysCheckBoxListener);
		garrisonSysButton.addActionListener(sysCheckBoxListener);
		lowProbButton.addActionListener(sysCheckBoxListener);
		medProbButton.addActionListener(sysCheckBoxListener);
		highProbButton.addActionListener(sysCheckBoxListener);
		
		
		capScrollPanel = new JPanel();
		capScrollPanel.setVisible(false);
		
		GridBagConstraints gbc_capScrollPanel = new GridBagConstraints();
		gbc_capScrollPanel.gridheight = 6;
		gbc_capScrollPanel.fill = GridBagConstraints.BOTH;
		gbc_capScrollPanel.gridx = 5;
		gbc_capScrollPanel.gridy = 0;
		systemSelectPanel.add(capScrollPanel, gbc_capScrollPanel);
		
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
		systemSelectPanel.add(dataSelectDropDown.pane, gbc_dataSelectDropDown);

		String[] dataArray = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.} }");
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
		systemSelectPanel.add(bluSelectDropDown.pane, gbc_bluSelectDropDown);
		
		String[] bluArray = makeListFromQuery("BusinessLogicUnit","SELECT DISTINCT ?entity WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?entity}}");
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
		updateDataBLUListener.setComponents(sysSelectDropDown,capSelectDropDown,dataSelectDropDown,bluSelectDropDown,lblDataSelectHeader,lblBLUSelectHeader,showSystemSelectBtn);
		updateDataBLUListener.setUpdateButtons(updateDataBLUPanelButton,updateDataBLUButton,updateComplementDataBLUButton);
		updateDataBLUPanelButton.addActionListener(updateDataBLUListener);
		updateDataBLUButton.addActionListener(updateDataBLUListener);
		updateComplementDataBLUButton.addActionListener(updateDataBLUListener);
	}
	@Override
	public void createSpecificParamComponents()
	{
		yearField.setText("20");
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
		
		JLabel lblAnnualBudget = new JLabel("Annual Budget During Transition:");
		GridBagConstraints gbc_lblAnnualBudget = new GridBagConstraints();
		gbc_lblAnnualBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblAnnualBudget.gridx = 4;
		gbc_lblAnnualBudget.gridy = 1;
		panel_1.add(lblAnnualBudget, gbc_lblAnnualBudget);		
		
		solutionLbl = new JLabel("");
		solutionLbl.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_solutionLbl = new GridBagConstraints();
		gbc_solutionLbl.insets = new Insets(0, 0, 5, 5);
		gbc_solutionLbl.gridx = 0;
		gbc_solutionLbl.gridwidth = 5;
		gbc_solutionLbl.gridy = 0;
		panel_1.add(solutionLbl, gbc_solutionLbl);
		
		annualBudgetLbl = new JLabel("");
		GridBagConstraints gbc_annualBudgetLbl = new GridBagConstraints();
		gbc_annualBudgetLbl.insets = new Insets(0, 0, 5, 5);
		gbc_annualBudgetLbl.gridx = 5;
		gbc_annualBudgetLbl.gridy = 1;
		panel_1.add(annualBudgetLbl, gbc_annualBudgetLbl);
		
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

		OptFunctionRadioBtnListener opl = new OptFunctionRadioBtnListener();
		rdbtnROI.addActionListener(opl);
		rdbtnProfit.addActionListener(opl);
		opl.setRadioBtn(rdbtnProfit, rdbtnROI);
	}
	
}
